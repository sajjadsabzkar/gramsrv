package redisstore

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
)

// UserCounterAllocator 在一次 Lua 往返内同时分配某 user 的下一个 pts 与 box_id。
//
// counter:pts:{userID} 与 counter:box_id:{userID} 共享 hash-tag {userID}，处于同一
// Redis Cluster slot，可被单个 Lua 脚本原子操作——把发送热路径上「pts + box 两次往返」
// 合并成一次。语义与 PtsAllocator/BoxIDAllocator 各自调用等价：pts 账号级无洞、
// Redis 冷 miss 时从 PG durable log 恢复基线。
type UserCounterAllocator struct {
	c         *redis.Client
	ptsSource store.CounterSource
	boxSource store.CounterSource
}

// NewUserCounterAllocator 创建合并 allocator。ptsSource 恢复 pts（MAX(user_update_events.pts)），
// boxSource 恢复 box_id（MAX(message_boxes.box_id)）。
func NewUserCounterAllocator(c *redis.Client, ptsSource, boxSource store.CounterSource) *UserCounterAllocator {
	return &UserCounterAllocator{c: c, ptsSource: ptsSource, boxSource: boxSource}
}

// userCountersNextScript 热路径：两 key 都存在时各自 INCR 并返回；任一缺失返回 {-1,-1} 触发恢复。
var userCountersNextScript = redis.NewScript(`
local pts = redis.call("GET", KEYS[1])
local box = redis.call("GET", KEYS[2])
if pts and box then
  return {redis.call("INCR", KEYS[1]), redis.call("INCR", KEYS[2])}
end
return {-1, -1}
`)

// userCountersRecoverScript 冷路径：每个 key「缺失才用 PG 基线 SET，再 INCR」。
// 对已存在的 key 跳过 SET 只 INCR，故对「一存一缺」也安全；Redis 单线程串行化保证无重复无洞。
var userCountersRecoverScript = redis.NewScript(`
local function recnext(key, base)
  if not redis.call("GET", key) then
    redis.call("SET", key, base)
  end
  return redis.call("INCR", key)
end
return {recnext(KEYS[1], ARGV[1]), recnext(KEYS[2], ARGV[2])}
`)

// NextUserCounters 返回该 user 的下一个 (pts, boxID)。
func (a *UserCounterAllocator) NextUserCounters(ctx context.Context, userID int64) (int, int, error) {
	if userID == 0 {
		return 0, 0, fmt.Errorf("redis user counters: missing user id")
	}
	if a.c == nil {
		return 0, 0, fmt.Errorf("redis user counters: nil client")
	}
	keys := []string{ptsKey(userID), boxIDKey(userID)}

	pts, box, err := runTwoCounters(ctx, a.c, userCountersNextScript, keys)
	if err != nil {
		return 0, 0, fmt.Errorf("redis next user counters: %w", err)
	}
	if pts != missingCounterSentinel && box != missingCounterSentinel {
		return int(pts), int(box), nil
	}

	// 冷路径：至少一个 key 缺失，从 PG durable log 恢复两个基线，再 recover-next。
	ptsBase, err := recoverBase(ctx, a.ptsSource, userID, "pts")
	if err != nil {
		return 0, 0, err
	}
	boxBase, err := recoverBase(ctx, a.boxSource, userID, "box_id")
	if err != nil {
		return 0, 0, err
	}
	pts, box, err = runTwoCounters(ctx, a.c, userCountersRecoverScript, keys, ptsBase, boxBase)
	if err != nil {
		return 0, 0, fmt.Errorf("redis recover user counters: %w", err)
	}
	return int(pts), int(box), nil
}

// runTwoCounters 执行返回二元整数表的 Lua 脚本，用 .Slice() 手动解析（不依赖 Int64Slice）。
func runTwoCounters(ctx context.Context, c *redis.Client, script *redis.Script, keys []string, args ...any) (int64, int64, error) {
	raw, err := script.Run(ctx, c, keys, args...).Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(raw) != 2 {
		return 0, 0, fmt.Errorf("unexpected reply len %d, want 2", len(raw))
	}
	a, okA := raw[0].(int64)
	b, okB := raw[1].(int64)
	if !okA || !okB {
		return 0, 0, fmt.Errorf("unexpected reply element types %T,%T, want int64", raw[0], raw[1])
	}
	return a, b, nil
}

func recoverBase(ctx context.Context, source store.CounterSource, userID int64, name string) (int, error) {
	if source == nil {
		return 0, nil
	}
	v, err := source.Current(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("recover %s counter: %w", name, err)
	}
	return v, nil
}
