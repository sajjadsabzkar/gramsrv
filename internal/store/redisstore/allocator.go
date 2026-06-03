package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
)

// PtsAllocator 用 Redis INCR 分配账号级 pts，Redis 丢失时从 PG durable log 恢复当前最大 pts。
type PtsAllocator struct {
	counter counterAllocator
}

// BoxIDAllocator 用 Redis INCR 分配 owner 视角的 message box id。
type BoxIDAllocator struct {
	counter counterAllocator
}

// ChannelPtsAllocator 用 Redis INCR 分配 channel 维度 pts。
type ChannelPtsAllocator struct {
	counter counterAllocator
}

// ChannelIDAllocator 用 Redis INCR 分配全局 channel/supergroup id。
type ChannelIDAllocator struct {
	counter counterAllocator
}

// ChannelMessageIDAllocator 用 Redis INCR 分配 channel 维度 message id。
type ChannelMessageIDAllocator struct {
	counter counterAllocator
}

type counterAllocator struct {
	c      *redis.Client
	source store.CounterSource
	key    func(int64) string
	name   string
}

const missingCounterSentinel int64 = -1

var (
	counterNextScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current then
  return redis.call("INCR", KEYS[1])
end
return -1
`)

	counterNextByScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current then
  return redis.call("INCRBY", KEYS[1], ARGV[1])
end
return -1
`)

	counterRecoverCurrentScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current then
  return tonumber(current)
end
redis.call("SET", KEYS[1], ARGV[1])
return tonumber(ARGV[1])
`)

	counterRecoverNextScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current then
  redis.call("SET", KEYS[1], ARGV[1])
end
return redis.call("INCR", KEYS[1])
`)

	counterRecoverNextByScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current then
  redis.call("SET", KEYS[1], ARGV[1])
end
return redis.call("INCRBY", KEYS[1], ARGV[2])
`)
)

// NewPtsAllocator 创建 Redis-backed pts allocator。
func NewPtsAllocator(c *redis.Client, source store.CounterSource) *PtsAllocator {
	return &PtsAllocator{counter: counterAllocator{
		c:      c,
		source: source,
		key:    ptsKey,
		name:   "pts",
	}}
}

// NewBoxIDAllocator 创建 Redis-backed message box id allocator。
func NewBoxIDAllocator(c *redis.Client, source store.CounterSource) *BoxIDAllocator {
	return &BoxIDAllocator{counter: counterAllocator{
		c:      c,
		source: source,
		key:    boxIDKey,
		name:   "box_id",
	}}
}

// NewChannelPtsAllocator 创建 Redis-backed channel pts allocator。
func NewChannelPtsAllocator(c *redis.Client, source store.CounterSource) *ChannelPtsAllocator {
	return &ChannelPtsAllocator{counter: counterAllocator{
		c:      c,
		source: source,
		key:    channelPtsKey,
		name:   "channel_pts",
	}}
}

// NewChannelIDAllocator 创建 Redis-backed channel id allocator。
func NewChannelIDAllocator(c *redis.Client, source store.CounterSource) *ChannelIDAllocator {
	return &ChannelIDAllocator{counter: counterAllocator{
		c:      c,
		source: source,
		key:    channelIDKey,
		name:   "channel_id",
	}}
}

// NewChannelMessageIDAllocator 创建 Redis-backed channel message id allocator。
func NewChannelMessageIDAllocator(c *redis.Client, source store.CounterSource) *ChannelMessageIDAllocator {
	return &ChannelMessageIDAllocator{counter: counterAllocator{
		c:      c,
		source: source,
		key:    channelMessageIDKey,
		name:   "channel_msg_id",
	}}
}

func ptsKey(userID int64) string {
	return fmt.Sprintf("counter:pts:{%d}", userID)
}

func boxIDKey(userID int64) string {
	return fmt.Sprintf("counter:box_id:{%d}", userID)
}

func channelPtsKey(channelID int64) string {
	return fmt.Sprintf("counter:channel_pts:{%d}", channelID)
}

func channelIDKey(_ int64) string {
	return "counter:channel_id"
}

func channelMessageIDKey(channelID int64) string {
	return fmt.Sprintf("counter:channel_msg_id:{%d}", channelID)
}

func (a *PtsAllocator) NextPts(ctx context.Context, userID int64) (int, error) {
	v, err := a.counter.next(ctx, userID)
	return int(v), err
}

func (a *PtsAllocator) NextPtsN(ctx context.Context, userID int64, count int) (int, error) {
	v, err := a.counter.nextBy(ctx, userID, count)
	return int(v), err
}

func (a *PtsAllocator) CurrentPts(ctx context.Context, userID int64) (int, error) {
	v, err := a.counter.current(ctx, userID)
	return int(v), err
}

func (a *BoxIDAllocator) NextBoxID(ctx context.Context, userID int64) (int, error) {
	v, err := a.counter.next(ctx, userID)
	return int(v), err
}

func (a *BoxIDAllocator) CurrentBoxID(ctx context.Context, userID int64) (int, error) {
	v, err := a.counter.current(ctx, userID)
	return int(v), err
}

func (a *ChannelPtsAllocator) NextChannelPts(ctx context.Context, channelID int64) (int, error) {
	v, err := a.counter.next(ctx, channelID)
	return int(v), err
}

func (a *ChannelPtsAllocator) NextChannelPtsN(ctx context.Context, channelID int64, count int) (int, error) {
	v, err := a.counter.nextBy(ctx, channelID, count)
	return int(v), err
}

func (a *ChannelPtsAllocator) CurrentChannelPts(ctx context.Context, channelID int64) (int, error) {
	v, err := a.counter.current(ctx, channelID)
	return int(v), err
}

func (a *ChannelIDAllocator) NextChannelID(ctx context.Context) (int64, error) {
	return a.counter.next(ctx, 1)
}

func (a *ChannelIDAllocator) CurrentChannelID(ctx context.Context) (int64, error) {
	return a.counter.current(ctx, 1)
}

func (a *ChannelMessageIDAllocator) NextChannelMessageID(ctx context.Context, channelID int64) (int, error) {
	v, err := a.counter.next(ctx, channelID)
	return int(v), err
}

func (a *ChannelMessageIDAllocator) CurrentChannelMessageID(ctx context.Context, channelID int64) (int, error) {
	v, err := a.counter.current(ctx, channelID)
	return int(v), err
}

func (a counterAllocator) next(ctx context.Context, userID int64) (int64, error) {
	return a.nextBy(ctx, userID, 1)
}

func (a counterAllocator) nextBy(ctx context.Context, userID int64, count int) (int64, error) {
	if count <= 0 {
		return 0, fmt.Errorf("redis next %s counter: invalid count %d", a.name, count)
	}
	key, err := a.validatedKey(userID)
	if err != nil {
		return 0, err
	}
	script := counterNextScript
	args := []any{}
	if count > 1 {
		script = counterNextByScript
		args = append(args, count)
	}
	v, err := script.Run(ctx, a.c, []string{key}, args...).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis next %s counter: %w", a.name, err)
	}
	if v != missingCounterSentinel {
		return v, nil
	}
	recovered, err := a.recovered(ctx, userID)
	if err != nil {
		return 0, err
	}
	recoverScript := counterRecoverNextScript
	recoverArgs := []any{recovered}
	if count > 1 {
		recoverScript = counterRecoverNextByScript
		recoverArgs = append(recoverArgs, count)
	}
	v, err = recoverScript.Run(ctx, a.c, []string{key}, recoverArgs...).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis recover-next %s counter: %w", a.name, err)
	}
	return v, nil
}

func (a counterAllocator) current(ctx context.Context, userID int64) (int64, error) {
	key, err := a.validatedKey(userID)
	if err != nil {
		return 0, err
	}
	v, err := a.c.Get(ctx, key).Int64()
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("redis get %s counter: %w", a.name, err)
	}
	recovered, err := a.recovered(ctx, userID)
	if err != nil {
		return 0, err
	}
	v, err = counterRecoverCurrentScript.Run(ctx, a.c, []string{key}, recovered).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis recover-current %s counter: %w", a.name, err)
	}
	return v, nil
}

func (a counterAllocator) validatedKey(userID int64) (string, error) {
	if userID == 0 {
		return "", fmt.Errorf("redis %s counter: missing user id", a.name)
	}
	if a.c == nil {
		return "", fmt.Errorf("redis %s counter: nil client", a.name)
	}
	return a.key(userID), nil
}

func (a counterAllocator) recovered(ctx context.Context, userID int64) (int, error) {
	recovered := 0
	var err error
	if a.source != nil {
		recovered, err = a.source.Current(ctx, userID)
		if err != nil {
			return 0, fmt.Errorf("recover %s counter: %w", a.name, err)
		}
	}
	return recovered, nil
}
