package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
)

// DefaultSessionTTL 是 session 记录的默认过期时间。
// session 是连接态：过期或丢失后，客户端重连会触发 new_session_created / bad_server_salt 重建，
// 因此 TTL 不必很长。
const DefaultSessionTTL = 30 * 24 * time.Hour

// SessionStore 用 Redis 实现 store.SessionStore。
type SessionStore struct {
	c   *redis.Client
	ttl time.Duration
}

// NewSessionStore 创建 Redis SessionStore。ttl<=0 表示永不过期。
func NewSessionStore(c *redis.Client, ttl time.Duration) *SessionStore {
	return &SessionStore{c: c, ttl: ttl}
}

func sessionKey(id int64) string {
	return fmt.Sprintf("session:%d", id)
}

// sessionValue 是 SessionData 在 Redis 中的序列化形态（不含 ID，ID 即 key）。
type sessionValue struct {
	AuthKeyID [8]byte `json:"auth_key_id"`
	Salt      int64   `json:"salt"`
	LastSeen  int64   `json:"last_seen"`
}

// Save 实现 store.SessionStore。
func (s *SessionStore) Save(ctx context.Context, d store.SessionData) error {
	v, err := json.Marshal(sessionValue{AuthKeyID: d.AuthKeyID, Salt: d.Salt, LastSeen: d.LastSeen})
	if err != nil {
		return fmt.Errorf("marshal session: %w", err)
	}
	if err := s.c.Set(ctx, sessionKey(d.ID), v, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis set session: %w", err)
	}
	return nil
}

// Get 实现 store.SessionStore。不存在时 found=false。
func (s *SessionStore) Get(ctx context.Context, id int64) (store.SessionData, bool, error) {
	raw, err := s.c.Get(ctx, sessionKey(id)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return store.SessionData{}, false, nil
		}
		return store.SessionData{}, false, fmt.Errorf("redis get session: %w", err)
	}
	var v sessionValue
	if err := json.Unmarshal(raw, &v); err != nil {
		return store.SessionData{}, false, fmt.Errorf("unmarshal session: %w", err)
	}
	return store.SessionData{ID: id, AuthKeyID: v.AuthKeyID, Salt: v.Salt, LastSeen: v.LastSeen}, true, nil
}

func (s *SessionStore) Delete(ctx context.Context, id int64) error {
	if err := s.c.Del(ctx, sessionKey(id)).Err(); err != nil {
		return fmt.Errorf("redis delete session: %w", err)
	}
	return nil
}
