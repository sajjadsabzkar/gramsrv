package redisstore

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter 用 Redis INCR + TTL 实现固定窗口限流。
type RateLimiter struct {
	c *redis.Client
}

// NewRateLimiter 创建 Redis-backed RateLimiter。
func NewRateLimiter(c *redis.Client) *RateLimiter {
	return &RateLimiter{c: c}
}

func rateLimitKey(key string) string {
	return "ratelimit:" + key
}

func (l *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, int, error) {
	if limit <= 0 {
		return true, 0, nil
	}
	if window <= 0 {
		window = time.Second
	}
	if l == nil || l.c == nil {
		return false, 0, fmt.Errorf("redis rate limiter: nil client")
	}
	redisKey := rateLimitKey(key)
	count, err := l.c.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, 0, fmt.Errorf("redis incr rate limit: %w", err)
	}
	if count == 1 {
		if err := l.c.Expire(ctx, redisKey, window).Err(); err != nil {
			return false, 0, fmt.Errorf("redis expire rate limit: %w", err)
		}
	}
	if count <= int64(limit) {
		return true, 0, nil
	}
	ttl, err := l.c.TTL(ctx, redisKey).Result()
	if err != nil {
		return false, 0, fmt.Errorf("redis ttl rate limit: %w", err)
	}
	if ttl <= 0 {
		ttl = window
	}
	return false, int(math.Ceil(ttl.Seconds())), nil
}
