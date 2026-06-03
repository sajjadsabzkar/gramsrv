package redisstore

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

type staticCounterSource struct {
	value int
}

func (s staticCounterSource) Current(context.Context, int64) (int, error) {
	return s.value, nil
}

func TestRedisAllocatorsRecoverFromCounterSource(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	t.Cleanup(func() { _ = c.Del(ctx, ptsKey(userID), boxIDKey(userID)).Err() })

	pts := NewPtsAllocator(c, staticCounterSource{value: 41})
	currentPts, err := pts.CurrentPts(ctx, userID)
	if err != nil {
		t.Fatalf("CurrentPts: %v", err)
	}
	if currentPts != 41 {
		t.Fatalf("current pts = %d, want recovered 41", currentPts)
	}
	nextPts, err := pts.NextPts(ctx, userID)
	if err != nil {
		t.Fatalf("NextPts: %v", err)
	}
	if nextPts != 42 {
		t.Fatalf("next pts = %d, want 42", nextPts)
	}

	boxes := NewBoxIDAllocator(c, staticCounterSource{value: 100})
	currentBox, err := boxes.CurrentBoxID(ctx, userID)
	if err != nil {
		t.Fatalf("CurrentBoxID: %v", err)
	}
	if currentBox != 100 {
		t.Fatalf("current box = %d, want recovered 100", currentBox)
	}
	nextBox, err := boxes.NextBoxID(ctx, userID)
	if err != nil {
		t.Fatalf("NextBoxID: %v", err)
	}
	if nextBox != 101 {
		t.Fatalf("next box = %d, want 101", nextBox)
	}
}

func TestRedisAllocatorConcurrentFirstUse(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	t.Cleanup(func() { _ = c.Del(ctx, ptsKey(userID)).Err() })

	const workers = 32
	pts := NewPtsAllocator(c, staticCounterSource{value: 1000})
	values := make(chan int, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := pts.NextPts(ctx, userID)
			if err != nil {
				errs <- err
				return
			}
			values <- v
		}()
	}
	wg.Wait()
	close(values)
	close(errs)

	for err := range errs {
		t.Fatalf("NextPts: %v", err)
	}
	seen := make(map[int]bool, workers)
	for v := range values {
		if v < 1001 || v > 1000+workers {
			t.Fatalf("pts = %d, want recovered contiguous range", v)
		}
		if seen[v] {
			t.Fatalf("duplicate pts %d", v)
		}
		seen[v] = true
	}
	for want := 1001; want <= 1000+workers; want++ {
		if !seen[want] {
			t.Fatalf("missing pts %d", want)
		}
	}
	current, err := pts.CurrentPts(ctx, userID)
	if err != nil {
		t.Fatalf("CurrentPts: %v", err)
	}
	if current != 1000+workers {
		t.Fatalf("current pts = %d, want %d", current, 1000+workers)
	}
}

func TestRedisRateLimiterWindow(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	key := "test:" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = c.Del(ctx, rateLimitKey(key)).Err() })

	limiter := NewRateLimiter(c)
	allowed, retry, err := limiter.Allow(ctx, key, 1, time.Minute)
	if err != nil {
		t.Fatalf("Allow first: %v", err)
	}
	if !allowed || retry != 0 {
		t.Fatalf("first allowed=%v retry=%d, want allowed", allowed, retry)
	}
	allowed, retry, err = limiter.Allow(ctx, key, 1, time.Minute)
	if err != nil {
		t.Fatalf("Allow second: %v", err)
	}
	if allowed || retry <= 0 {
		t.Fatalf("second allowed=%v retry=%d, want limited with retry", allowed, retry)
	}
}
