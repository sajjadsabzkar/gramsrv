package redisstore

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/store"
)

// TestSessionStoreRoundTrip 验证 session 落 Redis 后能用全新 store 实例原样读回。
// 未设 TELESRV_TEST_REDIS_ADDR 则跳过。
func TestSessionStoreRoundTrip(t *testing.T) {
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

	want := store.SessionData{
		ID:        0x1234beef,
		AuthKeyID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Salt:      42,
		LastSeen:  1000,
	}
	t.Cleanup(func() { _ = c.Del(ctx, sessionKey(want.ID)).Err() })

	if err := NewSessionStore(c, time.Minute).Save(ctx, want); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, found, err := NewSessionStore(c, time.Minute).Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("session not found after save")
	}
	if got != want {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
	}

	if _, found, _ := NewSessionStore(c, time.Minute).Get(ctx, 999999); found {
		t.Fatal("unexpected found for missing session")
	}
}
