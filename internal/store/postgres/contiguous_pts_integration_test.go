package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

// TestMaxContiguousPtsStopsAtHole 用真实 PG 验证 RecentUserPts 窗口查询 + 连续计算：
// 存在在途空洞时只报告最大连续 pts，补洞后回升。
func TestMaxContiguousPtsStopsAtHole(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	suffix := randomSuffix(t)
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 1,
		Phone:      "+1556" + suffix + "01",
		FirstName:  "Contig",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = $1", owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	events := NewUpdateEventStore(pool)
	appendPts := func(pts int) {
		if err := events.Append(ctx, owner.ID, domain.UpdateEvent{
			Type:     domain.UpdateEventNoop,
			Pts:      pts,
			PtsCount: 1,
			Date:     1700000000 + pts,
		}); err != nil {
			t.Fatalf("append pts=%d: %v", pts, err)
		}
	}

	for _, p := range []int{1, 2, 3, 5, 6} { // pts=4 为在途空洞
		appendPts(p)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM user_update_watermarks WHERE user_id = $1", owner.ID); err != nil {
		t.Fatalf("delete watermark fallback row: %v", err)
	}
	got, err := events.MaxContiguousPts(ctx, owner.ID)
	if err != nil {
		t.Fatalf("MaxContiguousPts: %v", err)
	}
	if got != 3 {
		t.Fatalf("contiguous = %d, want 3（止于 pts=4 空洞，最大已提交为 6）", got)
	}

	appendPts(4) // 在途事务提交/补洞
	got, err = events.MaxContiguousPts(ctx, owner.ID)
	if err != nil {
		t.Fatalf("MaxContiguousPts after fill: %v", err)
	}
	if got != 6 {
		t.Fatalf("contiguous after fill = %d, want 6", got)
	}
}

func TestAppendWithDispatchWritesEventAndOutboxAtomically(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	suffix := randomSuffix(t)
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 2,
		Phone:      "+1557" + suffix + "01",
		FirstName:  "Dispatch",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM dispatch_outbox WHERE target_user_id = $1", owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = $1", owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	event := domain.UpdateEvent{
		UserID:   owner.ID,
		Type:     domain.UpdateEventDialogPinned,
		Pts:      1,
		PtsCount: 1,
		Date:     1700000001,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: 1000000002},
		Settings: domain.PeerSettings{
			ShareContact: true,
		},
		Bool: true,
	}
	var excludeAuthKeyID [8]byte
	excludeAuthKeyID[0] = 9
	if err := NewUpdateEventStore(pool).AppendWithDispatch(ctx, owner.ID, event, excludeAuthKeyID, 77); err != nil {
		t.Fatalf("AppendWithDispatch: %v", err)
	}

	got, err := NewUpdateEventStore(pool).ListAfter(ctx, owner.ID, 0, 10)
	if err != nil {
		t.Fatalf("ListAfter: %v", err)
	}
	if len(got) != 1 || got[0].Type != event.Type || got[0].Peer != event.Peer || !got[0].Bool {
		t.Fatalf("events = %+v, want dialog pinned event", got)
	}

	var outbox struct {
		pts              int
		eventType        string
		excludeAuthKeyID int64
		excludeSessionID int64
	}
	if err := pool.QueryRow(ctx, `
		SELECT pts, event_type, exclude_auth_key_id, exclude_session_id
		FROM dispatch_outbox
		WHERE target_user_id = $1
	`, owner.ID).Scan(&outbox.pts, &outbox.eventType, &outbox.excludeAuthKeyID, &outbox.excludeSessionID); err != nil {
		t.Fatalf("query dispatch outbox: %v", err)
	}
	if outbox.pts != 1 || outbox.eventType != string(domain.UpdateEventDialogPinned) || outbox.excludeAuthKeyID != authKeyIDToInt64(excludeAuthKeyID) || outbox.excludeSessionID != 77 {
		t.Fatalf("outbox = %+v, want event dispatch excluding current session", outbox)
	}
}
