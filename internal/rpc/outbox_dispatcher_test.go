package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestOutboxDispatcherPushesNewMessageAndMarksDelivered(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeSessionID: 99,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users: []domain.User{{
			ID:        msg.From.ID,
			FirstName: "Sender",
		}},
	}}}
	sessions := &captureSessions{}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewOutboxDispatcher(events, outbox, sessions, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.delivered || outbox.deliveredUserID != msg.OwnerUserID || outbox.deliveredID != 55 {
		t.Fatalf("delivered = %v user=%d id=%d, want outbox delivered", outbox.delivered, outbox.deliveredUserID, outbox.deliveredID)
	}
	if sessions.userID != msg.OwnerUserID || sessions.sessionID != 99 || sessions.messageType != proto.MessageFromServer {
		t.Fatalf("push target = user %d exclude %d type %v, want outbox target/exclude", sessions.userID, sessions.sessionID, sessions.messageType)
	}
	updates, ok := sessions.message.(*tg.Updates)
	if !ok {
		t.Fatalf("pushed message = %T, want *tg.Updates", sessions.message)
	}
	if len(updates.Updates) != 1 || len(updates.Users) != 1 {
		t.Fatalf("updates = %+v, want one update and sender user", updates)
	}
	update, ok := updates.Updates[0].(*tg.UpdateNewMessage)
	if !ok || update.Pts != msg.Pts {
		t.Fatalf("update = %#v, want UpdateNewMessage pts=%d", updates.Updates[0], msg.Pts)
	}
	if metrics.claimed != 1 || metrics.delivered != 1 || metrics.failed != 0 {
		t.Fatalf("metrics = claimed %d delivered %d failed %d, want 1/1/0", metrics.claimed, metrics.delivered, metrics.failed)
	}
}

func TestOutboxDispatcherUsesScopedAuthKeyExclusion(t *testing.T) {
	var excludeAuthKeyID [8]byte
	excludeAuthKeyID[0] = 7
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               57,
		TargetUserID:     1000000002,
		Pts:              9,
		EventType:        domain.UpdateEventPeerSettings,
		ExcludeAuthKeyID: excludeAuthKeyID,
		ExcludeSessionID: 99,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   1000000002,
		Type:     domain.UpdateEventPeerSettings,
		Pts:      9,
		PtsCount: 1,
		Date:     1700000302,
		Peer:     peer,
	}}}
	sessions := &captureScopedSessions{captureSessions: &captureSessions{}}
	dispatcher := NewOutboxDispatcher(events, outbox, sessions, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if sessions.scopedAuthKeyID != excludeAuthKeyID || sessions.sessionID != 99 || sessions.userID != 1000000002 {
		t.Fatalf("scoped push = auth %x session %d user %d, want precise outbox exclusion", sessions.scopedAuthKeyID, sessions.sessionID, sessions.userID)
	}
}

// TestOutboxDispatcherBatchPath 覆盖生产批量路径：store 同时具备 BatchByCursor + MarkDeliveredBatch
// 时，DispatchOnce 一次批量取事件、推送、再批量标记 delivered，而非逐条。
func TestOutboxDispatcherBatchPath(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &batchEventStore{captureUpdateEventStore: &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}}
	outbox := &batchDispatchOutbox{captureDispatchOutbox: &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeSessionID: 99,
	}}}}
	sessions := &captureSessions{}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewOutboxDispatcher(events, outbox, sessions, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if len(events.batchCursors) != 1 || events.batchCursors[0] != (store.EventCursor{UserID: msg.OwnerUserID, Pts: msg.Pts}) {
		t.Fatalf("batch cursors = %+v, want one cursor for (%d,%d)", events.batchCursors, msg.OwnerUserID, msg.Pts)
	}
	if sessions.userID != msg.OwnerUserID || sessions.sessionID != 99 {
		t.Fatalf("push target = user %d exclude %d, want batch push to outbox target", sessions.userID, sessions.sessionID)
	}
	if len(outbox.deliveredBatch) != 1 || outbox.deliveredBatch[0].ID != 55 {
		t.Fatalf("delivered batch = %+v, want one item id=55", outbox.deliveredBatch)
	}
	if outbox.delivered {
		t.Fatalf("batch path should not call per-item MarkDelivered")
	}
	if metrics.claimed != 1 || metrics.delivered != 1 || metrics.failed != 0 {
		t.Fatalf("metrics = claimed %d delivered %d failed %d, want 1/1/0", metrics.claimed, metrics.delivered, metrics.failed)
	}
}

func TestOutboxDispatcherUsesBestEffortPush(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeSessionID: 99,
	}}}
	sessions := &captureBestEffortSessions{captureSessions: &captureSessions{}}
	dispatcher := NewOutboxDispatcher(events, outbox, sessions, zaptest.NewLogger(t), WithOutboxPushTimeout(50*time.Millisecond))
	dispatcher.DispatchOnce(context.Background())

	if !sessions.bestEffort || sessions.timeout != 50*time.Millisecond {
		t.Fatalf("best-effort push = %v timeout %v, want true/50ms", sessions.bestEffort, sessions.timeout)
	}
	if !outbox.delivered || outbox.failed {
		t.Fatalf("outbox delivered=%v failed=%v, want delivered after accepted best-effort push", outbox.delivered, outbox.failed)
	}
}

type captureBestEffortSessions struct {
	*captureSessions
	bestEffort bool
	timeout    time.Duration
}

func (s *captureBestEffortSessions) PushToUserExceptSessionBestEffort(ctx context.Context, userID, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error) {
	s.bestEffort = true
	s.timeout = timeout
	return s.PushToUserExceptSession(ctx, userID, excludeSessionID, t, msg)
}

// batchEventStore 给 captureUpdateEventStore 加上 BatchByCursor 批量能力。
type batchEventStore struct {
	*captureUpdateEventStore
	batchCursors []store.EventCursor
}

func (s *batchEventStore) BatchByCursor(_ context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
	s.batchCursors = cursors
	out := make([]domain.UpdateEvent, 0, len(cursors))
	for _, c := range cursors {
		for _, event := range s.events {
			if event.UserID == c.UserID && event.Pts == c.Pts {
				out = append(out, event)
			}
		}
	}
	return out, nil
}

// batchDispatchOutbox 给 captureDispatchOutbox 加上 MarkDeliveredBatch 批量能力。
type batchDispatchOutbox struct {
	*captureDispatchOutbox
	deliveredBatch []store.DispatchOutboxItem
}

func (s *batchDispatchOutbox) MarkDeliveredBatch(_ context.Context, items []store.DispatchOutboxItem) error {
	s.deliveredBatch = append(s.deliveredBatch, items...)
	return nil
}

type captureUpdateEventStore struct {
	events []domain.UpdateEvent
}

func (s *captureUpdateEventStore) Append(context.Context, int64, domain.UpdateEvent) error {
	return nil
}

func (s *captureUpdateEventStore) ListAfter(_ context.Context, _ int64, pts, limit int) ([]domain.UpdateEvent, error) {
	out := make([]domain.UpdateEvent, 0, len(s.events))
	for _, event := range s.events {
		if event.Pts > pts {
			out = append(out, event)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (s *captureUpdateEventStore) Current(context.Context, int64) (int, error) {
	maxPts := 0
	for _, event := range s.events {
		if event.Pts > maxPts {
			maxPts = event.Pts
		}
	}
	return maxPts, nil
}

func (s *captureUpdateEventStore) MaxContiguousPts(context.Context, int64) (int, error) {
	present := make(map[int]struct{}, len(s.events))
	for _, event := range s.events {
		present[event.Pts] = struct{}{}
	}
	contiguous := 0
	for {
		if _, ok := present[contiguous+1]; !ok {
			break
		}
		contiguous++
	}
	return contiguous, nil
}

func (s *captureUpdateEventStore) AdvanceContiguousPts(ctx context.Context, userID int64) (int, error) {
	return s.MaxContiguousPts(ctx, userID)
}

type captureDispatchOutbox struct {
	items           []store.DispatchOutboxItem
	delivered       bool
	deliveredUserID int64
	deliveredID     int64
	failed          bool
	failedError     string
}

type captureScopedSessions struct {
	*captureSessions
	scopedAuthKeyID [8]byte
}

func (s *captureScopedSessions) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	s.BindAuthKey(sessionID, authKeyID)
	s.scopedAuthKeyID = rawAuthKeyID
}

func (s *captureScopedSessions) AuthKeyIDForSession([8]byte, int64) ([8]byte, bool) {
	return s.AuthKeyID(0)
}

func (s *captureScopedSessions) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	s.BindUser(sessionID, userID)
	s.scopedAuthKeyID = rawAuthKeyID
}

func (s *captureScopedSessions) UserIDForAuthKey([8]byte, int64) (int64, bool) {
	return s.UserID(0)
}

func (s *captureScopedSessions) UserIDResolvedForAuthKey([8]byte, int64) (int64, bool) {
	return s.UserIDResolved(0)
}

func (s *captureScopedSessions) SetReceivesUpdatesForAuthKey([8]byte, int64, bool) {}

func (s *captureScopedSessions) PushToSessionForAuthKey(_ context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg bin.Encoder) error {
	s.scopedAuthKeyID = rawAuthKeyID
	return s.PushToSession(context.Background(), sessionID, t, msg)
}

func (s *captureScopedSessions) PushToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error) {
	s.scopedAuthKeyID = excludeAuthKeyID
	return s.PushToUserExceptSession(context.Background(), userID, excludeSessionID, t, msg)
}

func (s *captureDispatchOutbox) ClaimPending(context.Context, int) ([]store.DispatchOutboxItem, error) {
	items := s.items
	s.items = nil
	return items, nil
}

func (s *captureDispatchOutbox) MarkDelivered(_ context.Context, targetUserID, id int64) error {
	s.delivered = true
	s.deliveredUserID = targetUserID
	s.deliveredID = id
	return nil
}

func (s *captureDispatchOutbox) MarkFailed(_ context.Context, _ int64, _ int64, lastError string) error {
	s.failed = true
	s.failedError = lastError
	return nil
}

func (s *captureDispatchOutbox) DeleteFailed(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

func TestOutboxDispatcherUsesNoopAsDelivered(t *testing.T) {
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:           56,
		TargetUserID: 1000000002,
		Pts:          8,
		EventType:    domain.UpdateEventNoop,
	}}}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID: 1000000002,
		Type:   domain.UpdateEventNoop,
		Pts:    8,
		Date:   1700000301,
	}}}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewOutboxDispatcher(events, outbox, &captureSessions{}, zaptest.NewLogger(t), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if !outbox.delivered || outbox.failed {
		t.Fatalf("noop delivered=%v failed=%v, want delivered without push", outbox.delivered, outbox.failed)
	}
	if metrics.delivered != 1 {
		t.Fatalf("noop delivered metrics = %d, want 1", metrics.delivered)
	}
}

type captureOutboxMetrics struct {
	claimed   int
	delivered int
	failed    int
}

func (m *captureOutboxMetrics) MessageSend(time.Duration, bool, error) {}

func (m *captureOutboxMetrics) MessageRateLimited(int) {}

func (m *captureOutboxMetrics) OutboxClaimed(count int) {
	m.claimed += count
}

func (m *captureOutboxMetrics) OutboxDelivered(time.Duration) {
	m.delivered++
}

func (m *captureOutboxMetrics) OutboxFailed(error) {
	m.failed++
}

// queueFullBestEffortSessions 模拟出站队列拥塞：best-effort 推送总是失败（入队超时 / 队列满）。
type queueFullBestEffortSessions struct {
	*captureSessions
	attempts int
}

func (s *queueFullBestEffortSessions) PushToUserExceptSessionBestEffort(_ context.Context, _ int64, _ int64, _ proto.MessageType, _ bin.Encoder, _ time.Duration) (int, error) {
	s.attempts++
	return 0, errors.New("mtproto outbound queue full")
}

// TestOutboxDispatcherDefersOnPushQueueFull 验证 best-effort 推送因出站队列拥塞失败时，dispatcher
// 既不标记 delivered（任务保留，靠 dispatching 租约过期重投，满足至少一次投递语义），也不标记
// failed（拥塞不计入 attempts 升级，避免正常满 fan-out 负载把可靠 update 误打成 failed）。
func TestOutboxDispatcherDefersOnPushQueueFull(t *testing.T) {
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000002,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: 1000000001},
		Date:        1700000300,
		Body:        "hello",
		Pts:         7,
	}
	events := &captureUpdateEventStore{events: []domain.UpdateEvent{{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
		Users:    []domain.User{{ID: msg.From.ID, FirstName: "Sender"}},
	}}}
	outbox := &captureDispatchOutbox{items: []store.DispatchOutboxItem{{
		ID:               55,
		TargetUserID:     msg.OwnerUserID,
		Pts:              msg.Pts,
		EventType:        domain.UpdateEventNewMessage,
		ExcludeSessionID: 99,
	}}}
	sessions := &queueFullBestEffortSessions{captureSessions: &captureSessions{}}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewOutboxDispatcher(events, outbox, sessions, zaptest.NewLogger(t), WithOutboxPushTimeout(50*time.Millisecond), WithOutboxMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if sessions.attempts != 1 {
		t.Fatalf("best-effort push attempts = %d, want 1（应走 best-effort 推送路径）", sessions.attempts)
	}
	if outbox.delivered {
		t.Fatalf("outbox delivered=true, want 未投递（拥塞应保留 dispatching 行靠租约重投）")
	}
	if outbox.failed {
		t.Fatalf("outbox failed=true, want 未失败（拥塞不计入 attempts 升级）")
	}
	if metrics.failed != 0 {
		t.Fatalf("metrics.failed=%d, want 0（拥塞不算投递失败）", metrics.failed)
	}
}
