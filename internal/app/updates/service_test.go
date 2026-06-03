package updates

import (
	"context"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestRecordNewMessageFeedsGetDifference(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 1
	svc := NewService(memory.NewUpdateStateStore(), memory.NewUpdateEventStore())
	msg := domain.Message{
		ID:          10,
		OwnerUserID: 1000000001,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		Date:        1700000000,
		Body:        "Login code: 12345",
	}

	event, state, err := svc.RecordNewMessage(ctx, authKeyID, msg.OwnerUserID, msg)
	if err != nil {
		t.Fatalf("RecordNewMessage: %v", err)
	}
	if event.Pts != 1 || event.PtsCount != 1 || state.Pts != 1 || state.Seq != 0 {
		t.Fatalf("event/state = %+v / %+v, want first pts event with seq=0", event, state)
	}

	diff, err := svc.GetDifference(ctx, authKeyID, msg.OwnerUserID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.State != state || len(diff.Events) != 1 || diff.Events[0].Message.ID != msg.ID {
		t.Fatalf("diff = %+v, want recorded login message event and state %+v", diff, state)
	}

	diff, err = svc.GetDifference(ctx, authKeyID, msg.OwnerUserID, state)
	if err != nil {
		t.Fatalf("GetDifference current: %v", err)
	}
	if len(diff.Events) != 0 || diff.State != state {
		t.Fatalf("current diff = %+v, want empty events and same state", diff)
	}
}

func TestRecordReadHistoryFeedsGetDifference(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 2
	svc := NewService(memory.NewUpdateStateStore(), memory.NewUpdateEventStore())
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID}

	ownerUserID := int64(1000000001)
	event, state, err := svc.RecordReadHistory(ctx, authKeyID, ownerUserID, domain.ReadHistoryResult{
		OwnerUserID: ownerUserID,
		Peer:        peer,
		MaxID:       10,
		Changed:     true,
	}, 0)
	if err != nil {
		t.Fatalf("RecordReadHistory: %v", err)
	}
	if event.Type != domain.UpdateEventReadHistoryInbox || event.Pts != 1 || event.PtsCount != 1 || state.Pts != 1 {
		t.Fatalf("event/state = %+v / %+v, want read history event with first pts", event, state)
	}

	diff, err := svc.GetDifference(ctx, authKeyID, ownerUserID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.State != state || len(diff.Events) != 1 || diff.Events[0].Peer != peer || diff.Events[0].MaxID != 10 {
		t.Fatalf("diff = %+v, want recorded read history event and state %+v", diff, state)
	}
}

func TestRecordSettingsEventsFeedGetDifference(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 3
	svc := NewService(memory.NewUpdateStateStore(), memory.NewUpdateEventStore())
	ownerUserID := int64(1000000001)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 1000000002}

	if _, _, err := svc.RecordContactsReset(ctx, authKeyID, ownerUserID, 0); err != nil {
		t.Fatalf("RecordContactsReset: %v", err)
	}
	if _, _, err := svc.RecordDialogPinned(ctx, authKeyID, ownerUserID, peer, true, 0); err != nil {
		t.Fatalf("RecordDialogPinned: %v", err)
	}
	order := []domain.Peer{peer}
	if _, _, err := svc.RecordPinnedDialogs(ctx, authKeyID, ownerUserID, order, 0); err != nil {
		t.Fatalf("RecordPinnedDialogs: %v", err)
	}
	if _, _, err := svc.RecordDialogUnreadMark(ctx, authKeyID, ownerUserID, peer, false, 0); err != nil {
		t.Fatalf("RecordDialogUnreadMark: %v", err)
	}
	settings := domain.PeerSettings{ShareContact: true}
	stateEvent, state, err := svc.RecordPeerSettings(ctx, authKeyID, ownerUserID, peer, settings, 0)
	if err != nil {
		t.Fatalf("RecordPeerSettings: %v", err)
	}
	if stateEvent.Pts != 5 || state.Pts != 5 {
		t.Fatalf("last event/state = %+v / %+v, want pts=5", stateEvent, state)
	}

	diff, err := svc.GetDifference(ctx, authKeyID, ownerUserID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.State.Pts != 5 || len(diff.Events) != 5 {
		t.Fatalf("diff = %+v, want five settings events", diff)
	}
	wantTypes := []domain.UpdateEventType{
		domain.UpdateEventContactsReset,
		domain.UpdateEventDialogPinned,
		domain.UpdateEventPinnedDialogs,
		domain.UpdateEventDialogUnreadMark,
		domain.UpdateEventPeerSettings,
	}
	for i, typ := range wantTypes {
		if diff.Events[i].Type != typ || diff.Events[i].Pts != i+1 || diff.Events[i].PtsCount != 1 {
			t.Fatalf("event[%d] = %+v, want type=%s pts=%d pts_count=1", i, diff.Events[i], typ, i+1)
		}
	}
	if diff.Events[1].Peer != peer || !diff.Events[1].Bool {
		t.Fatalf("dialog pinned event = %+v, want peer and pinned=true", diff.Events[1])
	}
	if diff.Events[3].Peer != peer || diff.Events[3].Bool {
		t.Fatalf("unread mark event = %+v, want peer and unread=false", diff.Events[3])
	}
	if len(diff.Events[2].Peers) != 1 || diff.Events[2].Peers[0] != peer {
		t.Fatalf("pinned dialogs event = %+v, want order peer", diff.Events[2])
	}
	if diff.Events[4].Peer != peer || !diff.Events[4].Settings.ShareContact {
		t.Fatalf("peer settings event = %+v, want peer and settings", diff.Events[4])
	}
}

func TestRecordSettingsEventUsesDispatchAppender(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 4
	events := &captureDispatchAppender{UpdateEventStore: memory.NewUpdateEventStore()}
	svc := NewService(memory.NewUpdateStateStore(), events)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 1000000002}

	event, state, err := svc.RecordDialogPinned(ctx, authKeyID, 1000000001, peer, true, 42)
	if err != nil {
		t.Fatalf("RecordDialogPinned: %v", err)
	}
	if event.Pts != 1 || state.Pts != 1 {
		t.Fatalf("event/state = %+v / %+v, want first pts", event, state)
	}
	if !events.dispatched || events.excludeAuthKeyID != authKeyID || events.excludeSessionID != 42 || events.event.Type != domain.UpdateEventDialogPinned || events.event.Peer != peer {
		t.Fatalf("dispatch capture = %+v exclude_auth=%v exclude_session=%d dispatched=%v, want dialog_pinned outbox", events.event, events.excludeAuthKeyID, events.excludeSessionID, events.dispatched)
	}
}

func TestClearAuthKeyDropsStateAndEvents(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 8
	states := memory.NewUpdateStateStore()
	events := memory.NewUpdateEventStore()
	svc := NewService(states, events)
	msg := domain.Message{
		ID:          1,
		OwnerUserID: 1000000001,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		Date:        1700000000,
	}
	if _, _, err := svc.RecordNewMessage(ctx, authKeyID, msg.OwnerUserID, msg); err != nil {
		t.Fatalf("RecordNewMessage: %v", err)
	}
	if err := svc.ClearAuthKey(ctx, authKeyID); err != nil {
		t.Fatalf("ClearAuthKey: %v", err)
	}
	diff, err := svc.GetDifference(ctx, authKeyID, msg.OwnerUserID, domain.UpdateState{})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.State.Pts != 1 || len(diff.Events) != 1 {
		t.Fatalf("difference after clear = %+v, want durable user events to remain", diff)
	}
	diff, err = svc.GetDifference(ctx, authKeyID, msg.OwnerUserID+1, domain.UpdateState{})
	if err != nil {
		t.Fatalf("GetDifference other user: %v", err)
	}
	if diff.State.Pts != 0 || len(diff.Events) != 0 {
		t.Fatalf("difference for other user after clear = %+v, want no cross-account events", diff)
	}
}

func TestDeleteMessagesPtsRangeFeedsGetDifference(t *testing.T) {
	ctx := context.Background()
	var authKeyID [8]byte
	authKeyID[0] = 9
	userID := int64(1000000001)
	events := memory.NewUpdateEventStore()
	svc := NewService(memory.NewUpdateStateStore(), events)
	for _, event := range []domain.UpdateEvent{
		{UserID: userID, Type: domain.UpdateEventNewMessage, Pts: 1, PtsCount: 1, Date: 1700000001, Message: domain.Message{ID: 1, OwnerUserID: userID}},
		{UserID: userID, Type: domain.UpdateEventNewMessage, Pts: 2, PtsCount: 1, Date: 1700000002, Message: domain.Message{ID: 2, OwnerUserID: userID}},
		{UserID: userID, Type: domain.UpdateEventDeleteMessages, Pts: 4, PtsCount: 2, Date: 1700000003, MessageIDs: []int{1, 2}},
	} {
		if err := events.Append(ctx, userID, event); err != nil {
			t.Fatalf("append event pts=%d: %v", event.Pts, err)
		}
	}

	state, err := svc.GetState(ctx, authKeyID, userID)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state.Pts != 4 {
		t.Fatalf("state = %+v, want contiguous pts=4 across delete range", state)
	}
	diff, err := svc.GetDifference(ctx, authKeyID, userID, domain.UpdateState{Pts: 2})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.State.Pts != 4 || len(diff.Events) != 1 {
		t.Fatalf("diff = %+v, want one delete event ending at pts=4", diff)
	}
	got := diff.Events[0]
	if got.Type != domain.UpdateEventDeleteMessages || got.Pts != 4 || got.PtsCount != 2 || len(got.MessageIDs) != 2 {
		t.Fatalf("delete event = %+v, want pts=4 pts_count=2 ids", got)
	}
}

type captureDispatchAppender struct {
	*memory.UpdateEventStore
	dispatched       bool
	userID           int64
	event            domain.UpdateEvent
	excludeAuthKeyID [8]byte
	excludeSessionID int64
}

func (s *captureDispatchAppender) AppendWithDispatch(ctx context.Context, userID int64, event domain.UpdateEvent, excludeAuthKeyID [8]byte, excludeSessionID int64) error {
	s.dispatched = true
	s.userID = userID
	s.event = event
	s.excludeAuthKeyID = excludeAuthKeyID
	s.excludeSessionID = excludeSessionID
	return s.UpdateEventStore.Append(ctx, userID, event)
}
