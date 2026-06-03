package postgres

import (
	"context"
	"sync"
	"testing"

	"telesrv/internal/domain"
)

func TestMessageStoreSendPrivateTextRoundTrip(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{
		AccessHash: 11,
		Phone:      "+1666" + suffix + "01",
		FirstName:  "Sender",
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{
		AccessHash: 22,
		Phone:      "+1666" + suffix + "02",
		FirstName:  "Recipient",
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := NewMessageStore(pool)
	var originAuthKeyID [8]byte
	originAuthKeyID[0] = 5
	req := domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        123456,
		Message:         "hello from pg",
		Entities:        []domain.MessageEntity{{Type: domain.MessageEntityBold, Offset: 0, Length: 5}},
		Date:            1700000200,
		OriginAuthKeyID: originAuthKeyID,
		OriginSessionID: 77,
	}
	got, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	if got.SenderMessage.ID != 1 || got.SenderMessage.Pts != 1 || !got.SenderMessage.Out || got.SenderMessage.Peer.ID != recipient.ID {
		t.Fatalf("sender message = %+v, want first outgoing box to recipient", got.SenderMessage)
	}
	if got.RecipientMessage.ID != 1 || got.RecipientMessage.Pts != 1 || got.RecipientMessage.Out || got.RecipientMessage.Peer.ID != sender.ID {
		t.Fatalf("recipient message = %+v, want first incoming box from sender", got.RecipientMessage)
	}
	if got.SenderMessage.UID == 0 || got.SenderMessage.UID != got.RecipientMessage.UID {
		t.Fatalf("uid = sender %d recipient %d, want shared private message uid", got.SenderMessage.UID, got.RecipientMessage.UID)
	}

	senderHistory, err := messages.ListByUser(ctx, sender.ID, domain.MessageFilter{HasPeer: true, Peer: got.SenderMessage.Peer, Limit: 10})
	if err != nil {
		t.Fatalf("sender history: %v", err)
	}
	recipientHistory, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{HasPeer: true, Peer: got.RecipientMessage.Peer, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(senderHistory.Messages) != 1 || len(recipientHistory.Messages) != 1 {
		t.Fatalf("history sizes = sender %d recipient %d, want both owner partitions populated", len(senderHistory.Messages), len(recipientHistory.Messages))
	}

	events, err := NewUpdateEventStore(pool).ListAfter(ctx, recipient.ID, 0, 10)
	if err != nil {
		t.Fatalf("list recipient events: %v", err)
	}
	if len(events) != 1 || events[0].Message.ID != got.RecipientMessage.ID || len(events[0].Users) != 1 || events[0].Users[0].ID != sender.ID {
		t.Fatalf("recipient events = %+v, want new message with sender user", events)
	}

	var pendingOutbox int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM dispatch_outbox
		WHERE target_user_id = ANY($1::bigint[])
		  AND status = 'pending'
	`, []int64{sender.ID, recipient.ID}).Scan(&pendingOutbox); err != nil {
		t.Fatalf("count dispatch outbox: %v", err)
	}
	if pendingOutbox != 2 {
		t.Fatalf("pending outbox = %d, want sender + recipient dispatch rows", pendingOutbox)
	}
	var excludeAuthKeyID, excludeSessionID int64
	if err := pool.QueryRow(ctx, `
		SELECT exclude_auth_key_id, exclude_session_id
		FROM dispatch_outbox
		WHERE target_user_id = $1
	`, sender.ID).Scan(&excludeAuthKeyID, &excludeSessionID); err != nil {
		t.Fatalf("sender dispatch outbox: %v", err)
	}
	if excludeAuthKeyID != authKeyIDToInt64(originAuthKeyID) || excludeSessionID != 77 {
		t.Fatalf("sender dispatch exclude = auth %d session %d, want origin auth/session", excludeAuthKeyID, excludeSessionID)
	}

	dup, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("SendPrivateText duplicate: %v", err)
	}
	if !dup.Duplicate || dup.SenderMessage.ID != got.SenderMessage.ID || dup.RecipientMessage.ID != got.RecipientMessage.ID {
		t.Fatalf("duplicate = %+v, want original message boxes", dup)
	}
}

func TestMessageStoreListByUserSupportsForwardAndAroundHistoryOffsets(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice, err := users.Create(ctx, domain.User{
		AccessHash: 91,
		Phone:      "+1667" + suffix + "01",
		FirstName:  "Alice",
	})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{
		AccessHash: 92,
		Phone:      "+1667" + suffix + "02",
		FirstName:  "Bob",
	})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{alice.ID, bob.ID})
	})

	messages := NewMessageStore(pool)
	for i := 1; i <= 6; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    alice.ID,
			RecipientUserID: bob.ID,
			RandomID:        int64(700 + i),
			Message:         "history",
			Date:            1700000000 + i,
		}); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID}

	around, err := messages.ListByUser(ctx, bob.ID, domain.MessageFilter{
		HasPeer:        true,
		Peer:           peer,
		OffsetID:       3,
		AddOffset:      -3,
		Limit:          6,
		NeedTotalCount: true,
	})
	if err != nil {
		t.Fatalf("around history: %v", err)
	}
	if got := messageIDs(around.Messages); !sameInts(got, []int{6, 5, 4, 3, 2, 1}) {
		t.Fatalf("around ids = %v, want unread/newer side plus older context", got)
	}
	if around.Count != 6 {
		t.Fatalf("around count = %d, want full dialog count", around.Count)
	}

	forward, err := messages.ListByUser(ctx, bob.ID, domain.MessageFilter{
		HasPeer:        true,
		Peer:           peer,
		OffsetID:       3,
		AddOffset:      -3,
		Limit:          3,
		NeedTotalCount: true,
	})
	if err != nil {
		t.Fatalf("forward history: %v", err)
	}
	if got := messageIDs(forward.Messages); !sameInts(got, []int{6, 5, 4}) {
		t.Fatalf("forward ids = %v, want messages newer than offset", got)
	}
	if forward.Count != 6 {
		t.Fatalf("forward count = %d, want full dialog count", forward.Count)
	}
}

func TestMessageStoreReadAndEditEmitDurableEvents(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{
		AccessHash: 31,
		Phone:      "+1666" + suffix + "11",
		FirstName:  "ReadSender",
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{
		AccessHash: 32,
		Phone:      "+1666" + suffix + "12",
		FirstName:  "ReadRecipient",
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := NewMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        223344,
		Message:         "before edit",
		Date:            1700000300,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	read, err := messages.ReadHistory(ctx, domain.ReadHistoryRequest{
		OwnerUserID: recipient.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID},
		Date:        1700000310,
	})
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if !read.Changed || read.InboxEvent.Pts != 2 || read.InboxEvent.Type != domain.UpdateEventReadHistoryInbox || read.InboxEvent.MaxID != sent.RecipientMessage.ID {
		t.Fatalf("read inbox = %+v, want recipient pts=2 max recipient id", read)
	}
	if !read.OutboxChanged || read.OutboxEvent.Pts != 2 || read.OutboxEvent.Type != domain.UpdateEventReadHistoryOutbox || read.OutboxEvent.MaxID != sent.SenderMessage.ID {
		t.Fatalf("read outbox = %+v, want sender pts=2 max sender id", read)
	}
	readDate, err := messages.GetOutboxReadDate(ctx, domain.OutboxReadDateRequest{
		OwnerUserID: sender.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
		ID:          sent.SenderMessage.ID,
	})
	if err != nil || readDate != 1700000310 {
		t.Fatalf("outbox read date = %d err=%v, want read date", readDate, err)
	}

	edited, err := messages.EditMessage(ctx, domain.EditMessageRequest{
		OwnerUserID: sender.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
		ID:          sent.SenderMessage.ID,
		Message:     "after edit",
		EditDate:    1700000320,
	})
	if err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if self := edited.Self(); self.Event.Pts != 3 || self.Event.Type != domain.UpdateEventEditMessage || self.Message.Body != "after edit" {
		t.Fatalf("self edit = %+v, want sender edit event pts=3", self)
	}
	recipientHistory, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{
		HasPeer: true,
		Peer:    domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(recipientHistory.Messages) != 1 || recipientHistory.Messages[0].Body != "after edit" || recipientHistory.Messages[0].EditDate != 1700000320 {
		t.Fatalf("recipient history = %+v, want edited message visible", recipientHistory.Messages)
	}

	senderEvents, err := NewUpdateEventStore(pool).ListAfter(ctx, sender.ID, 0, 10)
	if err != nil {
		t.Fatalf("sender events: %v", err)
	}
	if len(senderEvents) != 3 || senderEvents[1].Type != domain.UpdateEventReadHistoryOutbox || senderEvents[2].Type != domain.UpdateEventEditMessage {
		t.Fatalf("sender events = %+v, want new/read_outbox/edit", senderEvents)
	}
	recipientEvents, err := NewUpdateEventStore(pool).ListAfter(ctx, recipient.ID, 0, 10)
	if err != nil {
		t.Fatalf("recipient events: %v", err)
	}
	if len(recipientEvents) != 3 || recipientEvents[1].Type != domain.UpdateEventReadHistoryInbox || recipientEvents[2].Type != domain.UpdateEventEditMessage || recipientEvents[2].Message.Body != "after edit" {
		t.Fatalf("recipient events = %+v, want new/read_inbox/edit with edited body", recipientEvents)
	}
}

func TestMessageStoreSendPrivateTextRollbackRecordsPtsNoop(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{
		AccessHash: 31,
		Phone:      "+1777" + suffix + "01",
		FirstName:  "GapSender",
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{
		AccessHash: 32,
		Phone:      "+1777" + suffix + "02",
		FirstName:  "GapRecipient",
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := NewMessageStore(pool)
	if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        223344,
		Message:         "seed box",
		Date:            1700000210,
	}); err != nil {
		t.Fatalf("seed SendPrivateText: %v", err)
	}

	failing := NewMessageStore(pool, WithMessageAllocators(fixedBoxIDAllocator{next: 1}, fixedPtsAllocator{next: 42}))
	_, err = failing.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        223345,
		Message:         "should roll back",
		Date:            1700000211,
	})
	if err == nil {
		t.Fatal("SendPrivateText succeeded, want box id conflict")
	}

	events, err := NewUpdateEventStore(pool).ListAfter(ctx, sender.ID, 1, 10)
	if err != nil {
		t.Fatalf("list sender events: %v", err)
	}
	for _, event := range events {
		if event.Pts == 42 && event.Type == domain.UpdateEventNoop {
			return
		}
	}
	t.Fatalf("events = %+v, want noop gap at pts=42", events)
}

func TestMessageStoreConcurrentRandomIDIdempotent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1888" + suffix + "01",
		FirstName:  "ConcurrentSender",
	})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1888" + suffix + "02",
		FirstName:  "ConcurrentRecipient",
	})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	boxCounters := &perUserCounterAllocator{}
	ptsCounters := &perUserCounterAllocator{}
	messages := NewMessageStore(pool, WithMessageAllocators(boxCounters, ptsCounters))
	req := domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        556677,
		Message:         "same random id",
		Date:            1700000220,
	}

	const workers = 8
	results := make(chan domain.SendPrivateTextResult, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := messages.SendPrivateText(ctx, req)
			if err != nil {
				errs <- err
				return
			}
			results <- res
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("SendPrivateText: %v", err)
	}
	var uid int64
	duplicates := 0
	successes := 0
	for res := range results {
		if res.SenderMessage.UID == 0 || res.RecipientMessage.UID == 0 {
			t.Fatalf("result = %+v, want populated shared message uid", res)
		}
		if uid == 0 {
			uid = res.SenderMessage.UID
		}
		if res.SenderMessage.UID != uid || res.RecipientMessage.UID != uid {
			t.Fatalf("result = %+v, want same private message uid %d", res, uid)
		}
		if res.Duplicate {
			duplicates++
		} else {
			successes++
		}
	}
	if successes != 1 || duplicates != workers-1 {
		t.Fatalf("successes=%d duplicates=%d, want one insert and duplicate rest", successes, duplicates)
	}

	var privateCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM private_messages
		WHERE sender_user_id = $1
		  AND random_id = $2
	`, sender.ID, req.RandomID).Scan(&privateCount); err != nil {
		t.Fatalf("count private_messages: %v", err)
	}
	if privateCount != 1 {
		t.Fatalf("private message count = %d, want 1", privateCount)
	}
	var boxCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM message_boxes
		WHERE private_message_id = $1
	`, uid).Scan(&boxCount); err != nil {
		t.Fatalf("count message boxes: %v", err)
	}
	if boxCount != 2 {
		t.Fatalf("message box count = %d, want sender + recipient boxes", boxCount)
	}
}

func TestMessageStoreDeleteHistoryRebuildsDialogAndEmitsDeleteUpdates(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender := createTestUser(t, ctx, users, "+1991"+suffix+"01", "DeleteSender", "")
	recipient := createTestUser(t, ctx, users, "+1991"+suffix+"02", "DeleteRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := NewMessageStore(pool)
	for i := 0; i < 2; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    sender.ID,
			RecipientUserID: recipient.ID,
			RandomID:        int64(7000 + i),
			Message:         "history",
			Date:            1700000700 + i,
		}); err != nil {
			t.Fatalf("seed send %d: %v", i, err)
		}
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID}
	deleted, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: sender.ID,
		Peer:        peer,
		Date:        1700000800,
	})
	if err != nil {
		t.Fatalf("DeleteHistory: %v", err)
	}
	if self := deleted.Self(); self.Event.Pts != 4 || self.Event.PtsCount != 2 || len(self.MessageIDs) != 2 {
		t.Fatalf("delete result = %+v, want sender delete range pts=4 count=2 ids", self)
	}
	senderHistory, err := messages.ListByUser(ctx, sender.ID, domain.MessageFilter{HasPeer: true, Peer: peer, Limit: 10})
	if err != nil {
		t.Fatalf("sender history: %v", err)
	}
	recipientHistory, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID}, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(senderHistory.Messages) != 0 || len(recipientHistory.Messages) != 2 {
		t.Fatalf("history sizes sender=%d recipient=%d, want sender cleared only", len(senderHistory.Messages), len(recipientHistory.Messages))
	}
	senderDialogs, err := NewDialogStore(pool).ListByUser(ctx, sender.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("sender dialogs after delete: %v", err)
	}
	if len(senderDialogs.Dialogs) != 0 {
		t.Fatalf("sender dialogs = %+v, want empty after full history delete", senderDialogs.Dialogs)
	}
	events, err := NewUpdateEventStore(pool).ListAfter(ctx, sender.ID, 2, 10)
	if err != nil {
		t.Fatalf("list sender events: %v", err)
	}
	if len(events) != 1 || events[0].Type != domain.UpdateEventDeleteMessages || events[0].Pts != 4 || events[0].PtsCount != 2 || len(events[0].MessageIDs) != 2 {
		t.Fatalf("events = %+v, want delete messages event pts=4 pts_count=2", events)
	}

	rebuilt, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        8000,
		Message:         "after clear",
		Date:            1700000900,
	})
	if err != nil {
		t.Fatalf("send after delete: %v", err)
	}
	senderDialogs, err = NewDialogStore(pool).ListByUser(ctx, sender.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("sender dialogs after rebuild: %v", err)
	}
	if len(senderDialogs.Dialogs) != 1 || senderDialogs.Dialogs[0].Peer != peer || senderDialogs.Dialogs[0].TopMessage != rebuilt.SenderMessage.ID {
		t.Fatalf("rebuilt dialogs = %+v, want new top message %d", senderDialogs.Dialogs, rebuilt.SenderMessage.ID)
	}

	revoked, err := messages.DeleteMessages(ctx, domain.DeleteMessagesRequest{
		OwnerUserID: sender.ID,
		IDs:         []int{rebuilt.SenderMessage.ID},
		Revoke:      true,
		Date:        1700001000,
	})
	if err != nil {
		t.Fatalf("DeleteMessages revoke: %v", err)
	}
	if len(revoked.Deleted) != 2 || !revoked.Changed() {
		t.Fatalf("revoked = %+v, want delete events for both owners", revoked)
	}
	senderHistory, err = messages.ListByUser(ctx, sender.ID, domain.MessageFilter{HasPeer: true, Peer: peer, Limit: 10})
	if err != nil {
		t.Fatalf("sender history after revoke: %v", err)
	}
	recipientHistory, err = messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID}, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history after revoke: %v", err)
	}
	if len(senderHistory.Messages) != 0 || len(recipientHistory.Messages) != 2 {
		t.Fatalf("history sizes after revoke sender=%d recipient=%d, want new message removed from both owners", len(senderHistory.Messages), len(recipientHistory.Messages))
	}
}

func TestMessageStoreDeleteHistoryJustClearPreservesEmptyDialog(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner := createTestUser(t, ctx, users, "+1992"+suffix+"01", "ClearOwner", "")
	peerUser := createTestUser(t, ctx, users, "+1992"+suffix+"02", "ClearPeer", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, peerUser.ID})
	})

	messages := NewMessageStore(pool)
	if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    owner.ID,
		RecipientUserID: peerUser.ID,
		RandomID:        9000,
		Message:         "clear but keep dialog",
		Date:            1700001100,
	}); err != nil {
		t.Fatalf("seed send: %v", err)
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: peerUser.ID}
	if _, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: owner.ID,
		Peer:        peer,
		JustClear:   true,
		Date:        1700001200,
	}); err != nil {
		t.Fatalf("DeleteHistory just_clear: %v", err)
	}
	dialogs, err := NewDialogStore(pool).ListByUser(ctx, owner.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("dialogs after just_clear: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].Peer != peer || dialogs.Dialogs[0].TopMessage != 0 || len(dialogs.Messages) != 0 {
		t.Fatalf("dialogs = %+v messages=%+v, want empty dialog preserved after just_clear", dialogs.Dialogs, dialogs.Messages)
	}
	history, err := messages.ListByUser(ctx, owner.ID, domain.MessageFilter{HasPeer: true, Peer: peer, Limit: 10, NeedTotalCount: true})
	if err != nil {
		t.Fatalf("history after just_clear: %v", err)
	}
	if len(history.Messages) != 0 {
		t.Fatalf("history = %+v, want cleared", history.Messages)
	}
}

func TestMessageStoreDeleteHistoryBatchesHugeMaxID(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner := createTestUser(t, ctx, users, "+1993"+suffix+"01", "BulkOwner", "")
	peerUser := createTestUser(t, ctx, users, "+1993"+suffix+"02", "BulkPeer", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, peerUser.ID})
	})

	total := domain.MaxDeleteHistoryBatch + 2
	if _, err := pool.Exec(ctx, `
		WITH src AS (
		  SELECT generate_series(1, $3::int) AS g
		),
		pm AS (
		  INSERT INTO private_messages (
		    sender_user_id,
		    recipient_user_id,
		    random_id,
		    message_date,
		    body,
		    entities
		  )
		  SELECT
		    $1::bigint,
		    $2::bigint,
		    910000000 + g,
		    1700002000 + g,
		    'bulk history',
		    '[]'::jsonb
		  FROM src
		  RETURNING id, random_id, message_date
		)
		INSERT INTO message_boxes (
		  owner_user_id,
		  box_id,
		  private_message_id,
		  message_sender_id,
		  peer_type,
		  peer_id,
		  from_user_id,
		  message_date,
		  outgoing,
		  body,
		  entities,
		  pts
		)
		SELECT
		  $1::bigint,
		  (random_id - 910000000)::int,
		  id,
		  $1::bigint,
		  'user',
		  $2::bigint,
		  $1::bigint,
		  message_date,
		  true,
		  'bulk history',
		  '[]'::jsonb,
		  0
		FROM pm
	`, owner.ID, peerUser.ID, total); err != nil {
		t.Fatalf("seed bulk history: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO dialogs (
		  user_id,
		  peer_type,
		  peer_id,
		  top_message_id,
		  top_message_date,
		  read_outbox_max_id,
		  unread_count
		) VALUES ($1, 'user', $2, $3, $4, $3, 0)
	`, owner.ID, peerUser.ID, total, 1700002000+total); err != nil {
		t.Fatalf("seed dialog: %v", err)
	}

	messages := NewMessageStore(pool)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: peerUser.ID}
	first, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: owner.ID,
		Peer:        peer,
		MaxID:       domain.MaxMessageBoxID,
		Date:        1700003000,
	})
	if err != nil {
		t.Fatalf("DeleteHistory first batch: %v", err)
	}
	self := first.Self()
	if first.Offset != 1 || self.Event.Pts != domain.MaxDeleteHistoryBatch || self.Event.PtsCount != domain.MaxDeleteHistoryBatch || len(self.MessageIDs) != domain.MaxDeleteHistoryBatch {
		t.Fatalf("first batch = %+v self=%+v, want offset=1 and exactly %d deleted ids", first, self, domain.MaxDeleteHistoryBatch)
	}
	history, err := messages.ListByUser(ctx, owner.ID, domain.MessageFilter{HasPeer: true, Peer: peer, Limit: 10, NeedTotalCount: true})
	if err != nil {
		t.Fatalf("history after first batch: %v", err)
	}
	if history.Count != 2 || len(history.Messages) != 2 || history.Messages[0].ID != 2 {
		t.Fatalf("history after first batch = %+v, want only two oldest messages left", history)
	}

	second, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: owner.ID,
		Peer:        peer,
		MaxID:       domain.MaxMessageBoxID,
		Date:        1700003001,
	})
	if err != nil {
		t.Fatalf("DeleteHistory second batch: %v", err)
	}
	if second.Offset != 0 || second.Self().Event.PtsCount != 2 {
		t.Fatalf("second batch = %+v, want final offset=0 pts_count=2", second)
	}
}

type fixedBoxIDAllocator struct {
	next int
}

func (a fixedBoxIDAllocator) NextBoxID(context.Context, int64) (int, error) {
	return a.next, nil
}

func (a fixedBoxIDAllocator) CurrentBoxID(context.Context, int64) (int, error) {
	return a.next, nil
}

type fixedPtsAllocator struct {
	next int
}

func (a fixedPtsAllocator) NextPts(context.Context, int64) (int, error) {
	return a.next, nil
}

func (a fixedPtsAllocator) CurrentPts(context.Context, int64) (int, error) {
	return a.next, nil
}

type perUserCounterAllocator struct {
	mu     sync.Mutex
	values map[int64]int
}

func (a *perUserCounterAllocator) NextBoxID(_ context.Context, userID int64) (int, error) {
	return a.next(userID), nil
}

func (a *perUserCounterAllocator) CurrentBoxID(_ context.Context, userID int64) (int, error) {
	return a.current(userID), nil
}

func (a *perUserCounterAllocator) NextPts(_ context.Context, userID int64) (int, error) {
	return a.next(userID), nil
}

func (a *perUserCounterAllocator) CurrentPts(_ context.Context, userID int64) (int, error) {
	return a.current(userID), nil
}

func (a *perUserCounterAllocator) next(userID int64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.values == nil {
		a.values = map[int64]int{}
	}
	a.values[userID]++
	return a.values[userID]
}

func (a *perUserCounterAllocator) current(userID int64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.values[userID]
}

func messageIDs(messages []domain.Message) []int {
	out := make([]int, 0, len(messages))
	for _, msg := range messages {
		out = append(out, msg.ID)
	}
	return out
}

func sameInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
