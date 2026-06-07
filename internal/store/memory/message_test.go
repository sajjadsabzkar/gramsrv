package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"telesrv/internal/domain"
)

func TestMessageStoreSendPrivateTextCreatesBothOwnerBoxes(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	req := domain.SendPrivateTextRequest{
		SenderUserID:    1000000001,
		RecipientUserID: 1000000002,
		RandomID:        99,
		Message:         "hello",
		Date:            1700000100,
	}

	got, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	if got.SenderMessage.ID != 1 || got.SenderMessage.OwnerUserID != req.SenderUserID || !got.SenderMessage.Out || got.SenderMessage.Pts != 1 {
		t.Fatalf("sender message = %+v, want first outgoing box with pts=1", got.SenderMessage)
	}
	if got.RecipientMessage.ID != 1 || got.RecipientMessage.OwnerUserID != req.RecipientUserID || got.RecipientMessage.Out || got.RecipientMessage.Pts != 1 {
		t.Fatalf("recipient message = %+v, want first incoming box with pts=1", got.RecipientMessage)
	}
	if got.SenderMessage.UID == 0 || got.SenderMessage.UID != got.RecipientMessage.UID {
		t.Fatalf("uid = sender %d recipient %d, want shared private message uid", got.SenderMessage.UID, got.RecipientMessage.UID)
	}

	second, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    req.SenderUserID,
		RecipientUserID: req.RecipientUserID,
		RandomID:        100,
		Message:         "again",
		Date:            1700000110,
	})
	if err != nil {
		t.Fatalf("SendPrivateText second: %v", err)
	}
	if second.SenderMessage.ID != 2 || second.SenderMessage.Pts != 2 || second.RecipientMessage.ID != 2 || second.RecipientMessage.Pts != 2 {
		t.Fatalf("second send = %+v/%+v, want per-owner box_id and pts to advance", second.SenderMessage, second.RecipientMessage)
	}

	dup, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("SendPrivateText duplicate: %v", err)
	}
	if !dup.Duplicate || dup.SenderMessage.ID != got.SenderMessage.ID || dup.RecipientMessage.ID != got.RecipientMessage.ID {
		t.Fatalf("duplicate = %+v, want original message boxes", dup)
	}

	senderHistory, err := messages.ListByUser(ctx, req.SenderUserID, domain.MessageFilter{HasPeer: true, Peer: got.SenderMessage.Peer, Limit: 10})
	if err != nil {
		t.Fatalf("sender history: %v", err)
	}
	recipientHistory, err := messages.ListByUser(ctx, req.RecipientUserID, domain.MessageFilter{HasPeer: true, Peer: got.RecipientMessage.Peer, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(senderHistory.Messages) != 2 || len(recipientHistory.Messages) != 2 {
		t.Fatalf("history sizes = sender %d recipient %d, want both owner partitions populated", len(senderHistory.Messages), len(recipientHistory.Messages))
	}
}

func TestMessageStorePrivateMessageReactionsAreSharedAcrossOwnerBoxes(t *testing.T) {
	ctx := context.Background()
	messages := NewMessageStore()
	aliceID := int64(1000000001)
	bobID := int64(1000000002)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    aliceID,
		RecipientUserID: bobID,
		RandomID:        101,
		Message:         "react to me",
		Date:            1700000100,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	reaction := domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: "\U0001f44d"}
	res, err := messages.SetMessageReactions(ctx, domain.SetPrivateMessageReactionsRequest{
		UserID:    bobID,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: aliceID},
		MessageID: sent.RecipientMessage.ID,
		Reactions: []domain.MessageReaction{
			reaction,
		},
		Big:  true,
		Date: 1700000200,
	})
	if err != nil {
		t.Fatalf("SetMessageReactions: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("reaction result messages = %d, want both owner boxes", len(res.Messages))
	}

	aliceReactions, err := messages.GetMessageReactions(ctx, domain.PrivateMessageReactionsRequest{
		OwnerUserID: aliceID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
		IDs:         []int{sent.SenderMessage.ID},
	})
	if err != nil {
		t.Fatalf("alice GetMessageReactions: %v", err)
	}
	if len(aliceReactions.Messages) != 1 || aliceReactions.Messages[0].Reactions == nil {
		t.Fatalf("alice reactions = %+v, want one enriched message", aliceReactions)
	}
	if got := aliceReactions.Messages[0].Reactions.Results; len(got) != 1 || got[0].Reaction != reaction || got[0].Count != 1 || got[0].ChosenOrder != 0 {
		t.Fatalf("alice reaction counts = %+v, want one peer reaction without chosen order", got)
	}
	if got := aliceReactions.Messages[0].Reactions.Recent; len(got) != 1 || got[0].UserID != bobID || !got[0].Big || got[0].My {
		t.Fatalf("alice recent reactions = %+v, want bob non-my big reaction", got)
	}
	aliceBox, err := messages.GetByIDs(ctx, aliceID, []int{sent.SenderMessage.ID})
	if err != nil {
		t.Fatalf("alice GetByIDs after reaction: %v", err)
	}
	if len(aliceBox.Messages) != 1 || !aliceBox.Messages[0].ReactionUnread {
		t.Fatalf("alice box after reaction = %+v, want reaction_unread", aliceBox.Messages)
	}
	read, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: aliceID,
		IDs:         []int{sent.SenderMessage.ID},
		Date:        1700000210,
	})
	if err != nil {
		t.Fatalf("ReadMessageContents reaction: %v", err)
	}
	if !reflect.DeepEqual(read.MessageIDs, []int{sent.SenderMessage.ID}) || read.Event.Type != domain.UpdateEventReadMessageContents || read.Event.Pts == 0 {
		t.Fatalf("read reaction contents = %+v, want one read_message_contents event", read)
	}
	aliceBox, err = messages.GetByIDs(ctx, aliceID, []int{sent.SenderMessage.ID})
	if err != nil {
		t.Fatalf("alice GetByIDs after read reaction: %v", err)
	}
	if len(aliceBox.Messages) != 1 || aliceBox.Messages[0].ReactionUnread {
		t.Fatalf("alice box after read reaction = %+v, want reaction_unread cleared", aliceBox.Messages)
	}

	bobReactions, err := messages.GetMessageReactions(ctx, domain.PrivateMessageReactionsRequest{
		OwnerUserID: bobID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: aliceID},
		IDs:         []int{sent.RecipientMessage.ID},
	})
	if err != nil {
		t.Fatalf("bob GetMessageReactions: %v", err)
	}
	if got := bobReactions.Messages[0].Reactions.Results; len(got) != 1 || got[0].ChosenOrder != 1 {
		t.Fatalf("bob reaction counts = %+v, want own chosen order", got)
	}
	if got := bobReactions.Messages[0].Reactions.Recent; len(got) != 1 || !got[0].My {
		t.Fatalf("bob recent reactions = %+v, want my reaction", got)
	}
}

func TestMessageStoreSendPrivateTextReplyAndForwardMetadata(t *testing.T) {
	ctx := context.Background()
	messages := NewMessageStore()
	aliceID := int64(1000000001)
	bobID := int64(1000000002)
	first, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    aliceID,
		RecipientUserID: bobID,
		RandomID:        501,
		Message:         "first",
		Date:            1700000100,
	})
	if err != nil {
		t.Fatalf("seed SendPrivateText: %v", err)
	}
	reply, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    aliceID,
		RecipientUserID: bobID,
		RandomID:        502,
		Message:         "reply",
		Silent:          true,
		NoForwards:      true,
		ReplyTo: &domain.MessageReply{
			MessageID:   first.SenderMessage.ID,
			Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
			QuoteText:   "fir",
			QuoteOffset: 0,
		},
		Date: 1700000110,
	})
	if err != nil {
		t.Fatalf("reply SendPrivateText: %v", err)
	}
	if reply.SenderMessage.ReplyTo == nil || reply.SenderMessage.ReplyTo.MessageID != first.SenderMessage.ID {
		t.Fatalf("sender reply = %+v, want sender-side message id", reply.SenderMessage.ReplyTo)
	}
	if reply.RecipientMessage.ReplyTo == nil || reply.RecipientMessage.ReplyTo.MessageID != first.RecipientMessage.ID {
		t.Fatalf("recipient reply = %+v, want translated recipient-side message id", reply.RecipientMessage.ReplyTo)
	}
	if !reply.SenderMessage.Silent || !reply.SenderMessage.NoForwards {
		t.Fatalf("reply flags = silent %v noforwards %v, want true/true", reply.SenderMessage.Silent, reply.SenderMessage.NoForwards)
	}
	if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    aliceID,
		RecipientUserID: bobID,
		RandomID:        504,
		Message:         "bad quote offset",
		ReplyTo: &domain.MessageReply{
			MessageID:   first.SenderMessage.ID,
			Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
			QuoteText:   "fir",
			QuoteOffset: domain.MaxMessageReplyQuoteOffset + 1,
		},
		Date: 1700000115,
	}); !errors.Is(err, domain.ErrReplyMessageIDInvalid) {
		t.Fatalf("bad quote offset err = %v, want ErrReplyMessageIDInvalid", err)
	}

	forwarded, err := messages.ForwardPrivateMessages(ctx, domain.ForwardPrivateMessagesRequest{
		OwnerUserID: aliceID,
		FromPeer:    domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
		ToUserID:    bobID,
		MessageIDs:  []int{first.SenderMessage.ID},
		RandomIDs:   []int64{503},
		ReplyTo: &domain.MessageReply{
			MessageID: first.SenderMessage.ID,
			Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
		},
		Date: 1700000120,
	})
	if err != nil {
		t.Fatalf("ForwardPrivateMessages: %v", err)
	}
	if len(forwarded.SenderMessages) != 1 || forwarded.SenderMessages[0].Forward == nil || forwarded.SenderMessages[0].Forward.From.ID != aliceID {
		t.Fatalf("forwarded messages = %+v, want original author header", forwarded.SenderMessages)
	}
	if forwarded.SenderMessages[0].ReplyTo == nil || forwarded.SenderMessages[0].ReplyTo.MessageID != first.SenderMessage.ID {
		t.Fatalf("forward reply = %+v, want target dialog reply header", forwarded.SenderMessages[0].ReplyTo)
	}
	if _, err := messages.ForwardPrivateMessages(ctx, domain.ForwardPrivateMessagesRequest{
		OwnerUserID: aliceID,
		FromPeer:    domain.Peer{Type: domain.PeerTypeUser, ID: bobID},
		ToUserID:    aliceID,
		MessageIDs:  []int{reply.SenderMessage.ID},
		RandomIDs:   []int64{504},
		Date:        1700000130,
	}); err != domain.ErrChatForwardsRestricted {
		t.Fatalf("forward protected err=%v, want ErrChatForwardsRestricted", err)
	}
}

func TestMessageStoreListByUserSupportsForwardAndAroundHistoryOffsets(t *testing.T) {
	ctx := context.Background()
	messages := NewMessageStore()
	aliceID := int64(1000000001)
	bobID := int64(1000000002)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: aliceID}

	for i := 1; i <= 6; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    aliceID,
			RecipientUserID: bobID,
			RandomID:        int64(600 + i),
			Message:         "history",
			Date:            1700000000 + i,
		}); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}

	around, err := messages.ListByUser(ctx, bobID, domain.MessageFilter{
		HasPeer:   true,
		Peer:      peer,
		OffsetID:  3,
		AddOffset: -3,
		Limit:     6,
	})
	if err != nil {
		t.Fatalf("around history: %v", err)
	}
	if got := messageIDs(around.Messages); !sameInts(got, []int{6, 5, 4, 3, 2, 1}) {
		t.Fatalf("around ids = %v, want unread/newer side plus older context", got)
	}

	forward, err := messages.ListByUser(ctx, bobID, domain.MessageFilter{
		HasPeer:   true,
		Peer:      peer,
		OffsetID:  3,
		AddOffset: -3,
		Limit:     3,
	})
	if err != nil {
		t.Fatalf("forward history: %v", err)
	}
	if got := messageIDs(forward.Messages); !sameInts(got, []int{6, 5, 4}) {
		t.Fatalf("forward ids = %v, want messages newer than offset", got)
	}

	hugePositive, err := messages.ListByUser(ctx, bobID, domain.MessageFilter{
		HasPeer:   true,
		Peer:      peer,
		AddOffset: 1 << 30,
		Limit:     3,
	})
	if err != nil {
		t.Fatalf("huge positive add_offset history: %v", err)
	}
	if len(hugePositive.Messages) != 0 {
		t.Fatalf("huge positive add_offset ids = %v, want bounded empty page", messageIDs(hugePositive.Messages))
	}

	hugeNegative, err := messages.ListByUser(ctx, bobID, domain.MessageFilter{
		HasPeer:   true,
		Peer:      peer,
		OffsetID:  3,
		AddOffset: -1 << 30,
		Limit:     3,
	})
	if err != nil {
		t.Fatalf("huge negative add_offset history: %v", err)
	}
	if got := messageIDs(hugeNegative.Messages); !sameInts(got, []int{6, 5, 4}) {
		t.Fatalf("huge negative add_offset ids = %v, want clamped forward page", got)
	}
}

func TestMessageStoreReadHistoryEmitsInboxAndOutboxReceipts(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	senderID := int64(1000000001)
	recipientID := int64(1000000002)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		RandomID:        101,
		Message:         "hello",
		Date:            1700000100,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}

	read, err := messages.ReadHistory(ctx, domain.ReadHistoryRequest{
		OwnerUserID: recipientID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: senderID},
		Date:        1700000200,
	})
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if !read.Changed || read.InboxEvent.Type != domain.UpdateEventReadHistoryInbox || read.InboxEvent.Pts != 2 || read.InboxEvent.MaxID != sent.RecipientMessage.ID {
		t.Fatalf("inbox read = %+v, want recipient inbox pts=2 max recipient id", read)
	}
	if !read.OutboxChanged || read.OutboxUserID != senderID || read.OutboxEvent.Type != domain.UpdateEventReadHistoryOutbox || read.OutboxEvent.MaxID != sent.SenderMessage.ID {
		t.Fatalf("outbox read = %+v, want sender outbox receipt with sender message id", read)
	}
	date, err := messages.GetOutboxReadDate(ctx, domain.OutboxReadDateRequest{
		OwnerUserID: senderID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: recipientID},
		ID:          sent.SenderMessage.ID,
	})
	if err != nil || date != 1700000200 {
		t.Fatalf("outbox read date = %d err=%v, want read date", date, err)
	}
}

func TestMessageStoreReadMessageContentsClearsUnreadContentOnce(t *testing.T) {
	ctx := context.Background()
	messages := NewMessageStore()
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    1001,
		RecipientUserID: 1002,
		RandomID:        88,
		Message:         "voice placeholder",
		Media:           &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Voice: true},
		Date:            1700000300,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	if !sent.RecipientMessage.MediaUnread {
		t.Fatalf("recipient MediaUnread = false, want true for incoming media")
	}
	got, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: 1002,
		IDs:         []int{sent.RecipientMessage.ID, domain.MaxMessageBoxID},
		Date:        1700000400,
	})
	if err != nil {
		t.Fatalf("ReadMessageContents: %v", err)
	}
	if !reflect.DeepEqual(got.MessageIDs, []int{sent.RecipientMessage.ID}) {
		t.Fatalf("MessageIDs = %v, want unread recipient id", got.MessageIDs)
	}
	if got.Event.Type != domain.UpdateEventReadMessageContents || got.Event.Pts == 0 || got.Event.PtsCount != 1 {
		t.Fatalf("Event = %+v, want read_message_contents pts update", got.Event)
	}
	repeated, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: 1002,
		IDs:         []int{sent.RecipientMessage.ID},
		Date:        1700000500,
	})
	if err != nil {
		t.Fatalf("ReadMessageContents repeat: %v", err)
	}
	if len(repeated.MessageIDs) != 0 || repeated.Event.Pts != 0 {
		t.Fatalf("repeat = %+v, want no affected messages and no pts", repeated)
	}
	if _, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: 1002,
		IDs:         []int{0},
	}); !errors.Is(err, domain.ErrMessageIDInvalid) {
		t.Fatalf("invalid id error = %v, want ErrMessageIDInvalid", err)
	}
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

func TestMessageStoreEditMessageUpdatesBothBoxes(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	senderID := int64(1000000001)
	recipientID := int64(1000000002)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		RandomID:        102,
		Message:         "before",
		Date:            1700000100,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}

	edited, err := messages.EditMessage(ctx, domain.EditMessageRequest{
		OwnerUserID: senderID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: recipientID},
		ID:          sent.SenderMessage.ID,
		Message:     "after",
		EditDate:    1700000200,
	})
	if err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if len(edited.Edited) != 2 || edited.Self().Message.Body != "after" || edited.Self().Event.Type != domain.UpdateEventEditMessage {
		t.Fatalf("edited = %+v, want both owner boxes and self edit event", edited)
	}
	recipientHistory, err := messages.ListByUser(ctx, recipientID, domain.MessageFilter{HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: senderID}, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(recipientHistory.Messages) != 1 || recipientHistory.Messages[0].Body != "after" || recipientHistory.Messages[0].EditDate != 1700000200 {
		t.Fatalf("recipient history = %+v, want edited body/date", recipientHistory.Messages)
	}
}

func TestMessageStoreDeleteHistoryDeletesOrPreservesDialogAndRebuilds(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	senderID := int64(1000000001)
	recipientID := int64(1000000002)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: recipientID}

	for i := 0; i < 2; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    senderID,
			RecipientUserID: recipientID,
			RandomID:        int64(100 + i),
			Message:         "hello",
			Date:            1700000200 + i,
		}); err != nil {
			t.Fatalf("seed send %d: %v", i, err)
		}
	}
	deleted, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: senderID,
		Peer:        peer,
		Date:        1700000300,
	})
	if err != nil {
		t.Fatalf("DeleteHistory: %v", err)
	}
	if self := deleted.Self(); self.Event.Pts != 4 || self.Event.PtsCount != 2 || len(self.MessageIDs) != 2 {
		t.Fatalf("delete result = %+v, want sender delete range pts=4 count=2", self)
	}
	senderHistory, err := messages.ListByUser(ctx, senderID, domain.MessageFilter{HasPeer: true, Peer: peer, Limit: 10})
	if err != nil {
		t.Fatalf("sender history: %v", err)
	}
	recipientHistory, err := messages.ListByUser(ctx, recipientID, domain.MessageFilter{HasPeer: true, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: senderID}, Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	if len(senderHistory.Messages) != 0 || len(recipientHistory.Messages) != 2 {
		t.Fatalf("history sizes sender=%d recipient=%d, want sender cleared only", len(senderHistory.Messages), len(recipientHistory.Messages))
	}
	senderDialogs, err := dialogs.ListByUser(ctx, senderID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("sender dialogs after delete: %v", err)
	}
	if len(senderDialogs.Dialogs) != 0 {
		t.Fatalf("sender dialogs = %+v, want dialog deleted after full history delete", senderDialogs.Dialogs)
	}

	rebuilt, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    senderID,
		RecipientUserID: recipientID,
		RandomID:        200,
		Message:         "rebuilt",
		Date:            1700000400,
	})
	if err != nil {
		t.Fatalf("send after delete: %v", err)
	}
	senderDialogs, err = dialogs.ListByUser(ctx, senderID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("sender dialogs after rebuild: %v", err)
	}
	if len(senderDialogs.Dialogs) != 1 || senderDialogs.Dialogs[0].Peer != peer || senderDialogs.Dialogs[0].TopMessage != rebuilt.SenderMessage.ID {
		t.Fatalf("rebuilt dialogs = %+v, want one dialog with new top message %d", senderDialogs.Dialogs, rebuilt.SenderMessage.ID)
	}

	preservedOwner := int64(1000000003)
	preservedPeerID := int64(1000000004)
	preservedPeer := domain.Peer{Type: domain.PeerTypeUser, ID: preservedPeerID}
	if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    preservedOwner,
		RecipientUserID: preservedPeerID,
		RandomID:        300,
		Message:         "clear but keep dialog",
		Date:            1700000500,
	}); err != nil {
		t.Fatalf("seed preserved send: %v", err)
	}
	if _, err := messages.DeleteHistory(ctx, domain.DeleteHistoryRequest{
		OwnerUserID: preservedOwner,
		Peer:        preservedPeer,
		JustClear:   true,
		Date:        1700000600,
	}); err != nil {
		t.Fatalf("DeleteHistory just_clear: %v", err)
	}
	preservedDialogs, err := dialogs.ListByUser(ctx, preservedOwner, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("preserved dialogs: %v", err)
	}
	if len(preservedDialogs.Dialogs) != 1 || preservedDialogs.Dialogs[0].Peer != preservedPeer || preservedDialogs.Dialogs[0].TopMessage != 0 || len(preservedDialogs.Messages) != 0 {
		t.Fatalf("preserved dialogs = %+v messages=%+v, want empty dialog kept after just_clear", preservedDialogs.Dialogs, preservedDialogs.Messages)
	}
}
