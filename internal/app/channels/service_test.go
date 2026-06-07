package channels

import (
	"context"
	"errors"
	"strings"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestCreateChatCreatesMegagroupWithChannelPts(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)

	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	if !created.Channel.Megagroup || created.Channel.Broadcast {
		t.Fatalf("channel flags = megagroup:%v broadcast:%v, want megagroup only", created.Channel.Megagroup, created.Channel.Broadcast)
	}
	if created.Channel.Pts != 1 || created.Message.ID != 1 || created.Event.PtsCount != 1 {
		t.Fatalf("created pts/message/event = %+v/%+v/%+v, want initial pts=1 message id=1", created.Channel, created.Message, created.Event)
	}
	if created.Message.Action == nil || created.Message.Action.Type != domain.ChannelActionCreate {
		t.Fatalf("create service action = %+v, want channel create", created.Message.Action)
	}

	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  99,
		Message:   "hello",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.Message.ID != 2 || sent.Message.Pts != 2 || sent.Event.Pts != 2 || sent.Event.PtsCount != 1 {
		t.Fatalf("sent = %+v event=%+v, want message id/pts=2", sent.Message, sent.Event)
	}

	duplicate, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  99,
		Message:   "hello again",
		Date:      12,
	})
	if err != nil {
		t.Fatalf("duplicate SendMessage: %v", err)
	}
	if !duplicate.Duplicate || duplicate.Message.ID != sent.Message.ID || duplicate.Message.Body != "hello" {
		t.Fatalf("duplicate = %+v, want original single-copy message", duplicate)
	}

	history, err := service.GetHistory(ctx, 1002, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 10})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(history.Messages) != 2 || history.Messages[0].ID != 2 || history.Messages[1].ID != 1 {
		t.Fatalf("history = %+v, want channel messages newest first", history.Messages)
	}

	diff, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: 1, Limit: 10})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if !diff.Final || diff.Pts != 2 || len(diff.NewMessages) != 1 || diff.NewMessages[0].Body != "hello" {
		t.Fatalf("diff = %+v, want single new channel message at pts=2", diff)
	}
	if _, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: sent.Event.Pts + 1, Limit: 10}); !errors.Is(err, domain.ErrPersistentTimestamp) {
		t.Fatalf("future pts diff err = %v, want persistent timestamp invalid", err)
	}

	read, err := service.ReadHistory(ctx, 1002, domain.ReadChannelHistoryRequest{ChannelID: created.Channel.ID, MaxID: 2})
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if !read.Changed || read.StillUnreadCount != 0 || read.Dialog.ReadInboxMaxID != 2 {
		t.Fatalf("read = %+v, want read watermark at message 2", read)
	}
	if len(read.OutboxUpdates) != 1 || read.OutboxUpdates[0].UserID != 1001 || read.OutboxUpdates[0].MaxID != sent.Message.ID {
		t.Fatalf("read outbox updates = %+v, want owner read_outbox through sent message", read.OutboxUpdates)
	}
	ownerView, err := service.GetChannel(ctx, 1001, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel owner: %v", err)
	}
	if ownerView.Dialog.ReadOutboxMaxID != sent.Message.ID {
		t.Fatalf("owner dialog read_outbox = %d, want %d", ownerView.Dialog.ReadOutboxMaxID, sent.Message.ID)
	}
}

func TestChannelUnreadMentionsArePagedAndCleared(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Mentions",
		MemberUserIDs: []int64{1002, 1003},
		Date:          1700000100,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID:      created.Channel.ID,
		RandomID:       9101,
		Message:        "hello @friend",
		MentionUserIDs: []int64{1002, 1002, 1001},
		Date:           1700000101,
	})
	if err != nil {
		t.Fatalf("SendMessage mention: %v", err)
	}
	view, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel mentioned: %v", err)
	}
	if view.Dialog.UnreadMentions != 1 {
		t.Fatalf("mentioned dialog unread mentions = %d, want 1", view.Dialog.UnreadMentions)
	}
	other, err := service.GetChannel(ctx, 1003, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel other: %v", err)
	}
	if other.Dialog.UnreadMentions != 0 {
		t.Fatalf("unmentioned dialog unread mentions = %d, want 0", other.Dialog.UnreadMentions)
	}
	mentions, err := service.GetUnreadMentions(ctx, 1002, domain.ChannelUnreadMentionsFilter{
		ChannelID: created.Channel.ID,
		OffsetID:  1,
		AddOffset: -10,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("GetUnreadMentions: %v", err)
	}
	if mentions.Count != 1 || len(mentions.Messages) != 1 || mentions.Messages[0].ID != sent.Message.ID {
		t.Fatalf("mentions = count %d messages %+v, want sent message", mentions.Count, mentions.Messages)
	}
	read, err := service.ReadMentions(ctx, 1002, domain.ReadChannelMentionsRequest{ChannelID: created.Channel.ID})
	if err != nil {
		t.Fatalf("ReadMentions: %v", err)
	}
	if read.ChannelPts != sent.Event.Pts || read.Offset != 0 || read.Cleared != 1 {
		t.Fatalf("read mentions = %+v, want pts %d cleared 1 no offset", read, sent.Event.Pts)
	}
	mentions, err = service.GetUnreadMentions(ctx, 1002, domain.ChannelUnreadMentionsFilter{ChannelID: created.Channel.ID, Limit: 10})
	if err != nil {
		t.Fatalf("GetUnreadMentions after read: %v", err)
	}
	if mentions.Count != 0 || len(mentions.Messages) != 0 {
		t.Fatalf("mentions after read = count %d messages %d, want empty", mentions.Count, len(mentions.Messages))
	}
}

func TestServiceRejectsMismatchedUserContextForStateReads(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Context Guard",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  91,
		Message:   "guard",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := service.ReadHistory(ctx, 1001, domain.ReadChannelHistoryRequest{
		UserID:    1002,
		ChannelID: created.Channel.ID,
		MaxID:     sent.Message.ID,
	}); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("ReadHistory mismatched user err = %v, want ErrChannelInvalid", err)
	}
	if _, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		UserID:    1002,
		ChannelID: created.Channel.ID,
		MessageID: sent.Message.ID,
	}); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("GetMessageReadParticipants mismatched user err = %v, want ErrChannelInvalid", err)
	}
	if _, err := service.GetDifference(ctx, 1001, domain.ChannelDifferenceRequest{
		UserID:    1002,
		ChannelID: created.Channel.ID,
		Pts:       0,
	}); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("GetDifference mismatched user err = %v, want ErrChannelInvalid", err)
	}
}

func TestServiceRejectsHugeChannelDialogVector(t *testing.T) {
	service := NewService(memory.NewChannelStore())
	ids := make([]int64, domain.MaxDialogFolderPeers+1)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	if _, err := service.GetDialogs(context.Background(), 1001, ids); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("GetDialogs huge channel vector err = %v, want ErrChannelInvalid", err)
	}
}

func TestChannelHistorySearchQueryIsBounded(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		CreatorUserID: 1001,
		Title:         "Bounded History",
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	_, err = service.GetHistory(ctx, 1001, domain.ChannelHistoryFilter{
		ChannelID: created.Channel.ID,
		Query:     strings.Repeat("x", domain.MaxChannelHistoryQueryLength+1),
		Limit:     10,
	})
	if !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("GetHistory long query err = %v, want channel invalid", err)
	}
}

func TestChannelHistorySupportsOffsetDateOnly(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		CreatorUserID: 1001,
		Title:         "Date Cursor",
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "old",
		Date:      20,
	}); err != nil {
		t.Fatalf("send old: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "new",
		Date:      30,
	}); err != nil {
		t.Fatalf("send new: %v", err)
	}

	history, err := service.GetHistory(ctx, 1001, domain.ChannelHistoryFilter{
		ChannelID:  created.Channel.ID,
		OffsetDate: 30,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(history.Messages) != 2 || history.Messages[0].Body != "old" || history.Messages[1].Action == nil {
		t.Fatalf("history = %+v, want messages older than offset date including service message", history.Messages)
	}
}

func TestChannelDifferenceTooLongReturnsLatestSnapshot(t *testing.T) {
	ctx := context.Background()
	store := memory.NewChannelStore()
	service := NewService(store)
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		CreatorUserID: 1001,
		Title:         "Long Difference",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	var lastPts int
	for i := 0; i < 12; i++ {
		sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
			ChannelID: created.Channel.ID,
			RandomID:  int64(i + 1),
			Message:   "msg",
			Date:      11 + i,
		})
		if err != nil {
			t.Fatalf("SendMessage %d: %v", i, err)
		}
		lastPts = sent.Event.Pts
	}
	diff, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{
		ChannelID: created.Channel.ID,
		Pts:       0,
		Limit:     3,
	})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if !diff.TooLong || !diff.Final || diff.Pts != lastPts {
		t.Fatalf("diff = %+v, want tooLong final snapshot at pts %d", diff, lastPts)
	}
	if len(diff.NewMessages) == 0 || len(diff.NewMessages) > domain.MaxChannelDifferenceTooLongMessages {
		t.Fatalf("tooLong messages = %d, want bounded latest snapshot", len(diff.NewMessages))
	}
}

func TestGetParticipantsCapsDeepOffset(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002, 1003},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	page, err := service.GetParticipants(ctx, 1001, created.Channel.ID, domain.ChannelParticipantsFilter{}, domain.MaxChannelParticipantsOffset+1_000_000, 10)
	if err != nil {
		t.Fatalf("GetParticipants deep offset: %v", err)
	}
	if len(page.Participants) != 0 || page.Count != 3 {
		t.Fatalf("deep offset page = %+v, want bounded empty page with real count", page)
	}
}

func TestDefaultBannedRightsRestrictMemberSendAndInvite(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Permissions",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	updated, err := service.EditDefaultBannedRights(ctx, 1001, domain.EditChannelDefaultBannedRightsRequest{
		ChannelID: created.Channel.ID,
		BannedRights: domain.ChannelBannedRights{
			SendMessages: true,
			InviteUsers:  true,
		},
		Date: 11,
	})
	if err != nil {
		t.Fatalf("EditDefaultBannedRights: %v", err)
	}
	if !updated.DefaultBannedRights.SendMessages || !updated.DefaultBannedRights.InviteUsers {
		t.Fatalf("default banned rights = %+v, want send+invite restricted", updated.DefaultBannedRights)
	}
	if _, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "blocked",
		Date:      12,
	}); !errors.Is(err, domain.ErrChannelWriteForbidden) {
		t.Fatalf("member SendMessage err = %v, want ErrChannelWriteForbidden", err)
	}
	if _, err := service.InviteToChannel(ctx, 1002, created.Channel.ID, []int64{1003}, 12); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member InviteToChannel err = %v, want ErrChannelAdminRequired", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "owner ok",
		Date:      13,
	}); err != nil {
		t.Fatalf("creator SendMessage under default rights: %v", err)
	}
	if _, err := service.EditDefaultBannedRights(ctx, 1001, domain.EditChannelDefaultBannedRightsRequest{
		ChannelID:    created.Channel.ID,
		BannedRights: domain.ChannelBannedRights{},
		Date:         14,
	}); err != nil {
		t.Fatalf("clear default banned rights: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  3,
		Message:   "member ok",
		Date:      15,
	}); err != nil {
		t.Fatalf("member SendMessage after clear: %v", err)
	}
}

func TestSendMessageResolvesChannelReplyTopID(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())

	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Replies",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	root, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "root",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("send root: %v", err)
	}
	reply, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "reply",
		ReplyTo: &domain.MessageReply{
			MessageID:   root.Message.ID,
			QuoteText:   "ro",
			QuoteOffset: 0,
			QuoteEntities: []domain.MessageEntity{{
				Type:   domain.MessageEntityBold,
				Offset: 0,
				Length: 2,
			}},
		},
		Date: 12,
	})
	if err != nil {
		t.Fatalf("send reply: %v", err)
	}
	if reply.Message.ReplyTo == nil {
		t.Fatal("reply metadata is nil")
	}
	channelPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: created.Channel.ID}
	if reply.Message.ReplyTo.MessageID != root.Message.ID || reply.Message.ReplyTo.Peer != channelPeer || reply.Message.ReplyTo.TopMessageID != root.Message.ID {
		t.Fatalf("reply metadata = %+v, want channel peer and root top id %d", reply.Message.ReplyTo, root.Message.ID)
	}
	if reply.Message.ReplyTo.QuoteText != "ro" || len(reply.Message.ReplyTo.QuoteEntities) != 1 {
		t.Fatalf("reply quote = %+v, want preserved quote metadata", reply.Message.ReplyTo)
	}

	nested, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  3,
		Message:   "nested",
		ReplyTo:   &domain.MessageReply{MessageID: reply.Message.ID},
		Date:      13,
	})
	if err != nil {
		t.Fatalf("send nested reply: %v", err)
	}
	if nested.Message.ReplyTo == nil || nested.Message.ReplyTo.TopMessageID != root.Message.ID {
		t.Fatalf("nested reply = %+v, want inherited top id %d", nested.Message.ReplyTo, root.Message.ID)
	}
	_, err = service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  4,
		Message:   "bad reply",
		ReplyTo:   &domain.MessageReply{MessageID: 999},
		Date:      14,
	})
	if !errors.Is(err, domain.ErrReplyMessageIDInvalid) {
		t.Fatalf("bad reply err = %v, want ErrReplyMessageIDInvalid", err)
	}
	_, err = service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  5,
		Message:   "bad quote offset",
		ReplyTo: &domain.MessageReply{
			MessageID:   root.Message.ID,
			QuoteText:   "ro",
			QuoteOffset: domain.MaxMessageReplyQuoteOffset + 1,
		},
		Date: 15,
	})
	if !errors.Is(err, domain.ErrReplyMessageIDInvalid) {
		t.Fatalf("bad quote offset err = %v, want ErrReplyMessageIDInvalid", err)
	}
}

func TestGetMessageReadParticipantsUsesChannelReadWatermark(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())

	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Readers",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  100,
		Message:   "read me",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := service.ReadHistory(ctx, 1002, domain.ReadChannelHistoryRequest{
		ChannelID: created.Channel.ID,
		MaxID:     sent.Message.ID,
		Date:      20,
	}); err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}

	readers, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		ChannelID: created.Channel.ID,
		MessageID: sent.Message.ID,
		Date:      21,
	})
	if err != nil {
		t.Fatalf("GetMessageReadParticipants: %v", err)
	}
	if len(readers.Participants) != 1 || readers.Participants[0].UserID != 1002 || readers.Participants[0].Date != 20 {
		t.Fatalf("readers = %+v, want friend read at date 20", readers.Participants)
	}
}

func TestParticipantsHiddenHidesMemberListAndReadParticipants(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())

	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Hidden Members",
		MemberUserIDs: []int64{1002, 1003},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  100,
		Message:   "read me",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := service.ReadHistory(ctx, 1002, domain.ReadChannelHistoryRequest{
		ChannelID: created.Channel.ID,
		MaxID:     sent.Message.ID,
		Date:      20,
	}); err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	hidden, err := service.SetParticipantsHidden(ctx, 1001, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("SetParticipantsHidden: %v", err)
	}
	if !hidden.ParticipantsHidden {
		t.Fatalf("channel = %+v, want participants hidden", hidden)
	}
	if _, err := service.SetParticipantsHidden(ctx, 1002, created.Channel.ID, false); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member SetParticipantsHidden err = %v, want ErrChannelAdminRequired", err)
	}
	members, err := service.GetParticipants(ctx, 1002, created.Channel.ID, domain.ChannelParticipantsFilter{}, 0, 10)
	if err != nil {
		t.Fatalf("GetParticipants hidden member view: %v", err)
	}
	if len(members.Participants) != 0 || members.Count != hidden.ParticipantsCount {
		t.Fatalf("hidden members page = %+v, want empty page with aggregate count", members)
	}
	admins, err := service.GetParticipants(ctx, 1002, created.Channel.ID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsAdmins}, 0, 10)
	if err != nil {
		t.Fatalf("GetParticipants hidden admins: %v", err)
	}
	if len(admins.Participants) != 1 || admins.Participants[0].UserID != 1001 {
		t.Fatalf("hidden admins page = %+v, want creator visible", admins.Participants)
	}
	readers, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		ChannelID: created.Channel.ID,
		MessageID: sent.Message.ID,
		Date:      21,
	})
	if err != nil {
		t.Fatalf("GetMessageReadParticipants hidden: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("hidden readers = %+v, want none", readers.Participants)
	}
}

func TestBroadcastRejectsMemberPost(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())

	created, err := service.CreateChannel(ctx, 1001, domain.CreateChannelRequest{
		Title:         "News",
		Broadcast:     true,
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if !created.Channel.Broadcast || created.Channel.Megagroup {
		t.Fatalf("channel flags = broadcast:%v megagroup:%v, want broadcast only", created.Channel.Broadcast, created.Channel.Megagroup)
	}

	_, err = service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "member post",
		Date:      11,
	})
	if !errors.Is(err, domain.ErrChannelWriteForbidden) {
		t.Fatalf("member SendMessage error = %v, want ErrChannelWriteForbidden", err)
	}

	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "owner post",
		Date:      12,
	})
	if err != nil {
		t.Fatalf("creator SendMessage: %v", err)
	}
	if !sent.Message.Post {
		t.Fatalf("broadcast message Post=false, want true")
	}
}

func TestChannelEditDeleteAndLocalClearUseChannelPts(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	first, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 1, Message: "one", Date: 11})
	if err != nil {
		t.Fatalf("SendMessage first: %v", err)
	}
	second, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 2, Message: "two", Date: 12})
	if err != nil {
		t.Fatalf("SendMessage second: %v", err)
	}

	edited, err := service.EditMessage(ctx, 1002, domain.EditChannelMessageRequest{
		ChannelID: created.Channel.ID,
		ID:        second.Message.ID,
		Message:   "two edited",
		EditDate:  13,
	})
	if err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if edited.Event.Type != domain.ChannelUpdateEditMessage || edited.Event.Pts != 4 || edited.Event.PtsCount != 1 {
		t.Fatalf("edit event = %+v, want channel edit pts=4 count=1", edited.Event)
	}
	duplicate, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 2, Message: "two retry", Date: 13})
	if err != nil {
		t.Fatalf("duplicate SendMessage after edit: %v", err)
	}
	if !duplicate.Duplicate || duplicate.Event.Type != domain.ChannelUpdateNewMessage || duplicate.Message.Body != "two" || duplicate.Event.Message.Body != "two" {
		t.Fatalf("duplicate after edit = %+v, want original new-message snapshot", duplicate)
	}

	deleted, err := service.DeleteMessages(ctx, 1001, domain.DeleteChannelMessagesRequest{
		ChannelID: created.Channel.ID,
		IDs:       []int{first.Message.ID, second.Message.ID},
		Date:      14,
	})
	if err != nil {
		t.Fatalf("DeleteMessages: %v", err)
	}
	if deleted.Event.Type != domain.ChannelUpdateDeleteMessages || deleted.Event.Pts != 6 || deleted.Event.PtsCount != 2 {
		t.Fatalf("delete event = %+v, want pts advanced by deleted id count", deleted.Event)
	}
	diff, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: 3, Limit: 10})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if len(diff.OtherUpdates) != 2 || diff.OtherUpdates[1].Type != domain.ChannelUpdateDeleteMessages || diff.Pts != 6 {
		t.Fatalf("diff after edit/delete = %+v, want edit then delete through channel pts", diff)
	}

	clear, err := service.DeleteHistory(ctx, 1002, domain.DeleteChannelHistoryRequest{ChannelID: created.Channel.ID, MaxID: 6})
	if err != nil {
		t.Fatalf("DeleteHistory local: %v", err)
	}
	if clear.Event.Pts != 0 {
		t.Fatalf("local clear event = %+v, want no channel pts event", clear.Event)
	}
	history, err := service.GetHistory(ctx, 1002, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 10})
	if err != nil {
		t.Fatalf("GetHistory after local clear: %v", err)
	}
	if len(history.Messages) != 0 {
		t.Fatalf("history after local clear = %+v, want hidden for current user", history.Messages)
	}
}

func TestDeleteParticipantHistoryDeletesOneBoundedSenderPage(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	ownerMsg, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 1, Message: "owner", Date: 11})
	if err != nil {
		t.Fatalf("owner SendMessage: %v", err)
	}
	first, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 2, Message: "member one", Date: 12})
	if err != nil {
		t.Fatalf("member first SendMessage: %v", err)
	}
	second, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 3, Message: "member two", Date: 13})
	if err != nil {
		t.Fatalf("member second SendMessage: %v", err)
	}
	if _, err := service.DeleteParticipantHistory(ctx, 1002, domain.DeleteChannelParticipantHistoryRequest{
		ChannelID:         created.Channel.ID,
		ParticipantUserID: 1001,
		Date:              14,
	}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member DeleteParticipantHistory err = %v, want ErrChannelAdminRequired", err)
	}

	deleted, err := service.DeleteParticipantHistory(ctx, 1001, domain.DeleteChannelParticipantHistoryRequest{
		ChannelID:         created.Channel.ID,
		ParticipantUserID: 1002,
		Date:              15,
	})
	if err != nil {
		t.Fatalf("DeleteParticipantHistory: %v", err)
	}
	if deleted.Event.Type != domain.ChannelUpdateDeleteMessages || deleted.Event.PtsCount != 2 || deleted.Offset != 0 {
		t.Fatalf("deleted = %+v, want one delete update with pts_count=2", deleted)
	}
	wantDeleted := map[int]bool{first.Message.ID: true, second.Message.ID: true}
	for _, id := range deleted.DeletedIDs {
		delete(wantDeleted, id)
	}
	if len(wantDeleted) != 0 {
		t.Fatalf("deleted IDs = %+v, missing member messages %+v", deleted.DeletedIDs, wantDeleted)
	}
	history, err := service.GetHistory(ctx, 1001, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 10})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(history.Messages) != 2 || history.Messages[0].ID != ownerMsg.Message.ID {
		t.Fatalf("history after participant delete = %+v, want owner message and create service only", history.Messages)
	}
}

func TestChannelAdminTitlePinAndInvite(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	ptsBeforeAdmin := created.Channel.Pts

	admin, err := service.EditAdmin(ctx, 1001, domain.EditChannelAdminRequest{
		ChannelID: created.Channel.ID,
		MemberID:  1002,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo:  true,
			InviteUsers: true,
			PinMessages: true,
		},
		Rank: "ops",
		Date: 11,
	})
	if err != nil {
		t.Fatalf("EditAdmin: %v", err)
	}
	if admin.Participant.Role != domain.ChannelRoleAdmin || !admin.Participant.AdminRights.PinMessages || admin.Channel.AdminsCount != 2 {
		t.Fatalf("admin result = %+v, want promoted admin with counts", admin)
	}
	if admin.Channel.Pts != ptsBeforeAdmin {
		t.Fatalf("admin channel pts = %d, want unchanged %d", admin.Channel.Pts, ptsBeforeAdmin)
	}
	if admin.Event.Type != domain.ChannelUpdateParticipant || admin.Event.Pts != 0 || admin.Event.PtsCount != 0 || admin.Event.Participant.UserID != 1002 || admin.Event.Previous.UserID != 1002 {
		t.Fatalf("admin participant event = %+v, want transient participant transition", admin.Event)
	}
	diffAfterAdmin, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: ptsBeforeAdmin, Limit: 10})
	if err != nil {
		t.Fatalf("GetDifference after admin: %v", err)
	}
	if len(diffAfterAdmin.OtherUpdates) != 0 || diffAfterAdmin.Pts != ptsBeforeAdmin {
		t.Fatalf("diff after admin = %+v, want no durable participant update", diffAfterAdmin)
	}
	admins, err := service.GetParticipants(ctx, 1001, created.Channel.ID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsAdmins}, 0, 10)
	if err != nil {
		t.Fatalf("GetParticipants admins: %v", err)
	}
	if len(admins.Participants) != 2 || admins.Participants[1].UserID != 1002 {
		t.Fatalf("admins participants = %+v, want creator and promoted admin", admins.Participants)
	}

	renamed, err := service.EditTitle(ctx, 1002, domain.EditChannelTitleRequest{ChannelID: created.Channel.ID, Title: "Team 2", Date: 12})
	if err != nil {
		t.Fatalf("EditTitle by promoted admin: %v", err)
	}
	if renamed.Channel.Title != "Team 2" || renamed.Event.Type != domain.ChannelUpdateNewMessage || renamed.Message.Action.Type != domain.ChannelActionEditTitle {
		t.Fatalf("renamed = %+v message=%+v, want edit-title service message", renamed.Channel, renamed.Message)
	}

	sent, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 42, Message: "pin me", Date: 13})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	pinned, err := service.UpdatePinnedMessage(ctx, 1002, domain.UpdateChannelPinnedMessageRequest{
		ChannelID: created.Channel.ID,
		MessageID: sent.Message.ID,
		Pinned:    true,
		Date:      14,
	})
	if err != nil {
		t.Fatalf("UpdatePinnedMessage: %v", err)
	}
	if pinned.Channel.PinnedMessageID != sent.Message.ID || pinned.Event.Type != domain.ChannelUpdatePinnedMessages || !pinned.Event.Pinned {
		t.Fatalf("pinned = %+v, want pinned channel message event", pinned)
	}

	invited, err := service.InviteToChannel(ctx, 1002, created.Channel.ID, []int64{1004}, 15)
	if err != nil {
		t.Fatalf("InviteToChannel: %v", err)
	}
	if len(invited.Members) != 1 || invited.Members[0].UserID != 1004 {
		t.Fatalf("invited = %+v, want invited user", invited.Members)
	}

	invite, err := service.ExportInvite(ctx, 1002, domain.ExportChannelInviteRequest{ChannelID: created.Channel.ID, Title: "join", Date: 15})
	if err != nil {
		t.Fatalf("ExportInvite: %v", err)
	}
	checked, err := service.CheckInvite(ctx, 1003, invite.Invite.Hash, 16)
	if err != nil {
		t.Fatalf("CheckInvite: %v", err)
	}
	if checked.Already || checked.Channel.ID != created.Channel.ID {
		t.Fatalf("checked invite = %+v, want preview for non-member", checked)
	}
	joined, err := service.ImportInvite(ctx, 1003, domain.ImportChannelInviteRequest{Hash: invite.Invite.Hash, Date: 17})
	if err != nil {
		t.Fatalf("ImportInvite: %v", err)
	}
	if len(joined.Members) != 1 || joined.Members[0].UserID != 1003 || joined.Event.Pts == 0 {
		t.Fatalf("joined = %+v, want imported member with megagroup join event", joined)
	}
	forum, err := service.SetForum(ctx, 1001, created.Channel.ID, true, true)
	if err != nil {
		t.Fatalf("SetForum: %v", err)
	}
	if !forum.Forum || !forum.ForumTabs {
		t.Fatalf("forum = %+v, want enabled with tabs", forum)
	}
	antiSpam, err := service.SetAntiSpam(ctx, 1001, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("SetAntiSpam: %v", err)
	}
	if !antiSpam.AntiSpam {
		t.Fatalf("antiSpam = %+v, want enabled", antiSpam)
	}
	logs, err := service.ListAdminLog(ctx, 1001, domain.ChannelAdminLogRequest{ChannelID: created.Channel.ID, Limit: 20})
	if err != nil {
		t.Fatalf("ListAdminLog: %v", err)
	}
	seen := map[domain.ChannelAdminLogEventType]bool{}
	for _, event := range logs.Events {
		seen[event.Type] = true
	}
	for _, typ := range []domain.ChannelAdminLogEventType{
		domain.ChannelAdminLogParticipantPromote,
		domain.ChannelAdminLogChangeTitle,
		domain.ChannelAdminLogUpdatePinned,
		domain.ChannelAdminLogParticipantInvite,
		domain.ChannelAdminLogParticipantJoin,
		domain.ChannelAdminLogToggleForum,
		domain.ChannelAdminLogToggleAntiSpam,
	} {
		if !seen[typ] {
			t.Fatalf("admin logs missing %s in %+v", typ, logs.Events)
		}
	}
	pinnedOnly, err := service.ListAdminLog(ctx, 1001, domain.ChannelAdminLogRequest{
		ChannelID: created.Channel.ID,
		Limit:     10,
		Filter:    domain.ChannelAdminLogFilter{Pinned: true},
	})
	if err != nil {
		t.Fatalf("ListAdminLog pinned: %v", err)
	}
	if len(pinnedOnly.Events) != 1 || pinnedOnly.Events[0].Type != domain.ChannelAdminLogUpdatePinned || pinnedOnly.Events[0].Message == nil {
		t.Fatalf("pinned admin logs = %+v, want one update_pinned with message", pinnedOnly.Events)
	}
	if _, err := service.ListAdminLog(ctx, 1003, domain.ChannelAdminLogRequest{ChannelID: created.Channel.ID, Limit: 10}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("non-admin ListAdminLog err = %v, want ErrChannelAdminRequired", err)
	}
}

func TestChannelAboutRequiresChangeInfo(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}

	if _, err := service.EditAbout(ctx, 1002, domain.EditChannelAboutRequest{
		ChannelID: created.Channel.ID,
		About:     "member cannot edit",
		Date:      11,
	}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("EditAbout by member err = %v, want ErrChannelAdminRequired", err)
	}

	updated, err := service.EditAbout(ctx, 1001, domain.EditChannelAboutRequest{
		ChannelID: created.Channel.ID,
		About:     "owner about",
		Date:      12,
	})
	if err != nil {
		t.Fatalf("EditAbout by owner: %v", err)
	}
	if updated.About != "owner about" {
		t.Fatalf("updated about = %q, want owner about", updated.About)
	}
	view, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel by member: %v", err)
	}
	if view.Channel.About != "owner about" {
		t.Fatalf("member view about = %q, want owner about", view.Channel.About)
	}

	if _, err := service.EditAdmin(ctx, 1001, domain.EditChannelAdminRequest{
		ChannelID: created.Channel.ID,
		MemberID:  1002,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo: true,
		},
		Date: 13,
	}); err != nil {
		t.Fatalf("EditAdmin: %v", err)
	}
	updated, err = service.EditAbout(ctx, 1002, domain.EditChannelAboutRequest{
		ChannelID: created.Channel.ID,
		About:     "admin about",
		Date:      14,
	})
	if err != nil {
		t.Fatalf("EditAbout by change_info admin: %v", err)
	}
	if updated.About != "admin about" {
		t.Fatalf("updated about = %q, want admin about", updated.About)
	}
}

func TestChannelBanAndDeletePermissions(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	ptsBeforeBan := created.Channel.Pts
	if _, err := service.DeleteChannel(ctx, 1002, domain.DeleteChannelRequest{ChannelID: created.Channel.ID, Date: 11}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member DeleteChannel err = %v, want ErrChannelAdminRequired", err)
	}
	banned, err := service.EditBanned(ctx, 1001, domain.EditChannelBannedRequest{
		ChannelID:   created.Channel.ID,
		Participant: domain.Peer{Type: domain.PeerTypeUser, ID: 1002},
		BannedRights: domain.ChannelBannedRights{
			ViewMessages: true,
			UntilDate:    100,
		},
		Date: 12,
	})
	if err != nil {
		t.Fatalf("EditBanned: %v", err)
	}
	if banned.Participant.Status != domain.ChannelMemberKicked || banned.Channel.ParticipantsCount != 1 || banned.Channel.KickedCount != 1 {
		t.Fatalf("banned = %+v, want kicked participant and counts", banned)
	}
	if banned.Channel.Pts != ptsBeforeBan {
		t.Fatalf("banned channel pts = %d, want unchanged %d", banned.Channel.Pts, ptsBeforeBan)
	}
	if banned.Event.Type != domain.ChannelUpdateParticipant || banned.Event.Participant.Status != domain.ChannelMemberKicked || banned.Event.Pts != 0 || banned.Event.PtsCount != 0 {
		t.Fatalf("ban participant event = %+v, want transient kicked transition", banned.Event)
	}
	kicked, err := service.GetParticipants(ctx, 1001, created.Channel.ID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsKicked}, 0, 10)
	if err != nil {
		t.Fatalf("GetParticipants kicked: %v", err)
	}
	if len(kicked.Participants) != 1 || kicked.Participants[0].UserID != 1002 || kicked.Participants[0].InviterUserID != 1001 {
		t.Fatalf("kicked participants = %+v, want kicked user with actor as inviter/kicked_by", kicked.Participants)
	}
	hidden, err := service.GetParticipants(ctx, 1002, created.Channel.ID, domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsKicked}, 0, 10)
	if !errors.Is(err, domain.ErrChannelUserBanned) && (err != nil || len(hidden.Participants) != 0) {
		t.Fatalf("banned viewer kicked participants = %+v err=%v, want no access", hidden.Participants, err)
	}
	if _, err := service.GetHistory(ctx, 1002, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 10}); !errors.Is(err, domain.ErrChannelUserBanned) {
		t.Fatalf("banned GetHistory err = %v, want ErrChannelUserBanned", err)
	}
	if _, err := service.JoinChannel(ctx, 1002, created.Channel.ID, 13); !errors.Is(err, domain.ErrChannelUserBanned) {
		t.Fatalf("kicked JoinChannel err = %v, want ErrChannelUserBanned", err)
	}
	deleted, err := service.DeleteChannel(ctx, 1001, domain.DeleteChannelRequest{ChannelID: created.Channel.ID, Date: 13})
	if err != nil {
		t.Fatalf("creator DeleteChannel: %v", err)
	}
	if !deleted.Channel.Deleted {
		t.Fatalf("deleted = %+v, want deleted channel", deleted)
	}
}

func TestChannelInviteCannotBypassKickedMemberWithoutBanRight(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Invite Kicked",
		MemberUserIDs: []int64{1002, 1003},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	if _, err := service.EditBanned(ctx, 1001, domain.EditChannelBannedRequest{
		ChannelID:   created.Channel.ID,
		Participant: domain.Peer{Type: domain.PeerTypeUser, ID: 1002},
		BannedRights: domain.ChannelBannedRights{
			ViewMessages: true,
			UntilDate:    100,
		},
		Date: 11,
	}); err != nil {
		t.Fatalf("EditBanned: %v", err)
	}
	if _, err := service.InviteToChannel(ctx, 1003, created.Channel.ID, []int64{1002}, 12); !errors.Is(err, domain.ErrUserKicked) {
		t.Fatalf("member InviteToChannel kicked err = %v, want ErrUserKicked", err)
	}
	restored, err := service.InviteToChannel(ctx, 1001, created.Channel.ID, []int64{1002}, 13)
	if err != nil {
		t.Fatalf("creator InviteToChannel kicked: %v", err)
	}
	if len(restored.Members) != 1 || restored.Members[0].Status != domain.ChannelMemberActive || restored.Members[0].BannedRights != (domain.ChannelBannedRights{}) {
		t.Fatalf("restored members = %+v, want active unbanned member", restored.Members)
	}
	if restored.Channel.ParticipantsCount != 3 || restored.Channel.KickedCount != 0 {
		t.Fatalf("restored counts = participants:%d kicked:%d, want 3/0", restored.Channel.ParticipantsCount, restored.Channel.KickedCount)
	}
	if _, err := service.InviteToChannel(ctx, 1001, created.Channel.ID, []int64{1002}, 14); !errors.Is(err, domain.ErrUserAlreadyParticipant) {
		t.Fatalf("duplicate InviteToChannel err = %v, want ErrUserAlreadyParticipant", err)
	}
}

func TestChannelLeaveAndRejoinRestoresParticipantCountAndNotifiesLeaver(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Leave Rejoin",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	left, err := service.LeaveChannel(ctx, 1002, created.Channel.ID, 11)
	if err != nil {
		t.Fatalf("LeaveChannel: %v", err)
	}
	if left.Members[0].Status != domain.ChannelMemberLeft || left.Channel.ParticipantsCount != 1 {
		t.Fatalf("left result = %+v, want left member and participants=1", left)
	}
	hasLeaverRecipient := false
	for _, id := range left.Recipients {
		if id == 1002 {
			hasLeaverRecipient = true
			break
		}
	}
	if !hasLeaverRecipient {
		t.Fatalf("leave recipients = %+v, want leaver included for other sessions", left.Recipients)
	}
	rejoined, err := service.JoinChannel(ctx, 1002, created.Channel.ID, 12)
	if err != nil {
		t.Fatalf("JoinChannel after leave: %v", err)
	}
	if rejoined.Members[0].Status != domain.ChannelMemberActive || rejoined.Channel.ParticipantsCount != 2 {
		t.Fatalf("rejoined result = %+v, want active member and participants=2", rejoined)
	}
	if _, err := service.JoinChannel(ctx, 1002, created.Channel.ID, 13); !errors.Is(err, domain.ErrUserAlreadyParticipant) {
		t.Fatalf("duplicate JoinChannel err = %v, want ErrUserAlreadyParticipant", err)
	}
}

func TestChannelUsernameAndSignatures(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	if ok, err := service.CheckUsername(ctx, 1001, created.Channel.ID, "team_public"); err != nil || !ok {
		t.Fatalf("CheckUsername free = ok %v err %v, want true", ok, err)
	}
	public, err := service.UpdateUsername(ctx, 1001, domain.UpdateChannelUsernameRequest{
		ChannelID: created.Channel.ID,
		Username:  "@team_public",
	})
	if err != nil {
		t.Fatalf("UpdateUsername: %v", err)
	}
	if public.Username != "team_public" {
		t.Fatalf("public username = %q, want team_public", public.Username)
	}
	if _, err := service.UpdateUsername(ctx, 1001, domain.UpdateChannelUsernameRequest{ChannelID: created.Channel.ID, Username: "TEAM_PUBLIC"}); !errors.Is(err, domain.ErrChannelNotModified) {
		t.Fatalf("UpdateUsername same username err = %v, want ErrChannelNotModified", err)
	}
	if _, err := service.UpdateUsername(ctx, 1002, domain.UpdateChannelUsernameRequest{ChannelID: created.Channel.ID, Username: "friend_try"}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("non-owner UpdateUsername err = %v, want ErrChannelAdminRequired", err)
	}

	other, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{Title: "Other", Date: 11})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat other: %v", err)
	}
	if ok, err := service.CheckUsername(ctx, 1001, other.Channel.ID, "TEAM_PUBLIC"); err != nil || ok {
		t.Fatalf("CheckUsername occupied = ok %v err %v, want false/nil", ok, err)
	}
	if _, err := service.UpdateUsername(ctx, 1001, domain.UpdateChannelUsernameRequest{ChannelID: other.Channel.ID, Username: "team_public"}); !errors.Is(err, domain.ErrUsernameOccupied) {
		t.Fatalf("UpdateUsername occupied err = %v, want ErrUsernameOccupied", err)
	}
	admined, err := service.ListAdminedPublicChannels(ctx, 1001)
	if err != nil {
		t.Fatalf("ListAdminedPublicChannels: %v", err)
	}
	if len(admined) != 1 || admined[0].ID != created.Channel.ID {
		t.Fatalf("admined public = %+v, want first channel only", admined)
	}

	if _, err := service.EditAdmin(ctx, 1001, domain.EditChannelAdminRequest{
		ChannelID: created.Channel.ID,
		MemberID:  1002,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo: true,
		},
		Date: 12,
	}); err != nil {
		t.Fatalf("EditAdmin: %v", err)
	}
	signed, err := service.SetSignatures(ctx, 1002, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("SetSignatures by change-info admin: %v", err)
	}
	if !signed.Signatures {
		t.Fatalf("signed channel = %+v, want signatures enabled", signed)
	}
}

func TestPublicChannelSearchAndResolveUsername(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "CU Public Lab",
		MemberUserIDs: []int64{1002},
		Date:          20,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	public, err := service.UpdateUsername(ctx, 1001, domain.UpdateChannelUsernameRequest{
		ChannelID: created.Channel.ID,
		Username:  "cu_public_lab",
	})
	if err != nil {
		t.Fatalf("UpdateUsername: %v", err)
	}
	if _, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "CU Private Lab",
		Date:  21,
	}); err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat private: %v", err)
	}

	joined, err := service.SearchPublicChannels(ctx, 1002, "CU Public", 10)
	if err != nil {
		t.Fatalf("SearchPublicChannels joined: %v", err)
	}
	if len(joined.MyResults) != 1 || joined.MyResults[0].ID != public.ID || len(joined.Results) != 0 {
		t.Fatalf("joined public search = %+v, want my public channel only", joined)
	}
	global, err := service.SearchPublicChannels(ctx, 1003, "public", 10)
	if err != nil {
		t.Fatalf("SearchPublicChannels global: %v", err)
	}
	if len(global.Results) != 1 || global.Results[0].ID != public.ID || len(global.MyResults) != 0 {
		t.Fatalf("global public search = %+v, want public channel result", global)
	}
	resolved, found, err := service.ResolvePublicUsername(ctx, 1003, "@CU_PUBLIC_LAB")
	if err != nil || !found || resolved.ID != public.ID {
		t.Fatalf("ResolvePublicUsername = %+v found %v err %v, want public channel", resolved, found, err)
	}
}

func TestPublicChannelPreviewAllowsNonMemberHistory(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	const (
		ownerID  = 1001
		viewerID = 1002
	)
	created, err := service.CreateChannel(ctx, ownerID, domain.CreateChannelRequest{
		Title:     "Public Preview",
		Broadcast: true,
		Date:      10,
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	public, err := service.UpdateUsername(ctx, ownerID, domain.UpdateChannelUsernameRequest{
		UserID:    ownerID,
		ChannelID: created.Channel.ID,
		Username:  "public_preview",
	})
	if err != nil {
		t.Fatalf("UpdateUsername: %v", err)
	}
	sent, err := service.SendMessage(ctx, ownerID, domain.SendChannelMessageRequest{
		ChannelID: public.ID,
		RandomID:  101,
		Message:   "public preview post",
		Date:      20,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	view, err := service.GetChannel(ctx, viewerID, public.ID)
	if err != nil {
		t.Fatalf("non-member GetChannel public preview: %v", err)
	}
	if view.Self.Status != domain.ChannelMemberLeft || view.Self.UserID != viewerID {
		t.Fatalf("preview self = %+v, want synthetic left member for viewer", view.Self)
	}
	if view.Dialog.UnreadCount != 0 || view.Dialog.ReadInboxMaxID < public.TopMessageID {
		t.Fatalf("preview dialog = %+v, want no unread count", view.Dialog)
	}
	history, err := service.GetHistory(ctx, viewerID, domain.ChannelHistoryFilter{ChannelID: public.ID, Limit: 10})
	if err != nil {
		t.Fatalf("non-member GetHistory public preview: %v", err)
	}
	if history.Self.Status != domain.ChannelMemberLeft || history.Self.UserID != viewerID {
		t.Fatalf("history self = %+v, want synthetic left member for viewer", history.Self)
	}
	foundPost := false
	for _, msg := range history.Messages {
		if msg.Body == "public preview post" {
			foundPost = true
		}
	}
	if !foundPost {
		t.Fatalf("history messages = %+v, want public preview post", history.Messages)
	}
	diff, err := service.GetDifference(ctx, viewerID, domain.ChannelDifferenceRequest{
		ChannelID: public.ID,
		Pts:       created.Event.Pts,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("non-member GetDifference public preview: %v", err)
	}
	if !diff.Final || diff.Pts != sent.Event.Pts || len(diff.NewMessages) != 1 || diff.NewMessages[0].Body != "public preview post" {
		t.Fatalf("preview diff = %+v, want public preview post at current pts", diff)
	}
	if diff.Dialog.UnreadCount != 0 || diff.Dialog.ReadInboxMaxID < sent.Message.ID {
		t.Fatalf("preview diff dialog = %+v, want read-only public preview dialog", diff.Dialog)
	}

	private, err := service.CreateChannel(ctx, ownerID, domain.CreateChannelRequest{
		Title:     "Private Preview",
		Broadcast: true,
		Date:      30,
	})
	if err != nil {
		t.Fatalf("CreateChannel private: %v", err)
	}
	if _, err := service.GetChannel(ctx, viewerID, private.Channel.ID); !errors.Is(err, domain.ErrChannelPrivate) {
		t.Fatalf("non-member private GetChannel err = %v, want ErrChannelPrivate", err)
	}
	if _, err := service.EditBanned(ctx, ownerID, domain.EditChannelBannedRequest{
		UserID:       ownerID,
		ChannelID:    public.ID,
		Participant:  domain.Peer{Type: domain.PeerTypeUser, ID: viewerID},
		BannedRights: domain.ChannelBannedRights{ViewMessages: true},
		Date:         40,
	}); err != nil {
		t.Fatalf("EditBanned public viewer: %v", err)
	}
	if _, err := service.GetHistory(ctx, viewerID, domain.ChannelHistoryFilter{ChannelID: public.ID, Limit: 10}); !errors.Is(err, domain.ErrChannelUserBanned) {
		t.Fatalf("banned public preview GetHistory err = %v, want ErrChannelUserBanned", err)
	}
	if _, err := service.GetDifference(ctx, viewerID, domain.ChannelDifferenceRequest{ChannelID: public.ID, Pts: created.Event.Pts, Limit: 10}); !errors.Is(err, domain.ErrChannelUserBanned) {
		t.Fatalf("banned public preview GetDifference err = %v, want ErrChannelUserBanned", err)
	}
}

func TestChannelDifferenceStartsAtMemberAvailableMinPts(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Visible PTS",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	ptsFloor := created.Channel.Pts
	promoted, err := service.EditAdmin(ctx, 1001, domain.EditChannelAdminRequest{
		ChannelID: created.Channel.ID,
		MemberID:  1002,
		AdminRights: domain.ChannelAdminRights{
			InviteUsers: true,
		},
		Date: 11,
	})
	if err != nil {
		t.Fatalf("EditAdmin: %v", err)
	}
	if promoted.Event.Pts != 0 || promoted.Channel.Pts != ptsFloor {
		t.Fatalf("promoted = %+v, want transient admin event and unchanged pts %d", promoted, ptsFloor)
	}
	joined, err := service.JoinChannel(ctx, 1003, created.Channel.ID, 12)
	if err != nil {
		t.Fatalf("JoinChannel: %v", err)
	}
	if joined.Members[0].AvailableMinPts != ptsFloor {
		t.Fatalf("joined available_min_pts = %d, want pre-join channel pts %d", joined.Members[0].AvailableMinPts, ptsFloor)
	}
	diff, err := service.GetDifference(ctx, 1003, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: 0, Limit: 100})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	if diff.Pts != joined.Channel.Pts {
		t.Fatalf("diff pts = %d, want current channel pts %d", diff.Pts, joined.Channel.Pts)
	}
	for _, msg := range diff.NewMessages {
		if msg.Pts <= ptsFloor {
			t.Fatalf("diff leaks pre-join message %+v at or before available_min_pts %d", msg, ptsFloor)
		}
	}
	for _, event := range diff.OtherUpdates {
		if event.Pts <= ptsFloor {
			t.Fatalf("diff leaks pre-join event %+v at or before available_min_pts %d", event, ptsFloor)
		}
	}
}

func TestChannelPreHistoryAndSlowMode(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Settings Team",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	if _, err := service.SetPreHistoryHidden(ctx, 1002, created.Channel.ID, true); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member SetPreHistoryHidden err = %v, want ErrChannelAdminRequired", err)
	}
	hidden, err := service.SetPreHistoryHidden(ctx, 1001, created.Channel.ID, true)
	if err != nil {
		t.Fatalf("SetPreHistoryHidden: %v", err)
	}
	if !hidden.PreHistoryHidden {
		t.Fatalf("hidden channel = %+v, want prehistory hidden", hidden)
	}
	hiddenMsg, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 1, Message: "before new member", Date: 90})
	if err != nil {
		t.Fatalf("owner send before new member: %v", err)
	}
	if _, err := service.JoinChannel(ctx, 1003, created.Channel.ID, 95); err != nil {
		t.Fatalf("new member JoinChannel: %v", err)
	}
	visibleMsg, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 100, Message: "after new member", Date: 96})
	if err != nil {
		t.Fatalf("owner send after new member: %v", err)
	}
	mixedDelete, err := service.DeleteMessages(ctx, 1001, domain.DeleteChannelMessagesRequest{
		ChannelID: created.Channel.ID,
		IDs:       []int{hiddenMsg.Message.ID, visibleMsg.Message.ID},
		Date:      97,
	})
	if err != nil {
		t.Fatalf("mixed DeleteMessages: %v", err)
	}
	if mixedDelete.Event.PtsCount != 2 {
		t.Fatalf("mixed delete pts_count = %d, want original deleted id count 2", mixedDelete.Event.PtsCount)
	}
	history, err := service.GetHistory(ctx, 1003, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 20})
	if err != nil {
		t.Fatalf("new member GetHistory: %v", err)
	}
	for _, msg := range history.Messages {
		if msg.Body == "before new member" {
			t.Fatalf("new member history includes hidden prehistory message: %+v", history.Messages)
		}
	}
	view, err := service.GetChannel(ctx, 1003, created.Channel.ID)
	if err != nil {
		t.Fatalf("new member GetChannel: %v", err)
	}
	diff, err := service.GetDifference(ctx, 1003, domain.ChannelDifferenceRequest{ChannelID: created.Channel.ID, Pts: 0, Limit: 100})
	if err != nil {
		t.Fatalf("new member GetDifference: %v", err)
	}
	if diff.TooLong {
		t.Fatalf("new member diff unexpectedly too long: %+v", diff)
	}
	if diff.Pts != view.Channel.Pts {
		t.Fatalf("new member diff pts = %d, want current channel pts %d", diff.Pts, view.Channel.Pts)
	}
	for _, msg := range diff.NewMessages {
		if msg.ID <= view.Self.AvailableMinID {
			t.Fatalf("new member diff includes hidden prehistory message id %d <= available_min_id %d", msg.ID, view.Self.AvailableMinID)
		}
	}
	for _, event := range diff.OtherUpdates {
		for _, id := range event.MessageIDs {
			if id <= view.Self.AvailableMinID {
				t.Fatalf("new member diff includes hidden prehistory message id %d in event %+v", id, event)
			}
		}
	}
	foundPartialDelete := false
	for _, event := range diff.OtherUpdates {
		if event.Type != domain.ChannelUpdateDeleteMessages || event.Pts != mixedDelete.Event.Pts {
			continue
		}
		foundPartialDelete = true
		if event.PtsCount != mixedDelete.Event.PtsCount || len(event.MessageIDs) != 1 || event.MessageIDs[0] != visibleMsg.Message.ID {
			t.Fatalf("visible mixed delete event = %+v, want pts_count=%d and only visible id %d", event, mixedDelete.Event.PtsCount, visibleMsg.Message.ID)
		}
	}
	if !foundPartialDelete {
		t.Fatalf("new member diff missing partial mixed delete event at pts %d: %+v", mixedDelete.Event.Pts, diff.OtherUpdates)
	}
	if _, err := service.SetSlowMode(ctx, 1002, created.Channel.ID, 30); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("member SetSlowMode err = %v, want ErrChannelAdminRequired", err)
	}
	slow, err := service.SetSlowMode(ctx, 1001, created.Channel.ID, 30)
	if err != nil {
		t.Fatalf("SetSlowMode: %v", err)
	}
	if slow.SlowmodeSeconds != 30 {
		t.Fatalf("slow mode = %+v, want 30 seconds", slow)
	}
	if _, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 2, Message: "first", Date: 100}); err != nil {
		t.Fatalf("first member send: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 3, Message: "too soon", Date: 110}); err == nil {
		t.Fatalf("second member send err = nil, want slow mode wait")
	} else if seconds, ok := domain.SlowModeWaitSeconds(err); !ok || seconds != 20 {
		t.Fatalf("second member send err = %v, want slow mode wait 20", err)
	}
	if _, err := service.SendMessage(ctx, 1002, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 4, Message: "after wait", Date: 130}); err != nil {
		t.Fatalf("third member send after slow mode: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 5, Message: "owner one", Date: 131}); err != nil {
		t.Fatalf("owner send with slow mode: %v", err)
	}
	if _, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{ChannelID: created.Channel.ID, RandomID: 6, Message: "owner two", Date: 132}); err != nil {
		t.Fatalf("owner second send with slow mode: %v", err)
	}
}

func TestImportInviteRespectsPreHistoryHidden(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateChannel(ctx, 1001, domain.CreateChannelRequest{
		Title:     "Private Invite",
		Megagroup: true,
		Date:      10,
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if _, err := service.SetPreHistoryHidden(ctx, 1001, created.Channel.ID, true); err != nil {
		t.Fatalf("SetPreHistoryHidden: %v", err)
	}
	hiddenMsg, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  501,
		Message:   "hidden before invite link",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage hidden: %v", err)
	}
	invite, err := service.ExportInvite(ctx, 1001, domain.ExportChannelInviteRequest{
		ChannelID: created.Channel.ID,
		Title:     "join",
		Date:      12,
	})
	if err != nil {
		t.Fatalf("ExportInvite: %v", err)
	}
	joined, err := service.ImportInvite(ctx, 1002, domain.ImportChannelInviteRequest{
		Hash: invite.Invite.Hash,
		Date: 13,
	})
	if err != nil {
		t.Fatalf("ImportInvite: %v", err)
	}
	if joined.Members[0].AvailableMinID != hiddenMsg.Message.ID || joined.Members[0].ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("imported member watermarks = %+v, want hidden top %d and read at join service %d", joined.Members[0], hiddenMsg.Message.ID, joined.Message.ID)
	}
	diff, err := service.GetDifference(ctx, 1002, domain.ChannelDifferenceRequest{
		ChannelID: created.Channel.ID,
		Pts:       0,
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("GetDifference: %v", err)
	}
	for _, msg := range diff.NewMessages {
		if msg.ID <= hiddenMsg.Message.ID {
			t.Fatalf("diff includes hidden message id %d <= available_min_id %d", msg.ID, hiddenMsg.Message.ID)
		}
	}
}

func TestImportInviteInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Import Watermark",
		Date:  10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	first, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "before import",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("send existing message: %v", err)
	}
	invite, err := service.ExportInvite(ctx, 1001, domain.ExportChannelInviteRequest{
		ChannelID: created.Channel.ID,
		Title:     "join",
		Date:      12,
	})
	if err != nil {
		t.Fatalf("ExportInvite: %v", err)
	}
	joined, err := service.ImportInvite(ctx, 1002, domain.ImportChannelInviteRequest{
		Hash: invite.Invite.Hash,
		Date: 13,
	})
	if err != nil {
		t.Fatalf("ImportInvite: %v", err)
	}
	if joined.Members[0].ReadInboxMaxID != joined.Message.ID || joined.Members[0].ReadOutboxMaxID != joined.Message.ID {
		t.Fatalf("joined member read watermarks = %+v message=%+v, want self join service read/outbox", joined.Members[0], joined.Message)
	}
	view, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel imported: %v", err)
	}
	if view.Dialog.UnreadCount != 0 || view.Self.ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("imported dialog/self = %+v / %+v, want no unread and read at join service", view.Dialog, view.Self)
	}
	readers, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		ChannelID: created.Channel.ID,
		MessageID: first.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      14,
	})
	if err != nil {
		t.Fatalf("GetMessageReadParticipants existing message: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("existing message readers after import = %+v, want none from initial watermark", readers.Participants)
	}
	future, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "after import",
		Date:      14,
	})
	if err != nil {
		t.Fatalf("send future message: %v", err)
	}
	after, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel after future: %v", err)
	}
	if after.Dialog.TopMessageID != future.Message.ID || after.Dialog.UnreadCount != 1 {
		t.Fatalf("imported dialog after future = %+v, want top %d unread 1", after.Dialog, future.Message.ID)
	}
}

func TestImportInviteRequestNeededAndUsageLimitErrors(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Invite Errors",
		Date:  10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	requested, err := service.ExportInvite(ctx, 1001, domain.ExportChannelInviteRequest{
		ChannelID:     created.Channel.ID,
		Title:         "approval",
		RequestNeeded: true,
		Date:          11,
	})
	if err != nil {
		t.Fatalf("ExportInvite request needed: %v", err)
	}
	if _, err := service.ImportInvite(ctx, 1002, domain.ImportChannelInviteRequest{Hash: requested.Invite.Hash, Date: 12}); !errors.Is(err, domain.ErrInviteRequestSent) {
		t.Fatalf("ImportInvite request-needed err = %v, want ErrInviteRequestSent", err)
	}
	limited, err := service.ExportInvite(ctx, 1001, domain.ExportChannelInviteRequest{
		ChannelID:  created.Channel.ID,
		Title:      "one",
		UsageLimit: 1,
		Date:       13,
	})
	if err != nil {
		t.Fatalf("ExportInvite limited: %v", err)
	}
	if _, err := service.ImportInvite(ctx, 1002, domain.ImportChannelInviteRequest{Hash: limited.Invite.Hash, Date: 14}); err != nil {
		t.Fatalf("ImportInvite first limited: %v", err)
	}
	if _, err := service.ImportInvite(ctx, 1003, domain.ImportChannelInviteRequest{Hash: limited.Invite.Hash, Date: 15}); !errors.Is(err, domain.ErrUsersTooMuch) {
		t.Fatalf("ImportInvite usage-limit err = %v, want ErrUsersTooMuch", err)
	}
}

func TestInviteInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Invite Watermark",
		Date:  10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	first, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "already there",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("send existing message: %v", err)
	}
	if _, err := service.InviteToChannel(ctx, 1001, created.Channel.ID, []int64{1002}, 12); err != nil {
		t.Fatalf("InviteToChannel: %v", err)
	}
	view, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel invited: %v", err)
	}
	if view.Self.ReadInboxMaxID != first.Message.ID || view.Dialog.ReadInboxMaxID != first.Message.ID {
		t.Fatalf("invited read watermark self/dialog = %d/%d, want existing top %d", view.Self.ReadInboxMaxID, view.Dialog.ReadInboxMaxID, first.Message.ID)
	}
	if view.Dialog.UnreadCount != 1 {
		t.Fatalf("invited unread = %d, want only invite service message unread", view.Dialog.UnreadCount)
	}
	readers, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		ChannelID: created.Channel.ID,
		MessageID: first.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      13,
	})
	if err != nil {
		t.Fatalf("GetMessageReadParticipants existing message: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("existing message readers after invite = %+v, want none from initial watermark", readers.Participants)
	}
}

func TestJoinChannelInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	ctx := context.Background()
	service := NewService(memory.NewChannelStore())
	created, err := service.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Join Watermark",
		Date:  10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	first, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  1,
		Message:   "before join",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("send existing message: %v", err)
	}
	joined, err := service.JoinChannel(ctx, 1002, created.Channel.ID, 12)
	if err != nil {
		t.Fatalf("JoinChannel: %v", err)
	}
	if joined.Members[0].ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("joined read watermark = %d, want self join service %d", joined.Members[0].ReadInboxMaxID, joined.Message.ID)
	}
	view, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel joined: %v", err)
	}
	if view.Dialog.UnreadCount != 0 || view.Self.ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("joined dialog/self = %+v / %+v, want no unread and read at join service", view.Dialog, view.Self)
	}
	readers, err := service.GetMessageReadParticipants(ctx, 1001, domain.ChannelReadParticipantsRequest{
		ChannelID: created.Channel.ID,
		MessageID: first.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      13,
	})
	if err != nil {
		t.Fatalf("GetMessageReadParticipants existing message: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("existing message readers after join = %+v, want none from initial watermark", readers.Participants)
	}
	future, err := service.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  2,
		Message:   "after join",
		Date:      13,
	})
	if err != nil {
		t.Fatalf("send future message: %v", err)
	}
	after, err := service.GetChannel(ctx, 1002, created.Channel.ID)
	if err != nil {
		t.Fatalf("GetChannel after future: %v", err)
	}
	if after.Dialog.TopMessageID != future.Message.ID || after.Dialog.UnreadCount != 1 {
		t.Fatalf("joined dialog after future = %+v, want top %d unread 1", after.Dialog, future.Message.ID)
	}
}
