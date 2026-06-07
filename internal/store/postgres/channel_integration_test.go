package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
)

func TestChannelStoreSendMessageFansOutDialogRows(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 31,
		Phone:      "+1777" + suffix + "01",
		FirstName:  "ChannelOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 32,
		Phone:      "+1777" + suffix + "02",
		FirstName:  "ChannelFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Dialog Top " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000300,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  901,
		Message:   "first visible channel text",
		Date:      1700000301,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}

	var friendTop, friendReadInbox, friendUnread int
	if err := pool.QueryRow(ctx, `
SELECT top_message_id, read_inbox_max_id, unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, friend.ID).Scan(&friendTop, &friendReadInbox, &friendUnread); err != nil {
		t.Fatalf("read friend dialog row after send: %v", err)
	}
	if friendTop != sent.Message.ID || friendReadInbox != 0 || friendUnread != 2 {
		t.Fatalf("friend dialog row top=%d read=%d unread=%d, want top %d read 0 unread 2", friendTop, friendReadInbox, friendUnread, sent.Message.ID)
	}

	var ownerTop, ownerReadInbox, ownerReadOutbox, ownerUnread int
	if err := pool.QueryRow(ctx, `
SELECT top_message_id, read_inbox_max_id, read_outbox_max_id, unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID).Scan(&ownerTop, &ownerReadInbox, &ownerReadOutbox, &ownerUnread); err != nil {
		t.Fatalf("read owner dialog row after send: %v", err)
	}
	if ownerTop != sent.Message.ID || ownerReadInbox != sent.Message.ID || ownerReadOutbox != sent.Message.ID || ownerUnread != 0 {
		t.Fatalf("owner dialog row top=%d read_in=%d read_out=%d unread=%d, want sent/read/unread 0 for %d", ownerTop, ownerReadInbox, ownerReadOutbox, ownerUnread, sent.Message.ID)
	}

	var ownerMemberReadInbox, ownerMemberReadOutbox int
	if err := pool.QueryRow(ctx, `
SELECT read_inbox_max_id, read_outbox_max_id
FROM channel_members
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID).Scan(&ownerMemberReadInbox, &ownerMemberReadOutbox); err != nil {
		t.Fatalf("read owner member row after send: %v", err)
	}
	if ownerMemberReadInbox != sent.Message.ID || ownerMemberReadOutbox != 0 {
		t.Fatalf("owner member read_in=%d read_out=%d, want read_in %d and read_out unchanged before peer read", ownerMemberReadInbox, ownerMemberReadOutbox, sent.Message.ID)
	}

	dialogs, err := channels.ListChannelDialogs(ctx, friend.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list friend dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].TopMessage != sent.Message.ID {
		t.Fatalf("friend dialogs = %+v, want top message %d", dialogs.Dialogs, sent.Message.ID)
	}
	if len(dialogs.Messages) != 1 || dialogs.Messages[0].Body != "first visible channel text" {
		t.Fatalf("friend dialog messages = %+v, want latest channel text", dialogs.Messages)
	}
	if dialogs.Dialogs[0].UnreadCount != 2 {
		t.Fatalf("friend unread = %d, want create service + latest text", dialogs.Dialogs[0].UnreadCount)
	}

	read, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		MaxID:     sent.Message.ID,
		Date:      1700000302,
	})
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if read.Dialog.UnreadCount != 0 || read.Dialog.ReadInboxMaxID != sent.Message.ID {
		t.Fatalf("read dialog = %+v, want fully read through latest", read.Dialog)
	}
	if len(read.OutboxUpdates) != 1 || read.OutboxUpdates[0].UserID != owner.ID || read.OutboxUpdates[0].MaxID != sent.Message.ID {
		t.Fatalf("read outbox updates = %+v, want owner max id %d", read.OutboxUpdates, sent.Message.ID)
	}
	ownerView, err := channels.GetChannel(ctx, owner.ID, channelID)
	if err != nil {
		t.Fatalf("get owner channel after read: %v", err)
	}
	if ownerView.Dialog.ReadOutboxMaxID != sent.Message.ID {
		t.Fatalf("owner dialog read_outbox = %d, want %d", ownerView.Dialog.ReadOutboxMaxID, sent.Message.ID)
	}
	if changed, err := channels.SetChannelDialogPinned(ctx, owner.ID, channelID, true); err != nil || !changed {
		t.Fatalf("set owner channel pinned = changed %v err %v, want changed", changed, err)
	}
	if err := channels.ReorderChannelPinnedDialogs(ctx, owner.ID, []domain.Peer{
		{Type: domain.PeerTypeChannel, ID: channelID},
	}, true); err != nil {
		t.Fatalf("reorder owner channel pinned: %v", err)
	}
	if changed, err := channels.SetChannelDialogUnreadMark(ctx, owner.ID, channelID, true); err != nil || !changed {
		t.Fatalf("set owner channel unread mark = changed %v err %v, want changed", changed, err)
	}
	if err := channels.EditChannelPeerFolders(ctx, owner.ID, []domain.FolderPeerUpdate{
		{Peer: domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}, FolderID: domain.DialogArchiveFolderID},
	}); err != nil {
		t.Fatalf("edit owner channel folder: %v", err)
	}
	ownerDialogs, err := channels.GetChannelDialogs(ctx, owner.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get owner channel dialogs after settings: %v", err)
	}
	if len(ownerDialogs.Dialogs) != 1 || !ownerDialogs.Dialogs[0].Pinned || ownerDialogs.Dialogs[0].PinnedOrder != 1 || !ownerDialogs.Dialogs[0].UnreadMark || ownerDialogs.Dialogs[0].FolderID != domain.DialogArchiveFolderID {
		t.Fatalf("owner channel dialog settings = %+v, want pinned/unread/archive", ownerDialogs.Dialogs)
	}
	unreadMarks, err := channels.ListChannelUnreadMarked(ctx, owner.ID)
	if err != nil {
		t.Fatalf("list owner channel unread marks: %v", err)
	}
	if len(unreadMarks) != 1 || unreadMarks[0].ID != channelID || unreadMarks[0].Type != domain.PeerTypeChannel {
		t.Fatalf("channel unread marks = %+v, want channel", unreadMarks)
	}

	readers, err := channels.ListMessageReadParticipants(ctx, domain.ChannelReadParticipantsRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MessageID: sent.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      1700000303,
	})
	if err != nil {
		t.Fatalf("list message read participants: %v", err)
	}
	if len(readers.Participants) != 1 || readers.Participants[0].UserID != friend.ID || readers.Participants[0].Date != 1700000302 {
		t.Fatalf("read participants = %+v, want friend read date", readers.Participants)
	}

	cleared, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		MaxID:     sent.Message.ID,
		Date:      1700000302,
	})
	if err != nil {
		t.Fatalf("local clear history: %v", err)
	}
	if cleared.AvailableMinID != sent.Message.ID {
		t.Fatalf("local clear available_min_id = %d, want %d", cleared.AvailableMinID, sent.Message.ID)
	}
	staleClear, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		MaxID:     created.Message.ID,
		Date:      1700000303,
	})
	if err != nil {
		t.Fatalf("stale local clear history: %v", err)
	}
	if staleClear.AvailableMinID != sent.Message.ID {
		t.Fatalf("stale local clear available_min_id = %d, want monotonic %d", staleClear.AvailableMinID, sent.Message.ID)
	}
	afterClear, err := channels.GetChannel(ctx, friend.ID, channelID)
	if err != nil {
		t.Fatalf("get channel after clear: %v", err)
	}
	if afterClear.Dialog.TopMessageID != 0 {
		t.Fatalf("dialog after clear = %+v, want no visible top", afterClear.Dialog)
	}

	next, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  902,
		Message:   "after local clear",
		Date:      1700000304,
	})
	if err != nil {
		t.Fatalf("send after clear: %v", err)
	}
	afterNext, err := channels.GetChannel(ctx, friend.ID, channelID)
	if err != nil {
		t.Fatalf("get channel after next: %v", err)
	}
	if afterNext.Dialog.TopMessageID != next.Message.ID || afterNext.Dialog.UnreadCount != 1 {
		t.Fatalf("dialog after next = %+v, want top %d unread 1", afterNext.Dialog, next.Message.ID)
	}
}

func TestChannelStoreReadOutboxDoesNotRegressSenderDialogUnread(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 35,
		Phone:      "+1777" + suffix + "17",
		FirstName:  "ReadOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 36,
		Phone:      "+1777" + suffix + "18",
		FirstName:  "ReadMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Read Outbox " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000340,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	ownerMsg, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9341,
		Message:   "owner message before member reply",
		Date:      1700000341,
	})
	if err != nil {
		t.Fatalf("send owner message: %v", err)
	}
	if _, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     ownerMsg.Message.ID,
		Date:      1700000342,
	}); err != nil {
		t.Fatalf("member read owner message: %v", err)
	}
	memberMsg, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		RandomID:  9342,
		Message:   "member reply should stay read for sender",
		Date:      1700000343,
	})
	if err != nil {
		t.Fatalf("send member message: %v", err)
	}

	var storedReadInbox, storedUnread int
	if err := pool.QueryRow(ctx, `
SELECT read_inbox_max_id, unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, member.ID).Scan(&storedReadInbox, &storedUnread); err != nil {
		t.Fatalf("read member dialog after self send: %v", err)
	}
	if storedReadInbox != memberMsg.Message.ID || storedUnread != 0 {
		t.Fatalf("member dialog after self send read=%d unread=%d, want read %d unread 0", storedReadInbox, storedUnread, memberMsg.Message.ID)
	}

	read, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MaxID:     memberMsg.Message.ID,
		Date:      1700000344,
	})
	if err != nil {
		t.Fatalf("owner read member message: %v", err)
	}
	if len(read.OutboxUpdates) != 1 || read.OutboxUpdates[0].UserID != member.ID || read.OutboxUpdates[0].MaxID != memberMsg.Message.ID {
		t.Fatalf("read outbox updates = %+v, want member max id %d", read.OutboxUpdates, memberMsg.Message.ID)
	}
	if err := pool.QueryRow(ctx, `
SELECT read_inbox_max_id, unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, member.ID).Scan(&storedReadInbox, &storedUnread); err != nil {
		t.Fatalf("read member dialog after owner read: %v", err)
	}
	if storedReadInbox != memberMsg.Message.ID || storedUnread != 0 {
		t.Fatalf("member dialog after owner read read=%d unread=%d, want read %d unread 0", storedReadInbox, storedUnread, memberMsg.Message.ID)
	}
}

func TestChannelStoreChannelUnreadExcludesOwnOutgoing(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 39,
		Phone:      "+1777" + suffix + "19",
		FirstName:  "OwnUnreadOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Own Unread " + suffix,
		Megagroup:     true,
		Date:          1700000350,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9351,
		Message:   "own outgoing should not be unread",
		Date:      1700000351,
	})
	if err != nil {
		t.Fatalf("send owner message: %v", err)
	}
	readBeforeOwnMessage := sent.Message.ID - 1
	if _, err := pool.Exec(ctx, `
UPDATE channel_members
SET read_inbox_max_id = $3, unread_mark = false
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID, readBeforeOwnMessage); err != nil {
		t.Fatalf("regress owner member read watermark: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_dialogs
SET read_inbox_max_id = $3, unread_count = 0, unread_mark = false
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID, readBeforeOwnMessage); err != nil {
		t.Fatalf("regress owner dialog unread: %v", err)
	}

	dialogs, err := channels.GetChannelDialogs(ctx, owner.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get owner channel dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 {
		t.Fatalf("dialogs = %+v, want one dialog", dialogs.Dialogs)
	}
	if dialogs.Dialogs[0].UnreadCount != 0 {
		t.Fatalf("owner dialog unread = %d, want own outgoing excluded", dialogs.Dialogs[0].UnreadCount)
	}
	unreadOnly, err := channels.ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{
		Folder: &domain.DialogFolder{ExcludeRead: true, Groups: true},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list unread-only channel dialogs: %v", err)
	}
	for _, dialog := range unreadOnly.Dialogs {
		if dialog.Peer.ID == channelID {
			t.Fatalf("unread-only dialogs include own-outgoing-only channel: %+v", unreadOnly.Dialogs)
		}
	}
	if _, err := pool.Exec(ctx, `
UPDATE channel_dialogs
SET unread_count = 99
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID); err != nil {
		t.Fatalf("corrupt owner dialog unread before read repair: %v", err)
	}

	read, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MaxID:     sent.Message.ID,
		Date:      1700000352,
	})
	if err != nil {
		t.Fatalf("read owner channel history: %v", err)
	}
	if read.StillUnreadCount != 0 || read.Dialog.UnreadCount != 0 {
		t.Fatalf("read result = %+v, want no unread own outgoing messages", read)
	}
	var storedUnread int
	if err := pool.QueryRow(ctx, `
SELECT unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, owner.ID).Scan(&storedUnread); err != nil {
		t.Fatalf("read stored owner unread: %v", err)
	}
	if storedUnread != 0 {
		t.Fatalf("stored owner unread = %d, want repaired to 0", storedUnread)
	}
}

func TestChannelStoreConcurrentSendAndReadHistoryDoNotSurfaceDeadlock(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 37,
		Phone:      "+1777" + suffix + "21",
		FirstName:  "ConcurrentOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 38,
		Phone:      "+1777" + suffix + "22",
		FirstName:  "ConcurrentMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(context.Background(), "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(context.Background(), "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Send Read Race " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000450,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	first, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  1700000451,
		Message:   "seed",
		Date:      1700000451,
	})
	if err != nil {
		t.Fatalf("seed send: %v", err)
	}

	for i := 0; i < 20; i++ {
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func(iter int) {
			defer wg.Done()
			<-start
			_, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
				UserID:    member.ID,
				ChannelID: channelID,
				RandomID:  int64(1700000500 + iter),
				Message:   fmt.Sprintf("race send %d", iter),
				Date:      1700000500 + iter,
			})
			errs <- err
		}(i)
		go func() {
			defer wg.Done()
			<-start
			_, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
				UserID:    member.ID,
				ChannelID: channelID,
				MaxID:     first.Message.ID,
				Date:      1700000600,
			})
			errs <- err
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent send/read iteration %d: %v", i, err)
			}
		}
	}
}

func TestChannelStoreJoinInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 131,
		Phone:      "+1777" + suffix + "11",
		FirstName:  "JoinOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 132,
		Phone:      "+1777" + suffix + "12",
		FirstName:  "JoinFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Join Watermark " + suffix,
		Megagroup:     true,
		Date:          1700000320,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	first, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  905,
		Message:   "before join",
		Date:      1700000321,
	})
	if err != nil {
		t.Fatalf("send existing message: %v", err)
	}
	joined, err := channels.JoinChannel(ctx, channelID, friend.ID, 1700000322)
	if err != nil {
		t.Fatalf("join channel: %v", err)
	}
	if _, err := channels.JoinChannel(ctx, channelID, friend.ID, 1700000323); !errors.Is(err, domain.ErrUserAlreadyParticipant) {
		t.Fatalf("duplicate join err = %v, want ErrUserAlreadyParticipant", err)
	}
	if len(joined.Members) != 1 || joined.Members[0].ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("joined member = %+v message=%+v, want read watermark at self join service", joined.Members, joined.Message)
	}
	view, err := channels.GetChannel(ctx, friend.ID, channelID)
	if err != nil {
		t.Fatalf("get joined channel: %v", err)
	}
	if view.Dialog.UnreadCount != 0 || view.Self.ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("joined view dialog/self = %+v / %+v, want no unread and read at join service", view.Dialog, view.Self)
	}
	readers, err := channels.ListMessageReadParticipants(ctx, domain.ChannelReadParticipantsRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MessageID: first.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      1700000323,
	})
	if err != nil {
		t.Fatalf("list read participants existing message: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("existing message readers after join = %+v, want none from initial watermark", readers.Participants)
	}
	future, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  906,
		Message:   "after join",
		Date:      1700000323,
	})
	if err != nil {
		t.Fatalf("send future message: %v", err)
	}
	after, err := channels.GetChannel(ctx, friend.ID, channelID)
	if err != nil {
		t.Fatalf("get channel after future: %v", err)
	}
	if after.Dialog.TopMessageID != future.Message.ID || after.Dialog.UnreadCount != 1 {
		t.Fatalf("joined dialog after future = %+v, want top %d unread 1", after.Dialog, future.Message.ID)
	}
}

func TestChannelStoreJoinRejectsKickedMember(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 33,
		Phone:      "+1777" + suffix + "21",
		FirstName:  "BanOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 34,
		Phone:      "+1777" + suffix + "22",
		FirstName:  "BanMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	helper, err := users.Create(ctx, domain.User{
		AccessHash: 35,
		Phone:      "+1777" + suffix + "23",
		FirstName:  "BanHelper",
	})
	if err != nil {
		t.Fatalf("create helper: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID, helper.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Ban Join " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID, helper.ID},
		Date:          1700000305,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	ptsFloor := created.Channel.Pts
	banned, err := channels.EditChannelBanned(ctx, domain.EditChannelBannedRequest{
		UserID:      owner.ID,
		ChannelID:   channelID,
		Participant: domain.Peer{Type: domain.PeerTypeUser, ID: member.ID},
		BannedRights: domain.ChannelBannedRights{
			ViewMessages: true,
			UntilDate:    1700001300,
		},
		Date: 1700000306,
	})
	if err != nil {
		t.Fatalf("kick member: %v", err)
	}
	if banned.Event.Pts != 0 || banned.Event.PtsCount != 0 || banned.Channel.Pts != ptsFloor {
		t.Fatalf("kick affected channel pts = event(%d,%d) channel %d, want no pts advance from %d", banned.Event.Pts, banned.Event.PtsCount, banned.Channel.Pts, ptsFloor)
	}
	banDiff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Pts:       ptsFloor,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("difference after kick: %v", err)
	}
	if len(banDiff.Events) != 0 || banDiff.Pts != ptsFloor {
		t.Fatalf("difference after kick = %+v, want no durable participant event at pts %d", banDiff, ptsFloor)
	}
	if _, err := channels.JoinChannel(ctx, channelID, member.ID, 1700000307); !errors.Is(err, domain.ErrChannelUserBanned) {
		t.Fatalf("kicked JoinChannel err = %v, want ErrChannelUserBanned", err)
	}
	if _, err := channels.InviteToChannel(ctx, channelID, helper.ID, []int64{member.ID}, 1700000308); !errors.Is(err, domain.ErrUserKicked) {
		t.Fatalf("helper InviteToChannel kicked err = %v, want ErrUserKicked", err)
	}
	restored, err := channels.InviteToChannel(ctx, channelID, owner.ID, []int64{member.ID}, 1700000309)
	if err != nil {
		t.Fatalf("owner InviteToChannel kicked: %v", err)
	}
	if len(restored.Members) != 1 || restored.Members[0].Status != domain.ChannelMemberActive || restored.Members[0].BannedRights != (domain.ChannelBannedRights{}) {
		t.Fatalf("restored members = %+v, want active unbanned member", restored.Members)
	}
	if restored.Channel.ParticipantsCount != 3 || restored.Channel.KickedCount != 0 {
		t.Fatalf("restored counts = participants:%d kicked:%d, want 3/0", restored.Channel.ParticipantsCount, restored.Channel.KickedCount)
	}
	if _, err := channels.InviteToChannel(ctx, channelID, owner.ID, []int64{member.ID}, 1700000310); !errors.Is(err, domain.ErrUserAlreadyParticipant) {
		t.Fatalf("duplicate InviteToChannel err = %v, want ErrUserAlreadyParticipant", err)
	}
}

func TestChannelStoreInviteInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1888" + suffix + "01",
		FirstName:  "InviteOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	invited, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1888" + suffix + "02",
		FirstName:  "InviteMember",
	})
	if err != nil {
		t.Fatalf("create invited: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, invited.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Invite Watermark " + suffix,
		Megagroup:     true,
		Date:          1700000320,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	first, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  902,
		Message:   "already visible before invite",
		Date:      1700000321,
	})
	if err != nil {
		t.Fatalf("send existing channel message: %v", err)
	}
	if _, err := channels.InviteToChannel(ctx, channelID, owner.ID, []int64{invited.ID}, 1700000322); err != nil {
		t.Fatalf("invite to channel: %v", err)
	}

	view, err := channels.GetChannel(ctx, invited.ID, channelID)
	if err != nil {
		t.Fatalf("get invited channel: %v", err)
	}
	if view.Self.ReadInboxMaxID != first.Message.ID || view.Dialog.ReadInboxMaxID != first.Message.ID {
		t.Fatalf("invited read watermark self/dialog = %d/%d, want existing top %d", view.Self.ReadInboxMaxID, view.Dialog.ReadInboxMaxID, first.Message.ID)
	}
	if view.Dialog.UnreadCount != 1 {
		t.Fatalf("invited unread = %d, want only invite service message unread", view.Dialog.UnreadCount)
	}
}

func TestChannelStoreImportInviteInitialReadWatermarkSkipsExistingHistory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 51,
		Phone:      "+1888" + suffix + "11",
		FirstName:  "ImportOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	joiner, err := users.Create(ctx, domain.User{
		AccessHash: 52,
		Phone:      "+1888" + suffix + "12",
		FirstName:  "ImportJoiner",
	})
	if err != nil {
		t.Fatalf("create joiner: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, joiner.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Import Watermark " + suffix,
		Megagroup:     true,
		Date:          1700000330,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	first, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  912,
		Message:   "already visible before import",
		Date:      1700000331,
	})
	if err != nil {
		t.Fatalf("send existing channel message: %v", err)
	}
	invite, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Title:     "join",
		Date:      1700000332,
	})
	if err != nil {
		t.Fatalf("export invite: %v", err)
	}
	joined, err := channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: joiner.ID,
		Hash:   invite.Invite.Hash,
		Date:   1700000333,
	})
	if err != nil {
		t.Fatalf("import invite: %v", err)
	}
	if len(joined.Members) != 1 || joined.Members[0].ReadInboxMaxID != joined.Message.ID || joined.Members[0].ReadOutboxMaxID != joined.Message.ID {
		t.Fatalf("imported member = %+v message=%+v, want read watermarks at self join service", joined.Members, joined.Message)
	}
	view, err := channels.GetChannel(ctx, joiner.ID, channelID)
	if err != nil {
		t.Fatalf("get imported channel: %v", err)
	}
	if view.Dialog.UnreadCount != 0 || view.Self.ReadInboxMaxID != joined.Message.ID {
		t.Fatalf("imported view dialog/self = %+v / %+v, want no unread and read at join service", view.Dialog, view.Self)
	}
	readers, err := channels.ListMessageReadParticipants(ctx, domain.ChannelReadParticipantsRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MessageID: first.Message.ID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      1700000334,
	})
	if err != nil {
		t.Fatalf("list read participants existing message: %v", err)
	}
	if len(readers.Participants) != 0 {
		t.Fatalf("existing message readers after import = %+v, want none from initial watermark", readers.Participants)
	}
}

func TestChannelStoreImportInviteRequestNeededAndUsageLimitErrors(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 61,
		Phone:      "+1888" + suffix + "21",
		FirstName:  "InviteErrorOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	first, err := users.Create(ctx, domain.User{
		AccessHash: 62,
		Phone:      "+1888" + suffix + "22",
		FirstName:  "InviteErrorFirst",
	})
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	second, err := users.Create(ctx, domain.User{
		AccessHash: 63,
		Phone:      "+1888" + suffix + "23",
		FirstName:  "InviteErrorSecond",
	})
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, first.ID, second.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Invite Errors " + suffix,
		Megagroup:     true,
		Date:          1700000340,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	requested, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:        owner.ID,
		ChannelID:     channelID,
		Title:         "approval",
		RequestNeeded: true,
		Date:          1700000341,
	})
	if err != nil {
		t.Fatalf("export request-needed invite: %v", err)
	}
	if _, err := channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: first.ID,
		Hash:   requested.Invite.Hash,
		Date:   1700000342,
	}); !errors.Is(err, domain.ErrInviteRequestSent) {
		t.Fatalf("import request-needed err = %v, want ErrInviteRequestSent", err)
	}
	limited, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:     owner.ID,
		ChannelID:  channelID,
		Title:      "one",
		UsageLimit: 1,
		Date:       1700000343,
	})
	if err != nil {
		t.Fatalf("export limited invite: %v", err)
	}
	if _, err := channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: first.ID,
		Hash:   limited.Invite.Hash,
		Date:   1700000344,
	}); err != nil {
		t.Fatalf("first import limited invite: %v", err)
	}
	if _, err := channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
		UserID: second.ID,
		Hash:   limited.Invite.Hash,
		Date:   1700000345,
	}); !errors.Is(err, domain.ErrUsersTooMuch) {
		t.Fatalf("second import limited invite err = %v, want ErrUsersTooMuch", err)
	}
}

func TestChannelStorePendingJoinRequestsSummaryAndInviteAdmins(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	userIDs := make([]int64, 0, 11)
	createUser := func(label string, accessHash int64, phoneSuffix int) domain.User {
		t.Helper()
		user, err := users.Create(ctx, domain.User{
			AccessHash: accessHash,
			Phone:      fmt.Sprintf("+1889%s%02d", suffix, phoneSuffix),
			FirstName:  label,
		})
		if err != nil {
			t.Fatalf("create %s: %v", label, err)
		}
		userIDs = append(userIDs, user.ID)
		return user
	}
	owner := createUser("PendingOwner", 71, 1)
	inviteAdmin := createUser("PendingInviteAdmin", 72, 2)
	plainMember := createUser("PendingPlainMember", 73, 3)
	changeAdmin := createUser("PendingChangeAdmin", 74, 4)
	requesters := make([]domain.User, 0, domain.MaxChannelPendingJoinRecentRequesters+2)
	for i := 0; i < domain.MaxChannelPendingJoinRecentRequesters+2; i++ {
		requesters = append(requesters, createUser("PendingRequester", int64(80+i), 10+i))
	}

	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Pending Summary " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{inviteAdmin.ID, plainMember.ID, changeAdmin.ID},
		Date:          1700000360,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MemberID:  inviteAdmin.ID,
		AdminRights: domain.ChannelAdminRights{
			InviteUsers: true,
		},
		Date: 1700000361,
	}); err != nil {
		t.Fatalf("promote invite admin: %v", err)
	}
	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MemberID:  changeAdmin.ID,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo: true,
		},
		Date: 1700000362,
	}); err != nil {
		t.Fatalf("promote change-info admin: %v", err)
	}
	invite, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:        owner.ID,
		ChannelID:     channelID,
		Title:         "approval",
		RequestNeeded: true,
		Date:          1700000363,
	})
	if err != nil {
		t.Fatalf("export invite: %v", err)
	}
	for i, requester := range requesters {
		_, err := channels.ImportInvite(ctx, domain.ImportChannelInviteRequest{
			UserID: requester.ID,
			Hash:   invite.Invite.Hash,
			Date:   1700000370 + i,
		})
		if !errors.Is(err, domain.ErrInviteRequestSent) {
			t.Fatalf("import pending %d err = %v, want ErrInviteRequestSent", i, err)
		}
	}
	pending, err := channels.PendingJoinRequests(ctx, channelID, 99)
	if err != nil {
		t.Fatalf("pending join requests: %v", err)
	}
	if pending.Count != len(requesters) || len(pending.RecentRequesters) != domain.MaxChannelPendingJoinRecentRequesters {
		t.Fatalf("pending summary = %+v, want bounded recent with full count", pending)
	}
	if pending.RecentRequesters[0] != requesters[len(requesters)-1].ID ||
		pending.RecentRequesters[len(pending.RecentRequesters)-1] != requesters[2].ID {
		t.Fatalf("recent requesters = %+v, want newest first", pending.RecentRequesters)
	}
	admins, err := channels.ListChannelInviteAdminMemberIDs(ctx, channelID, 0)
	if err != nil {
		t.Fatalf("invite admins: %v", err)
	}
	want := []int64{owner.ID, inviteAdmin.ID, changeAdmin.ID}
	if !reflect.DeepEqual(admins, want) {
		t.Fatalf("invite admins = %+v, want %+v", admins, want)
	}
}

func TestChannelStoreImportInviteUsageLimitSeesConcurrentIncrement(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 64,
		Phone:      "+1888" + suffix + "31",
		FirstName:  "InviteLimitOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	joiner, err := users.Create(ctx, domain.User{
		AccessHash: 65,
		Phone:      "+1888" + suffix + "32",
		FirstName:  "InviteLimitJoiner",
	})
	if err != nil {
		t.Fatalf("create joiner: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, joiner.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Invite Limit Race " + suffix,
		Megagroup:     true,
		Date:          1700000350,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	invite, err := channels.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:     owner.ID,
		ChannelID:  channelID,
		Title:      "single",
		UsageLimit: 1,
		Date:       1700000351,
	})
	if err != nil {
		t.Fatalf("export limited invite: %v", err)
	}

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `
UPDATE channel_invites
SET usage_count = usage_limit
WHERE channel_id = $1 AND invite_id = $2`, channelID, invite.Invite.InviteID); err != nil {
		t.Fatalf("lock and update invite usage: %v", err)
	}

	importCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := channels.ImportInvite(importCtx, domain.ImportChannelInviteRequest{
			UserID: joiner.ID,
			Hash:   invite.Invite.Hash,
			Date:   1700000352,
		})
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("commit lock tx: %v", err)
	}
	err = <-errCh
	if !errors.Is(err, domain.ErrUsersTooMuch) {
		t.Fatalf("concurrent import err = %v, want ErrUsersTooMuch after seeing committed usage_count", err)
	}
	if _, err := channels.GetChannel(ctx, joiner.ID, channelID); !errors.Is(err, domain.ErrChannelPrivate) {
		t.Fatalf("joiner channel after rejected import err = %v, want ErrChannelPrivate", err)
	}
}

func TestChannelStoreListDialogsUsesDateAndOffset(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 35,
		Phone:      "+1777" + suffix + "03",
		FirstName:  "DialogPageOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) > 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	channels := NewChannelStore(pool)
	older, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Older Dialog " + suffix,
		Megagroup:     true,
		Date:          1700000310,
	})
	if err != nil {
		t.Fatalf("create older channel: %v", err)
	}
	channelIDs = append(channelIDs, older.Channel.ID)
	newer, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Newer Dialog " + suffix,
		Megagroup:     true,
		Date:          1700000320,
	})
	if err != nil {
		t.Fatalf("create newer channel: %v", err)
	}
	channelIDs = append(channelIDs, newer.Channel.ID)

	first, err := channels.ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{Limit: 1})
	if err != nil {
		t.Fatalf("list first channel dialogs: %v", err)
	}
	if len(first.Dialogs) != 1 || first.Dialogs[0].Peer.ID != newer.Channel.ID {
		t.Fatalf("first page dialogs = %+v, want newer channel by top date", first.Dialogs)
	}

	next, err := channels.ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{
		OffsetDate:    first.Dialogs[0].TopMessageDate,
		OffsetID:      first.Dialogs[0].TopMessage,
		HasOffsetPeer: true,
		OffsetPeer:    first.Dialogs[0].Peer,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("list next channel dialogs: %v", err)
	}
	if len(next.Dialogs) != 1 || next.Dialogs[0].Peer.ID != older.Channel.ID {
		t.Fatalf("next page dialogs = %+v, want older channel without repeating offset peer", next.Dialogs)
	}
}

func TestChannelStoreDifferenceStartsAtMemberAvailableMinPts(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1778" + suffix + "01",
		FirstName:  "PtsOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1778" + suffix + "02",
		FirstName:  "PtsMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	joiner, err := users.Create(ctx, domain.User{
		AccessHash: 43,
		Phone:      "+1778" + suffix + "03",
		FirstName:  "PtsJoiner",
	})
	if err != nil {
		t.Fatalf("create joiner: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID, joiner.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "PTS Floor " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000350,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	ptsFloor := created.Channel.Pts
	promoted, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MemberID:  member.ID,
		AdminRights: domain.ChannelAdminRights{
			InviteUsers: true,
		},
		Date: 1700000351,
	})
	if err != nil {
		t.Fatalf("edit admin: %v", err)
	}
	if promoted.Event.Pts != 0 || promoted.Event.PtsCount != 0 || promoted.Channel.Pts != ptsFloor {
		t.Fatalf("promote affected channel pts = event(%d,%d) channel %d, want no pts advance from %d", promoted.Event.Pts, promoted.Event.PtsCount, promoted.Channel.Pts, ptsFloor)
	}
	adminDiff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		Pts:       ptsFloor,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("difference after promote: %v", err)
	}
	if len(adminDiff.Events) != 0 || adminDiff.Pts != ptsFloor {
		t.Fatalf("difference after promote = %+v, want no durable participant event at pts %d", adminDiff, ptsFloor)
	}
	joined, err := channels.JoinChannel(ctx, channelID, joiner.ID, 1700000352)
	if err != nil {
		t.Fatalf("join channel: %v", err)
	}
	if len(joined.Members) != 1 || joined.Members[0].AvailableMinPts != ptsFloor {
		t.Fatalf("joined members = %+v, want available_min_pts %d", joined.Members, ptsFloor)
	}
	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    joiner.ID,
		ChannelID: channelID,
		Pts:       0,
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("list channel difference: %v", err)
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

func TestChannelStorePublicPreviewDifferenceAllowsNonMember(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 241,
		Phone:      "+1778" + suffix + "41",
		FirstName:  "PreviewDiffOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	viewer, err := users.Create(ctx, domain.User{
		AccessHash: 242,
		Phone:      "+1778" + suffix + "42",
		FirstName:  "PreviewDiffViewer",
	})
	if err != nil {
		t.Fatalf("create viewer: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, viewer.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Preview Difference " + suffix,
		Broadcast:     true,
		Date:          1700000370,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.UpdateUsername(ctx, domain.UpdateChannelUsernameRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Username:  "preview_diff_" + suffix,
	}); err != nil {
		t.Fatalf("update username: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  1700000371,
		Message:   "public preview difference",
		Date:      1700000371,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}

	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    viewer.ID,
		ChannelID: channelID,
		Pts:       created.Event.Pts,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list public preview difference: %v", err)
	}
	if !diff.Final || diff.Pts != sent.Event.Pts || len(diff.NewMessages) != 1 || diff.NewMessages[0].Body != "public preview difference" {
		t.Fatalf("preview diff = %+v, want one public preview message at current pts", diff)
	}
	if diff.Dialog.UnreadCount != 0 || diff.Dialog.ReadInboxMaxID < sent.Message.ID {
		t.Fatalf("preview diff dialog = %+v, want read-only public preview dialog", diff.Dialog)
	}
}

func TestChannelStoreListDialogsDerivesRecipientTopWithoutWriteFanout(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1777" + suffix + "31",
		FirstName:  "DialogTopOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1777" + suffix + "32",
		FirstName:  "DialogTopMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Dialog Top " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000330,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     created.Message.ID,
		Date:      1700000331,
	}); err != nil {
		t.Fatalf("read initial service message: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9331,
		Message:   "recipient top without write fanout",
		Date:      1700000332,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}

	list, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list recipient channel dialogs: %v", err)
	}
	if len(list.Dialogs) != 1 {
		t.Fatalf("dialogs = %+v, want one channel dialog", list.Dialogs)
	}
	dialog := list.Dialogs[0]
	if dialog.TopMessage != sent.Message.ID || dialog.TopMessageDate != sent.Message.Date || dialog.UnreadCount != 1 {
		t.Fatalf("recipient dialog = %+v, want top sent message and unread=1", dialog)
	}
	if len(list.Messages) != 1 || list.Messages[0].ID != sent.Message.ID {
		t.Fatalf("dialog messages = %+v, want sent top message", list.Messages)
	}
}

func TestChannelStoreBroadcastUnreadDerivesDespiteStaleCache(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 421,
		Phone:      "+1777" + suffix + "33",
		FirstName:  "BroadcastUnreadOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 422,
		Phone:      "+1777" + suffix + "34",
		FirstName:  "BroadcastUnreadMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Broadcast Unread " + suffix,
		Broadcast:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000333,
	})
	if err != nil {
		t.Fatalf("create broadcast channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     created.Message.ID,
		Date:      1700000334,
	}); err != nil {
		t.Fatalf("read initial broadcast service message: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9333,
		Message:   "broadcast unread derives despite stale cache",
		Date:      1700000335,
	})
	if err != nil {
		t.Fatalf("send broadcast message: %v", err)
	}

	var storedUnread int
	if err := pool.QueryRow(ctx, `
SELECT unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, member.ID).Scan(&storedUnread); err != nil {
		t.Fatalf("read stale broadcast dialog cache: %v", err)
	}
	if storedUnread != 0 {
		t.Fatalf("stored broadcast unread cache = %d, want no send fanout", storedUnread)
	}

	list, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list broadcast channel dialogs: %v", err)
	}
	if len(list.Dialogs) != 1 || list.Dialogs[0].TopMessage != sent.Message.ID || list.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("broadcast dialogs = %+v, want sent top and dynamic unread=1", list.Dialogs)
	}

	unreadOnly, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{
		Folder: &domain.DialogFolder{ExcludeRead: true, Broadcasts: true},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list unread-only broadcast dialogs: %v", err)
	}
	if len(unreadOnly.Dialogs) != 1 || unreadOnly.Dialogs[0].Peer.ID != channelID {
		t.Fatalf("unread-only broadcast dialogs = %+v, want stale-cache channel included", unreadOnly.Dialogs)
	}

	view, err := channels.GetChannel(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("get broadcast channel: %v", err)
	}
	if view.Dialog.UnreadCount != 1 || view.Dialog.TopMessageID != sent.Message.ID {
		t.Fatalf("broadcast view dialog = %+v, want dynamic unread=1", view.Dialog)
	}
	dialogs, err := channels.GetChannelDialogs(ctx, member.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get broadcast channel dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("get broadcast dialogs = %+v, want dynamic unread=1", dialogs.Dialogs)
	}
}

func TestChannelStoreLargeMegagroupUnreadDerivesDespiteStaleCache(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 431,
		Phone:      "+1777" + suffix + "35",
		FirstName:  "LargeUnreadOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 432,
		Phone:      "+1777" + suffix + "36",
		FirstName:  "LargeUnreadMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Large Unread " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000336,
	})
	if err != nil {
		t.Fatalf("create large megagroup: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     created.Message.ID,
		Date:      1700000337,
	}); err != nil {
		t.Fatalf("read initial large service message: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channels
SET participants_count = $2
WHERE id = $1`, channelID, domain.MaxSynchronousChannelDialogFanout+1); err != nil {
		t.Fatalf("mark megagroup as over synchronous fanout threshold: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9336,
		Message:   "large megagroup unread derives despite stale cache",
		Date:      1700000338,
	})
	if err != nil {
		t.Fatalf("send large megagroup message: %v", err)
	}

	var storedTop, storedUnread int
	if err := pool.QueryRow(ctx, `
SELECT top_message_id, unread_count
FROM channel_dialogs
WHERE channel_id = $1 AND user_id = $2`, channelID, member.ID).Scan(&storedTop, &storedUnread); err != nil {
		t.Fatalf("read stale large dialog cache: %v", err)
	}
	if storedTop == sent.Message.ID || storedUnread != 0 {
		t.Fatalf("stored large dialog cache top=%d unread=%d, want stale top and unread=0", storedTop, storedUnread)
	}

	list, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list large channel dialogs: %v", err)
	}
	if len(list.Dialogs) != 1 || list.Dialogs[0].TopMessage != sent.Message.ID || list.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("large dialogs = %+v, want sent top and dynamic unread=1", list.Dialogs)
	}
	ownerView, err := channels.GetChannel(ctx, owner.ID, channelID)
	if err != nil {
		t.Fatalf("get large channel for sender: %v", err)
	}
	if ownerView.Dialog.UnreadCount != 0 {
		t.Fatalf("large sender dialog = %+v, want own outgoing excluded from dynamic unread", ownerView.Dialog)
	}

	unreadOnly, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{
		Folder: &domain.DialogFolder{ExcludeRead: true, Groups: true},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list unread-only large dialogs: %v", err)
	}
	if len(unreadOnly.Dialogs) != 1 || unreadOnly.Dialogs[0].Peer.ID != channelID {
		t.Fatalf("unread-only large dialogs = %+v, want stale-cache channel included", unreadOnly.Dialogs)
	}

	view, err := channels.GetChannel(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("get large channel: %v", err)
	}
	if view.Dialog.UnreadCount != 1 || view.Dialog.TopMessageID != sent.Message.ID {
		t.Fatalf("large view dialog = %+v, want dynamic unread=1", view.Dialog)
	}
	dialogs, err := channels.GetChannelDialogs(ctx, member.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get large channel dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("get large dialogs = %+v, want dynamic unread=1", dialogs.Dialogs)
	}

	cleared, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     sent.Message.ID,
		Date:      1700000339,
	})
	if err != nil {
		t.Fatalf("local clear large history: %v", err)
	}
	if cleared.AvailableMinID != sent.Message.ID {
		t.Fatalf("large local clear available_min_id = %d, want %d", cleared.AvailableMinID, sent.Message.ID)
	}
	afterClear, err := channels.GetChannel(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("get large channel after local clear: %v", err)
	}
	if afterClear.Dialog.TopMessageID != 0 || afterClear.Dialog.UnreadCount != 0 {
		t.Fatalf("large dialog after local clear = %+v, want no visible unread top", afterClear.Dialog)
	}
}

func TestChannelStoreLargeMegagroupUnreadSkipsDeletedHole(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 441,
		Phone:      "+1777" + suffix + "37",
		FirstName:  "DeletedHoleOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 442,
		Phone:      "+1777" + suffix + "38",
		FirstName:  "DeletedHoleMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Deleted Hole " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000339,
	})
	if err != nil {
		t.Fatalf("create deleted-hole megagroup: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		MaxID:     created.Message.ID,
		Date:      1700000340,
	}); err != nil {
		t.Fatalf("read initial deleted-hole service message: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channels
SET participants_count = $2
WHERE id = $1`, channelID, domain.MaxSynchronousChannelDialogFanout+1); err != nil {
		t.Fatalf("mark deleted-hole megagroup over threshold: %v", err)
	}
	first, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9339,
		Message:   "deleted unread hole",
		Date:      1700000341,
	})
	if err != nil {
		t.Fatalf("send first large message: %v", err)
	}
	second, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  9340,
		Message:   "remaining unread message",
		Date:      1700000342,
	})
	if err != nil {
		t.Fatalf("send second large message: %v", err)
	}
	deleted, err := channels.DeleteChannelMessages(ctx, domain.DeleteChannelMessagesRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		IDs:       []int{first.Message.ID},
		Date:      1700000343,
	})
	if err != nil {
		t.Fatalf("delete non-top unread message: %v", err)
	}
	if len(deleted.DeletedIDs) != 1 || deleted.DeletedIDs[0] != first.Message.ID {
		t.Fatalf("deleted ids = %+v, want first message only", deleted.DeletedIDs)
	}

	list, err := channels.ListChannelDialogs(ctx, member.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list deleted-hole dialogs: %v", err)
	}
	if len(list.Dialogs) != 1 || list.Dialogs[0].TopMessage != second.Message.ID || list.Dialogs[0].UnreadCount != 1 {
		t.Fatalf("deleted-hole dialogs = %+v, want only non-deleted unread top counted", list.Dialogs)
	}
}

func TestChannelStoreListDialogsSeeksBeyondQueryWindow(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 36,
		Phone:      "+1777" + suffix + "04",
		FirstName:  "DialogSeekOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	count := channelDialogQueryLimit + 5
	ids := make([]int64, count)
	baseID := owner.ID * 1000
	for i := range ids {
		ids[i] = baseID + int64(i+1)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO channels (
    id, access_hash, creator_user_id, title, broadcast, megagroup,
    participants_count, admins_count, top_message_id, pts, date
)
SELECT id, id + 900000, $2, 'Bulk Dialog ' || ord, false, true, 1, 1, 1, 1, (1700000400 + ord)::int
FROM unnest($1::bigint[]) WITH ORDINALITY AS t(id, ord)`, ids, owner.ID); err != nil {
		t.Fatalf("bulk insert channels: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO channel_members (channel_id, user_id, role, status, joined_at)
SELECT id, $2, 'creator', 'active', 1700000400
FROM unnest($1::bigint[]) AS t(id)`, ids, owner.ID); err != nil {
		t.Fatalf("bulk insert channel members: %v", err)
	}

	channels := NewChannelStore(pool)
	var cursor domain.Dialog
	var sixth domain.ChannelDialogList
	for page := 0; page < 6; page++ {
		filter := domain.DialogFilter{Limit: 100}
		if page > 0 {
			filter.OffsetDate = cursor.TopMessageDate
			filter.OffsetID = cursor.TopMessage
			filter.HasOffsetPeer = true
			filter.OffsetPeer = cursor.Peer
		}
		got, err := channels.ListChannelDialogs(ctx, owner.ID, filter)
		if err != nil {
			t.Fatalf("list channel dialogs page %d: %v", page+1, err)
		}
		if len(got.Dialogs) == 0 {
			t.Fatalf("page %d unexpectedly empty after cursor %+v", page+1, cursor)
		}
		cursor = got.Dialogs[len(got.Dialogs)-1]
		if page == 5 {
			sixth = got
		}
	}
	if len(sixth.Dialogs) != 5 {
		t.Fatalf("sixth page len = %d, want remaining 5 beyond query window", len(sixth.Dialogs))
	}
	if sixth.Dialogs[0].Peer.ID != ids[4] || sixth.Dialogs[4].Peer.ID != ids[0] {
		t.Fatalf("sixth page dialogs = %+v, want oldest five descending by date", sixth.Dialogs)
	}

	included, err := channels.ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{
		Folder: &domain.DialogFolder{
			IncludePeers: []domain.DialogFolderPeer{{
				Peer: domain.Peer{Type: domain.PeerTypeChannel, ID: ids[0]},
			}},
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("list included channel dialog beyond query window: %v", err)
	}
	if len(included.Dialogs) != 1 || included.Dialogs[0].Peer.ID != ids[0] {
		t.Fatalf("included dialogs = %+v, want oldest included channel beyond query window", included.Dialogs)
	}
}

func TestChannelStoreListDialogsFolderFiltersBeforeQueryLimit(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 37,
		Phone:      "+1777" + suffix + "05",
		FirstName:  "DialogFolderOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	count := channelDialogQueryLimit + 5
	ids := make([]int64, count)
	baseID := owner.ID*1000 + 100000
	for i := range ids {
		ids[i] = baseID + int64(i+1)
	}
	archivedID := ids[0]
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO channels (
    id, access_hash, creator_user_id, title, broadcast, megagroup,
    participants_count, admins_count, top_message_id, pts, date
)
SELECT id, id + 910000, $2, 'Folder Dialog ' || ord, false, true, 1, 1, 1, 1, (1700000500 + ord)::int
FROM unnest($1::bigint[]) WITH ORDINALITY AS t(id, ord)`, ids, owner.ID); err != nil {
		t.Fatalf("bulk insert channels: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO channel_members (channel_id, user_id, role, status, joined_at)
SELECT id, $2, 'creator', 'active', 1700000500
FROM unnest($1::bigint[]) AS t(id)`, ids, owner.ID); err != nil {
		t.Fatalf("bulk insert channel members: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO channel_dialogs (user_id, channel_id, folder_id, top_message_id, top_message_date)
VALUES ($1, $2, $3, 1, 1700000500)`, owner.ID, archivedID, domain.DialogArchiveFolderID); err != nil {
		t.Fatalf("archive oldest channel dialog: %v", err)
	}

	archive, err := NewChannelStore(pool).ListChannelDialogs(ctx, owner.ID, domain.DialogFilter{
		HasFolderID: true,
		FolderID:    domain.DialogArchiveFolderID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list archive channel dialogs: %v", err)
	}
	if len(archive.Dialogs) != 1 || archive.Dialogs[0].Peer.ID != archivedID {
		t.Fatalf("archive dialogs = %+v, want archived channel beyond first query window", archive.Dialogs)
	}
}

func TestChannelStoreEditAboutPersistsAndChecksPermission(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 131,
		Phone:      "+1888" + suffix + "01",
		FirstName:  "AboutOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := users.Create(ctx, domain.User{
		AccessHash: 132,
		Phone:      "+1888" + suffix + "02",
		FirstName:  "AboutMember",
	})
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, member.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "About " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{member.ID},
		Date:          1700000600,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	if _, err := channels.EditChannelAbout(ctx, domain.EditChannelAboutRequest{
		UserID:    member.ID,
		ChannelID: channelID,
		About:     "member cannot edit",
		Date:      1700000601,
	}); !errors.Is(err, domain.ErrChannelAdminRequired) {
		t.Fatalf("EditChannelAbout by member err = %v, want ErrChannelAdminRequired", err)
	}

	updated, err := channels.EditChannelAbout(ctx, domain.EditChannelAboutRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		About:     "owner about",
		Date:      1700000602,
	})
	if err != nil {
		t.Fatalf("EditChannelAbout by owner: %v", err)
	}
	if updated.About != "owner about" {
		t.Fatalf("updated about = %q, want owner about", updated.About)
	}
	view, err := channels.GetChannel(ctx, member.ID, channelID)
	if err != nil {
		t.Fatalf("GetChannel by member: %v", err)
	}
	if view.Channel.About != "owner about" {
		t.Fatalf("member view about = %q, want owner about", view.Channel.About)
	}
}

func TestChannelStoreSendMessageResolvesReplyTopID(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1778" + suffix + "01",
		FirstName:  "ReplyOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1778" + suffix + "02",
		FirstName:  "ReplyFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Reply Top " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000350,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	root, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  911,
		Message:   "root",
		Date:      1700000351,
	})
	if err != nil {
		t.Fatalf("send root: %v", err)
	}
	reply, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		RandomID:  912,
		Message:   "reply",
		ReplyTo:   &domain.MessageReply{MessageID: root.Message.ID, QuoteText: "root"},
		Date:      1700000352,
	})
	if err != nil {
		t.Fatalf("send reply: %v", err)
	}
	channelPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	if reply.Message.ReplyTo == nil || reply.Message.ReplyTo.Peer != channelPeer || reply.Message.ReplyTo.TopMessageID != root.Message.ID {
		t.Fatalf("reply metadata = %+v, want channel peer and top id %d", reply.Message.ReplyTo, root.Message.ID)
	}
	nested, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  913,
		Message:   "nested",
		ReplyTo:   &domain.MessageReply{MessageID: reply.Message.ID},
		Date:      1700000353,
	})
	if err != nil {
		t.Fatalf("send nested reply: %v", err)
	}
	if nested.Message.ReplyTo == nil || nested.Message.ReplyTo.TopMessageID != root.Message.ID {
		t.Fatalf("nested reply metadata = %+v, want inherited top id %d", nested.Message.ReplyTo, root.Message.ID)
	}
	_, err = channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  914,
		Message:   "bad quote offset",
		ReplyTo: &domain.MessageReply{
			MessageID:   root.Message.ID,
			QuoteText:   "root",
			QuoteOffset: domain.MaxMessageReplyQuoteOffset + 1,
		},
		Date: 1700000354,
	})
	if !errors.Is(err, domain.ErrReplyMessageIDInvalid) {
		t.Fatalf("bad quote offset err = %v, want ErrReplyMessageIDInvalid", err)
	}
}

func TestChannelStoreHistorySupportsOffsetDateOnly(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 37,
		Phone:      "+1778" + suffix + "03",
		FirstName:  "HistoryDateOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "History Date " + suffix,
		Megagroup:     true,
		Date:          1700000360,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  921,
		Message:   "old",
		Date:      1700000361,
	}); err != nil {
		t.Fatalf("send old: %v", err)
	}
	if _, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  922,
		Message:   "new",
		Date:      1700000362,
	}); err != nil {
		t.Fatalf("send new: %v", err)
	}

	history, err := channels.ListChannelHistory(ctx, owner.ID, domain.ChannelHistoryFilter{
		ChannelID:  channelID,
		OffsetDate: 1700000362,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list channel history: %v", err)
	}
	if len(history.Messages) != 2 || history.Messages[0].Body != "old" || history.Messages[1].Action == nil {
		t.Fatalf("history = %+v, want only messages older than offset date", history.Messages)
	}
}

func TestChannelStoreDifferenceUsesDurableMessageSnapshots(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 39,
		Phone:      "+1778" + suffix + "01",
		FirstName:  "SnapshotOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 40,
		Phone:      "+1778" + suffix + "02",
		FirstName:  "SnapshotFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Snapshot Diff " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000380,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  941,
		Message:   "original",
		Date:      1700000381,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	if _, err := channels.EditChannelMessage(ctx, domain.EditChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		ID:        sent.Message.ID,
		Message:   "first edit",
		EditDate:  1700000382,
	}); err != nil {
		t.Fatalf("first edit: %v", err)
	}
	if _, err := channels.EditChannelMessage(ctx, domain.EditChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		ID:        sent.Message.ID,
		Message:   "second edit",
		EditDate:  1700000383,
	}); err != nil {
		t.Fatalf("second edit: %v", err)
	}
	duplicate, found, err := channels.duplicateChannelMessage(ctx, channelID, owner.ID, sent.Message.RandomID)
	if err != nil {
		t.Fatalf("duplicate channel message: %v", err)
	}
	if !found || !duplicate.Duplicate || duplicate.Event.Type != domain.ChannelUpdateNewMessage || duplicate.Message.Body != "original" || duplicate.Event.Message.Body != "original" {
		t.Fatalf("duplicate after edit = %+v found=%v, want original new-message snapshot", duplicate, found)
	}

	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		Pts:       created.Event.Pts,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list channel difference: %v", err)
	}
	if len(diff.NewMessages) != 1 || diff.NewMessages[0].Body != "original" {
		t.Fatalf("new messages = %+v, want original send snapshot", diff.NewMessages)
	}
	if len(diff.OtherUpdates) != 2 {
		t.Fatalf("other updates = %+v, want two edit snapshots", diff.OtherUpdates)
	}
	if diff.OtherUpdates[0].Message.Body != "first edit" || diff.OtherUpdates[1].Message.Body != "second edit" {
		t.Fatalf("edit snapshots = %q/%q, want first edit/second edit", diff.OtherUpdates[0].Message.Body, diff.OtherUpdates[1].Message.Body)
	}
}

func TestChannelStoreSendFailureBeforePtsAllocationDoesNotRecordNoopGap(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 41,
		Phone:      "+1888" + suffix + "01",
		FirstName:  "NoopOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	outsider, err := users.Create(ctx, domain.User{
		AccessHash: 42,
		Phone:      "+1888" + suffix + "02",
		FirstName:  "NoopOutsider",
	})
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, outsider.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Noop Gap " + suffix,
		Megagroup:     true,
		Date:          1700000400,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	_, err = channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    outsider.ID,
		ChannelID: channelID,
		RandomID:  991,
		Message:   "outsider should fail",
		Date:      1700000401,
	})
	if err == nil {
		t.Fatal("SendChannelMessage outsider unexpectedly succeeded")
	}

	var gapRows int
	if err := pool.QueryRow(ctx, `
SELECT count(*)::int
FROM channel_update_events
WHERE channel_id = $1 AND pts = 2`, channelID).Scan(&gapRows); err != nil {
		t.Fatalf("count events after failed send: %v", err)
	}
	if gapRows != 0 {
		t.Fatalf("events after failed send = %d, want no pts allocation before member validation", gapRows)
	}

	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  992,
		Message:   "after noop gap",
		Date:      1700000402,
	})
	if err != nil {
		t.Fatalf("send owner after gap: %v", err)
	}
	if sent.Event.Pts != 2 {
		t.Fatalf("next channel pts = %d, want 2 after failed send before pts allocation", sent.Event.Pts)
	}
	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Pts:       1,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list channel difference: %v", err)
	}
	if diff.Pts != 2 || len(diff.Events) != 1 || diff.Events[0].Type != domain.ChannelUpdateNewMessage || diff.Events[0].Pts != 2 {
		t.Fatalf("diff after failed send = %+v, want only message pts=2", diff)
	}
}

func TestChannelStoreDifferenceTooLongSnapshot(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 45,
		Phone:      "+1889" + suffix + "01",
		FirstName:  "TooLongOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 46,
		Phone:      "+1889" + suffix + "02",
		FirstName:  "TooLongFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "TooLong Snapshot " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000410,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	lastPts := created.Event.Pts
	for i := 0; i < 12; i++ {
		sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
			UserID:    owner.ID,
			ChannelID: channelID,
			RandomID:  int64(10_000 + i),
			Message:   "too long snapshot",
			Date:      1700000411 + i,
		})
		if err != nil {
			t.Fatalf("send channel message %d: %v", i, err)
		}
		lastPts = sent.Event.Pts
	}

	diff, err := channels.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		Pts:       0,
		Limit:     3,
	})
	if err != nil {
		t.Fatalf("list channel difference: %v", err)
	}
	if !diff.TooLong || !diff.Final || diff.Pts != lastPts {
		t.Fatalf("diff = %+v, want tooLong final snapshot at pts %d", diff, lastPts)
	}
	if len(diff.NewMessages) == 0 || len(diff.NewMessages) > domain.MaxChannelDifferenceTooLongMessages {
		t.Fatalf("tooLong snapshot messages = %d, want bounded latest messages", len(diff.NewMessages))
	}
	if diff.Dialog.TopMessageID == 0 || diff.Dialog.UnreadCount == 0 {
		t.Fatalf("tooLong dialog = %+v, want current dialog state", diff.Dialog)
	}
}

func TestChannelStoreAdminLogFiltersAndSearch(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 51,
		Phone:      "+1999" + suffix + "01",
		FirstName:  "AdminLogOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 52,
		Phone:      "+1999" + suffix + "02",
		FirstName:  "AdminLogFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	invited, err := users.Create(ctx, domain.User{
		AccessHash: 53,
		Phone:      "+1999" + suffix + "03",
		FirstName:  "AdminLogInvited",
	})
	if err != nil {
		t.Fatalf("create invited: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID, invited.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Admin Log " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000500,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	if _, err := channels.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		MemberID:  friend.ID,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo:  true,
			InviteUsers: true,
			PinMessages: true,
		},
		Rank: "ops",
		Date: 1700000501,
	}); err != nil {
		t.Fatalf("edit admin: %v", err)
	}
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  501,
		Message:   "needle admin log body",
		Date:      1700000502,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	if _, err := channels.UpdatePinnedMessage(ctx, domain.UpdateChannelPinnedMessageRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		MessageID: sent.Message.ID,
		Pinned:    true,
		Date:      1700000503,
	}); err != nil {
		t.Fatalf("pin message: %v", err)
	}
	if _, err := channels.InviteToChannel(ctx, channelID, friend.ID, []int64{invited.ID}, 1700000504); err != nil {
		t.Fatalf("invite to channel: %v", err)
	}

	searched, err := channels.ListAdminLog(ctx, domain.ChannelAdminLogRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Query:     "needle",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("search admin log: %v", err)
	}
	if len(searched.Events) == 0 {
		t.Fatalf("search admin log returned no events, want message body match")
	}

	pinned, err := channels.ListAdminLog(ctx, domain.ChannelAdminLogRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		Filter:    domain.ChannelAdminLogFilter{Pinned: true},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("pinned admin log: %v", err)
	}
	if len(pinned.Events) != 1 || pinned.Events[0].Type != domain.ChannelAdminLogUpdatePinned || pinned.Events[0].Message == nil {
		t.Fatalf("pinned events = %+v, want one update_pinned with message", pinned.Events)
	}

	byFriend, err := channels.ListAdminLog(ctx, domain.ChannelAdminLogRequest{
		UserID:       owner.ID,
		ChannelID:    channelID,
		AdminUserIDs: []int64{friend.ID},
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("friend admin log: %v", err)
	}
	if len(byFriend.Events) == 0 {
		t.Fatalf("friend admin log returned no events, want pin/invite")
	}
	for _, event := range byFriend.Events {
		if event.UserID != friend.ID {
			t.Fatalf("friend admin log event actor = %d, want %d in %+v", event.UserID, friend.ID, byFriend.Events)
		}
	}

	if _, err := channels.ListAdminLog(ctx, domain.ChannelAdminLogRequest{
		UserID:    invited.ID,
		ChannelID: channelID,
		Limit:     10,
	}); err != domain.ErrChannelAdminRequired {
		t.Fatalf("member admin log err = %v, want ErrChannelAdminRequired", err)
	}
}

func TestChannelStoreDeleteHistoryForEveryoneBatchesHugeMaxID(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 71,
		Phone:      "+1998" + suffix + "01",
		FirstName:  "BulkChannelOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 72,
		Phone:      "+1998" + suffix + "02",
		FirstName:  "BulkChannelFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Bulk Delete " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000600,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	total := domain.MaxDeleteHistoryBatch + 2
	if _, err := pool.Exec(ctx, `
WITH src AS (
  SELECT generate_series(2, $3::int + 1) AS id
),
msgs AS (
  INSERT INTO channel_messages (
    channel_id,
    id,
    random_id,
    sender_user_id,
    from_peer_type,
    from_peer_id,
    message_date,
    body,
    entities,
    pts
  )
  SELECT
    $1::bigint,
    id,
    920000000 + id,
    $2::bigint,
    'user',
    $2::bigint,
    1700000600 + id,
    'bulk channel history',
    '[]'::jsonb,
    id
  FROM src
  RETURNING id, message_date
)
INSERT INTO channel_update_events (
  channel_id,
  pts,
  pts_count,
  date,
  event_type,
  message_id,
  sender_user_id,
  payload
)
SELECT
  $1::bigint,
  id,
  1,
  message_date,
  'new_channel_message',
  id,
  $2::bigint,
  '{}'::jsonb
FROM msgs
`, channelID, owner.ID, total); err != nil {
		t.Fatalf("seed bulk channel messages: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE channels
SET top_message_id = $2,
    pts = $2,
    updated_at = now()
WHERE id = $1`, channelID, total+1); err != nil {
		t.Fatalf("update channel bulk top: %v", err)
	}

	first, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:      owner.ID,
		ChannelID:   channelID,
		MaxID:       int(^uint(0) >> 1),
		ForEveryone: true,
		Date:        1700000700,
	})
	if err != nil {
		t.Fatalf("DeleteChannelHistory first batch: %v", err)
	}
	wantFirstPts := total + 1 + domain.MaxDeleteHistoryBatch
	if first.Offset != 1 || first.Event.Pts != wantFirstPts || first.Event.PtsCount != domain.MaxDeleteHistoryBatch || len(first.DeletedIDs) != domain.MaxDeleteHistoryBatch {
		t.Fatalf("first batch = %+v, want offset=1 pts=%d pts_count=%d", first, wantFirstPts, domain.MaxDeleteHistoryBatch)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_messages WHERE channel_id = $1 AND NOT deleted`, channelID).Scan(&remaining); err != nil {
		t.Fatalf("count remaining after first batch: %v", err)
	}
	if remaining != 3 {
		t.Fatalf("remaining after first batch = %d, want create service + two oldest messages", remaining)
	}

	second, err := channels.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:      owner.ID,
		ChannelID:   channelID,
		MaxID:       int(^uint(0) >> 1),
		ForEveryone: true,
		Date:        1700000701,
	})
	if err != nil {
		t.Fatalf("DeleteChannelHistory second batch: %v", err)
	}
	if second.Offset != 0 || second.Event.Pts != wantFirstPts+3 || second.Event.PtsCount != 3 || len(second.DeletedIDs) != 3 {
		t.Fatalf("second batch = %+v, want final offset=0 pts=%d pts_count=3", second, wantFirstPts+3)
	}
}

func TestChannelStoreCommonChannelsOnlySharedMegagroups(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 51, Phone: "+1888" + suffix + "01", FirstName: "CommonOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{AccessHash: 52, Phone: "+1888" + suffix + "02", FirstName: "CommonFriend"})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	other, err := users.Create(ctx, domain.User{AccessHash: 53, Phone: "+1888" + suffix + "03", FirstName: "CommonOther"})
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID, other.ID})
	})

	channels := NewChannelStore(pool)
	create := func(title string, broadcast bool, memberIDs []int64, date int) domain.CreateChannelResult {
		t.Helper()
		created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
			CreatorUserID: owner.ID,
			Title:         title,
			Broadcast:     broadcast,
			Megagroup:     !broadcast,
			MemberUserIDs: memberIDs,
			Date:          date,
		})
		if err != nil {
			t.Fatalf("create %s: %v", title, err)
		}
		channelIDs = append(channelIDs, created.Channel.ID)
		return created
	}
	first := create("common one "+suffix, false, []int64{friend.ID}, 1700000800)
	second := create("common two "+suffix, false, []int64{friend.ID}, 1700000801)
	create("broadcast excluded "+suffix, true, []int64{friend.ID}, 1700000802)
	left := create("left excluded "+suffix, false, []int64{friend.ID}, 1700000803)
	if _, err := channels.LeaveChannel(ctx, left.Channel.ID, friend.ID, 1700000804); err != nil {
		t.Fatalf("leave channel: %v", err)
	}
	create("not shared "+suffix, false, []int64{other.ID}, 1700000805)

	page, err := channels.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       owner.ID,
		TargetUserID: friend.ID,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list common channels: %v", err)
	}
	if page.Count != 2 || len(page.Channels) != 2 || page.Channels[0].ID != first.Channel.ID || page.Channels[1].ID != second.Channel.ID {
		t.Fatalf("common channels = %+v, want two shared megagroups in id order", page)
	}

	next, err := channels.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       owner.ID,
		TargetUserID: friend.ID,
		MaxID:        first.Channel.ID,
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("list common channels after max id: %v", err)
	}
	if next.Count != 2 || len(next.Channels) != 1 || next.Channels[0].ID != second.Channel.ID {
		t.Fatalf("paged common channels = %+v, want second channel with full count", next)
	}

	countOnly, err := channels.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       owner.ID,
		TargetUserID: friend.ID,
		CountOnly:    true,
	})
	if err != nil {
		t.Fatalf("count common channels: %v", err)
	}
	if countOnly.Count != 2 || len(countOnly.Channels) != 0 {
		t.Fatalf("count-only common channels = %+v, want count without channels", countOnly)
	}
}

func TestChannelStoreLeftChannelsReturnsPagedLeftMemberships(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 61, Phone: "+1889" + suffix + "01", FirstName: "LeftOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{AccessHash: 62, Phone: "+1889" + suffix + "02", FirstName: "LeftFriend"})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	create := func(title string, broadcast bool, date int) domain.CreateChannelResult {
		t.Helper()
		created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
			CreatorUserID: owner.ID,
			Title:         title,
			Broadcast:     broadcast,
			Megagroup:     !broadcast,
			MemberUserIDs: []int64{friend.ID},
			Date:          date,
		})
		if err != nil {
			t.Fatalf("create %s: %v", title, err)
		}
		channelIDs = append(channelIDs, created.Channel.ID)
		return created
	}
	older := create("older left "+suffix, false, 1700000810)
	newer := create("newer left "+suffix, true, 1700000811)
	create("active excluded "+suffix, false, 1700000812)
	if _, err := channels.LeaveChannel(ctx, older.Channel.ID, friend.ID, 1700000813); err != nil {
		t.Fatalf("leave older channel: %v", err)
	}
	if _, err := channels.LeaveChannel(ctx, newer.Channel.ID, friend.ID, 1700000814); err != nil {
		t.Fatalf("leave newer channel: %v", err)
	}

	page, err := channels.ListLeftChannels(ctx, friend.ID, 0, 1)
	if err != nil {
		t.Fatalf("list left channels: %v", err)
	}
	if page.Count != 2 || len(page.Channels) != 1 || page.Channels[0].Channel.ID != newer.Channel.ID || page.Channels[0].Self.Status != domain.ChannelMemberLeft {
		t.Fatalf("first left page = %+v, want newest left channel and full count", page)
	}
	next, err := channels.ListLeftChannels(ctx, friend.ID, 1, 1)
	if err != nil {
		t.Fatalf("list next left channels: %v", err)
	}
	if next.Count != 2 || len(next.Channels) != 1 || next.Channels[0].Channel.ID != older.Channel.ID {
		t.Fatalf("second left page = %+v, want older left channel", next)
	}
	empty, err := channels.ListLeftChannels(ctx, friend.ID, 2, 1)
	if err != nil {
		t.Fatalf("list empty left page: %v", err)
	}
	if empty.Count != 2 || len(empty.Channels) != 0 {
		t.Fatalf("empty left page = %+v, want full count and no chats", empty)
	}
	if _, err := channels.ListLeftChannels(ctx, friend.ID, domain.MaxLeftChannelsOffset+1, 1); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("huge offset err = %v, want ErrChannelInvalid", err)
	}
}

func TestChannelStoreDiscussionGroupLinksAreBidirectional(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 71, Phone: "+1890" + suffix + "01", FirstName: "DiscussionOwner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	var channelIDs []int64
	t.Cleanup(func() {
		if len(channelIDs) != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = ANY($1::bigint[])", channelIDs)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	channels := NewChannelStore(pool)
	create := func(title string, broadcast bool, date int) domain.CreateChannelResult {
		t.Helper()
		created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
			CreatorUserID: owner.ID,
			Title:         title,
			Broadcast:     broadcast,
			Megagroup:     !broadcast,
			Date:          date,
		})
		if err != nil {
			t.Fatalf("create %s: %v", title, err)
		}
		channelIDs = append(channelIDs, created.Channel.ID)
		return created
	}
	broadcast := create("discussion broadcast "+suffix, true, 1700000820)
	firstGroup := create("discussion first "+suffix, false, 1700000821)
	secondGroup := create("discussion second "+suffix, false, 1700000822)

	candidates, err := channels.ListDiscussionGroups(ctx, owner.ID, 10)
	if err != nil {
		t.Fatalf("list discussion groups: %v", err)
	}
	if len(candidates) < 2 || candidates[0].ID != secondGroup.Channel.ID || candidates[1].ID != firstGroup.Channel.ID {
		t.Fatalf("discussion groups = %+v, want newest creator megagroups", candidates)
	}

	linked, err := channels.SetDiscussionGroup(ctx, owner.ID, broadcast.Channel.ID, firstGroup.Channel.ID)
	if err != nil {
		t.Fatalf("link first discussion group: %v", err)
	}
	if len(linked.Channels) != 2 {
		t.Fatalf("linked changed channels = %+v, want broadcast and group", linked.Channels)
	}
	gotBroadcast, err := channels.GetChannelByID(ctx, broadcast.Channel.ID)
	if err != nil {
		t.Fatalf("get linked broadcast: %v", err)
	}
	gotFirst, err := channels.GetChannelByID(ctx, firstGroup.Channel.ID)
	if err != nil {
		t.Fatalf("get linked first group: %v", err)
	}
	if gotBroadcast.LinkedChatID != firstGroup.Channel.ID || gotFirst.LinkedChatID != broadcast.Channel.ID {
		t.Fatalf("first link = broadcast %d group %d, want bidirectional", gotBroadcast.LinkedChatID, gotFirst.LinkedChatID)
	}

	replaced, err := channels.SetDiscussionGroup(ctx, owner.ID, broadcast.Channel.ID, secondGroup.Channel.ID)
	if err != nil {
		t.Fatalf("replace discussion group: %v", err)
	}
	if len(replaced.Channels) != 3 {
		t.Fatalf("replace changed channels = %+v, want broadcast old group new group", replaced.Channels)
	}
	gotBroadcast, _ = channels.GetChannelByID(ctx, broadcast.Channel.ID)
	gotFirst, _ = channels.GetChannelByID(ctx, firstGroup.Channel.ID)
	gotSecond, err := channels.GetChannelByID(ctx, secondGroup.Channel.ID)
	if err != nil {
		t.Fatalf("get linked second group: %v", err)
	}
	if gotBroadcast.LinkedChatID != secondGroup.Channel.ID || gotSecond.LinkedChatID != broadcast.Channel.ID || gotFirst.LinkedChatID != 0 {
		t.Fatalf("replace link = broadcast %d first %d second %d, want old cleared and new bidirectional",
			gotBroadcast.LinkedChatID, gotFirst.LinkedChatID, gotSecond.LinkedChatID)
	}

	if _, err := channels.SetDiscussionGroup(ctx, owner.ID, 0, secondGroup.Channel.ID); err != nil {
		t.Fatalf("unlink from group side: %v", err)
	}
	gotBroadcast, _ = channels.GetChannelByID(ctx, broadcast.Channel.ID)
	gotSecond, _ = channels.GetChannelByID(ctx, secondGroup.Channel.ID)
	if gotBroadcast.LinkedChatID != 0 || gotSecond.LinkedChatID != 0 {
		t.Fatalf("unlink = broadcast %d group %d, want both cleared", gotBroadcast.LinkedChatID, gotSecond.LinkedChatID)
	}
	if _, err := channels.SetDiscussionGroup(ctx, owner.ID, 0, secondGroup.Channel.ID); !errors.Is(err, domain.ErrLinkNotModified) {
		t.Fatalf("repeat unlink err = %v, want ErrLinkNotModified", err)
	}
	if _, err := channels.SetPreHistoryHidden(ctx, owner.ID, firstGroup.Channel.ID, true); err != nil {
		t.Fatalf("hide first group prehistory: %v", err)
	}
	if _, err := channels.SetDiscussionGroup(ctx, owner.ID, broadcast.Channel.ID, firstGroup.Channel.ID); !errors.Is(err, domain.ErrMegagroupPrehistoryHidden) {
		t.Fatalf("hidden prehistory err = %v, want ErrMegagroupPrehistoryHidden", err)
	}
}

func TestChannelStoreReadMessageContentsClearsVisibleUnreadReactions(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 81,
		Phone:      "+1891" + suffix + "01",
		FirstName:  "ReactionOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{
		AccessHash: 82,
		Phone:      "+1891" + suffix + "02",
		FirstName:  "ReactionFriend",
	})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{owner.ID, friend.ID})
	})

	channels := NewChannelStore(pool)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "Visible Reaction " + suffix,
		Megagroup:     true,
		MemberUserIDs: []int64{friend.ID},
		Date:          1700000900,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID
	sent, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		RandomID:  90_001,
		Message:   "react to this",
		Date:      1700000901,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	if _, err := channels.SetChannelMessageReactions(ctx, domain.SetChannelMessageReactionsRequest{
		UserID:    friend.ID,
		ChannelID: channelID,
		MessageID: sent.Message.ID,
		Reactions: []domain.MessageReaction{{
			Type:     domain.MessageReactionEmoji,
			Emoticon: "\U0001f525",
		}},
		Date: 1700000902,
	}); err != nil {
		t.Fatalf("set channel reaction: %v", err)
	}
	dialogs, err := channels.GetChannelDialogs(ctx, owner.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get owner dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].UnreadReactions != 1 {
		t.Fatalf("owner dialogs = %+v, want one unread reaction", dialogs.Dialogs)
	}
	unread, err := channels.ListChannelUnreadReactions(ctx, owner.ID, domain.ChannelUnreadReactionsFilter{
		ChannelID: channelID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list unread reactions: %v", err)
	}
	if len(unread.Messages) != 1 || unread.Messages[0].ID != sent.Message.ID || unread.Messages[0].Reactions == nil || !hasUnreadChannelReactionPG(*unread.Messages[0].Reactions) {
		t.Fatalf("unread reactions = %+v, want unread sent message", unread.Messages)
	}

	read, err := channels.ReadChannelMessageContents(ctx, domain.ReadChannelMessageContentsRequest{
		UserID:    owner.ID,
		ChannelID: channelID,
		IDs:       []int{sent.Message.ID},
	})
	if err != nil {
		t.Fatalf("read channel message contents: %v", err)
	}
	if !reflect.DeepEqual(read.ClearedUnreadReactionMessageIDs, []int{sent.Message.ID}) {
		t.Fatalf("cleared reaction ids = %+v, want [%d]", read.ClearedUnreadReactionMessageIDs, sent.Message.ID)
	}
	if len(read.Messages) != 1 || read.Messages[0].Reactions == nil || hasUnreadChannelReactionPG(*read.Messages[0].Reactions) {
		t.Fatalf("read messages = %+v, want returned reaction marked read", read.Messages)
	}
	unreadAfter, err := channels.ListChannelUnreadReactions(ctx, owner.ID, domain.ChannelUnreadReactionsFilter{
		ChannelID: channelID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list unread reactions after read contents: %v", err)
	}
	if len(unreadAfter.Messages) != 0 {
		t.Fatalf("unread reactions after read contents = %+v, want empty", unreadAfter.Messages)
	}
	dialogsAfter, err := channels.GetChannelDialogs(ctx, owner.ID, []int64{channelID})
	if err != nil {
		t.Fatalf("get owner dialogs after read contents: %v", err)
	}
	if len(dialogsAfter.Dialogs) != 1 || dialogsAfter.Dialogs[0].UnreadReactions != 0 {
		t.Fatalf("owner dialogs after read contents = %+v, want unread reactions 0", dialogsAfter.Dialogs)
	}
	var stillUnread bool
	if err := pool.QueryRow(ctx, `
SELECT unread
FROM channel_message_reactions
WHERE channel_id = $1 AND message_id = $2 AND reacted_user_id = $3`,
		channelID, sent.Message.ID, friend.ID).Scan(&stillUnread); err != nil {
		t.Fatalf("read reaction row: %v", err)
	}
	if stillUnread {
		t.Fatal("reaction row still unread after read contents")
	}
}

func hasUnreadChannelReactionPG(reactions domain.ChannelMessageReactions) bool {
	for _, recent := range reactions.Recent {
		if recent.Unread {
			return true
		}
	}
	return false
}
