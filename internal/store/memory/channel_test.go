package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"telesrv/internal/domain"
)

func TestChannelRealtimeRecipientsAreCapped(t *testing.T) {
	store := NewChannelStore()
	memberIDs := make([]int64, domain.MaxChannelRealtimeFanout+25)
	for i := range memberIDs {
		memberIDs[i] = int64(10_000 + i)
	}

	created, err := store.CreateChannel(context.Background(), domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "large realtime cap",
		Megagroup:     true,
		MemberUserIDs: memberIDs,
		Date:          1_700_000_100,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if got := len(created.Recipients); got != domain.MaxChannelRealtimeFanout {
		t.Fatalf("create recipients = %d, want capped %d", got, domain.MaxChannelRealtimeFanout)
	}

	recipients, err := store.ListActiveChannelMemberIDs(context.Background(), 1, created.Channel.ID, 0)
	if err != nil {
		t.Fatalf("list active members: %v", err)
	}
	if got := len(recipients); got != domain.MaxChannelRealtimeFanout {
		t.Fatalf("listed active members = %d, want capped %d", got, domain.MaxChannelRealtimeFanout)
	}
}

func TestChannelAdminAndBanDoNotAdvanceChannelPts(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "participant state no pts",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_120,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID := created.Channel.ID
	ptsFloor := created.Channel.Pts

	promoted, err := store.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    1,
		ChannelID: channelID,
		MemberID:  2,
		AdminRights: domain.ChannelAdminRights{
			InviteUsers: true,
		},
		Date: 1_700_000_121,
	})
	if err != nil {
		t.Fatalf("edit admin: %v", err)
	}
	if promoted.Event.Pts != 0 || promoted.Event.PtsCount != 0 || promoted.Channel.Pts != ptsFloor {
		t.Fatalf("edit admin pts = event(%d,%d) channel %d, want unchanged %d", promoted.Event.Pts, promoted.Event.PtsCount, promoted.Channel.Pts, ptsFloor)
	}

	banned, err := store.EditChannelBanned(ctx, domain.EditChannelBannedRequest{
		UserID:      1,
		ChannelID:   channelID,
		Participant: domain.Peer{Type: domain.PeerTypeUser, ID: 2},
		BannedRights: domain.ChannelBannedRights{
			ViewMessages: true,
			UntilDate:    1_700_001_121,
		},
		Date: 1_700_000_122,
	})
	if err != nil {
		t.Fatalf("edit banned: %v", err)
	}
	if banned.Event.Pts != 0 || banned.Event.PtsCount != 0 || banned.Channel.Pts != ptsFloor {
		t.Fatalf("edit banned pts = event(%d,%d) channel %d, want unchanged %d", banned.Event.Pts, banned.Event.PtsCount, banned.Channel.Pts, ptsFloor)
	}

	diff, err := store.ListChannelDifference(ctx, domain.ChannelDifferenceRequest{
		UserID:    1,
		ChannelID: channelID,
		Pts:       ptsFloor,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list difference: %v", err)
	}
	if len(diff.Events) != 0 || diff.Pts != ptsFloor {
		t.Fatalf("difference after participant state change = %+v, want no durable events at pts %d", diff, ptsFloor)
	}
}

func TestPendingJoinRequestsSummaryAndInviteAdmins(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "pending join requests",
		Megagroup:     true,
		MemberUserIDs: []int64{2, 3, 4},
		Date:          1_700_000_150,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID := created.Channel.ID
	if _, err := store.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    1,
		ChannelID: channelID,
		MemberID:  2,
		AdminRights: domain.ChannelAdminRights{
			InviteUsers: true,
		},
		Date: 1_700_000_151,
	}); err != nil {
		t.Fatalf("promote invite admin: %v", err)
	}
	if _, err := store.EditChannelAdmin(ctx, domain.EditChannelAdminRequest{
		UserID:    1,
		ChannelID: channelID,
		MemberID:  4,
		AdminRights: domain.ChannelAdminRights{
			ChangeInfo: true,
		},
		Date: 1_700_000_152,
	}); err != nil {
		t.Fatalf("promote change-info admin: %v", err)
	}
	invite, err := store.ExportInvite(ctx, domain.ExportChannelInviteRequest{
		UserID:        1,
		ChannelID:     channelID,
		Title:         "approval",
		RequestNeeded: true,
		Date:          1_700_000_153,
	})
	if err != nil {
		t.Fatalf("export invite: %v", err)
	}
	for i := 0; i < domain.MaxChannelPendingJoinRecentRequesters+2; i++ {
		_, err := store.ImportInvite(ctx, domain.ImportChannelInviteRequest{
			UserID: int64(10 + i),
			Hash:   invite.Invite.Hash,
			Date:   1_700_000_160 + i,
		})
		if !errors.Is(err, domain.ErrInviteRequestSent) {
			t.Fatalf("import pending %d err = %v, want ErrInviteRequestSent", i, err)
		}
	}
	pending, err := store.PendingJoinRequests(ctx, channelID, 99)
	if err != nil {
		t.Fatalf("pending join requests: %v", err)
	}
	if pending.Count != domain.MaxChannelPendingJoinRecentRequesters+2 || len(pending.RecentRequesters) != domain.MaxChannelPendingJoinRecentRequesters {
		t.Fatalf("pending summary = %+v, want bounded recent with full count", pending)
	}
	if pending.RecentRequesters[0] != 16 || pending.RecentRequesters[len(pending.RecentRequesters)-1] != 12 {
		t.Fatalf("recent requesters = %+v, want newest first", pending.RecentRequesters)
	}
	admins, err := store.ListChannelInviteAdminMemberIDs(ctx, channelID, 0)
	if err != nil {
		t.Fatalf("invite admins: %v", err)
	}
	want := []int64{1, 2, 4}
	if !reflect.DeepEqual(admins, want) {
		t.Fatalf("invite admins = %+v, want %+v", admins, want)
	}
}

func TestCommonChannelsOnlySharedMegagroups(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	first, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "common one",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_170,
	})
	if err != nil {
		t.Fatalf("create first common channel: %v", err)
	}
	second, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "common two",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_171,
	})
	if err != nil {
		t.Fatalf("create second common channel: %v", err)
	}
	if _, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "broadcast excluded",
		Broadcast:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_172,
	}); err != nil {
		t.Fatalf("create broadcast channel: %v", err)
	}
	left, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "left excluded",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_173,
	})
	if err != nil {
		t.Fatalf("create left channel: %v", err)
	}
	if _, err := store.LeaveChannel(ctx, left.Channel.ID, 2, 1_700_000_174); err != nil {
		t.Fatalf("leave channel: %v", err)
	}
	if _, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "not shared",
		Megagroup:     true,
		MemberUserIDs: []int64{3},
		Date:          1_700_000_175,
	}); err != nil {
		t.Fatalf("create non-shared channel: %v", err)
	}

	page, err := store.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       1,
		TargetUserID: 2,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list common channels: %v", err)
	}
	if page.Count != 2 || len(page.Channels) != 2 || page.Channels[0].ID != first.Channel.ID || page.Channels[1].ID != second.Channel.ID {
		t.Fatalf("common channels = %+v, want two shared megagroups in id order", page)
	}

	next, err := store.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       1,
		TargetUserID: 2,
		MaxID:        first.Channel.ID,
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("list common channels after max id: %v", err)
	}
	if next.Count != 2 || len(next.Channels) != 1 || next.Channels[0].ID != second.Channel.ID {
		t.Fatalf("paged common channels = %+v, want second channel with full count", next)
	}

	countOnly, err := store.ListCommonChannels(ctx, domain.CommonChannelsRequest{
		UserID:       1,
		TargetUserID: 2,
		CountOnly:    true,
	})
	if err != nil {
		t.Fatalf("count common channels: %v", err)
	}
	if countOnly.Count != 2 || len(countOnly.Channels) != 0 {
		t.Fatalf("count-only common channels = %+v, want count without channels", countOnly)
	}
}

func TestLeftChannelsReturnsPagedLeftMemberships(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	older, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "older left",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_180,
	})
	if err != nil {
		t.Fatalf("create older channel: %v", err)
	}
	newer, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "newer left broadcast",
		Broadcast:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_181,
	})
	if err != nil {
		t.Fatalf("create newer channel: %v", err)
	}
	if _, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "active excluded",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_182,
	}); err != nil {
		t.Fatalf("create active channel: %v", err)
	}
	if _, err := store.LeaveChannel(ctx, older.Channel.ID, 2, 1_700_000_183); err != nil {
		t.Fatalf("leave older channel: %v", err)
	}
	if _, err := store.LeaveChannel(ctx, newer.Channel.ID, 2, 1_700_000_184); err != nil {
		t.Fatalf("leave newer channel: %v", err)
	}

	page, err := store.ListLeftChannels(ctx, 2, 0, 1)
	if err != nil {
		t.Fatalf("list left channels: %v", err)
	}
	if page.Count != 2 || len(page.Channels) != 1 || page.Channels[0].Channel.ID != newer.Channel.ID || page.Channels[0].Self.Status != domain.ChannelMemberLeft {
		t.Fatalf("first left page = %+v, want newest left channel and full count", page)
	}
	next, err := store.ListLeftChannels(ctx, 2, 1, 1)
	if err != nil {
		t.Fatalf("list next left channels: %v", err)
	}
	if next.Count != 2 || len(next.Channels) != 1 || next.Channels[0].Channel.ID != older.Channel.ID {
		t.Fatalf("second left page = %+v, want older left channel", next)
	}
	empty, err := store.ListLeftChannels(ctx, 2, 2, 1)
	if err != nil {
		t.Fatalf("list empty left page: %v", err)
	}
	if empty.Count != 2 || len(empty.Channels) != 0 {
		t.Fatalf("empty left page = %+v, want full count and no chats", empty)
	}
	if _, err := store.ListLeftChannels(ctx, 2, domain.MaxLeftChannelsOffset+1, 1); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("huge offset err = %v, want ErrChannelInvalid", err)
	}
}

func TestDiscussionGroupLinksAreBidirectionalAndReplaceOldLinks(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	broadcast, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "broadcast",
		Broadcast:     true,
		Date:          1_700_000_190,
	})
	if err != nil {
		t.Fatalf("create broadcast: %v", err)
	}
	firstGroup, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "first group",
		Megagroup:     true,
		Date:          1_700_000_191,
	})
	if err != nil {
		t.Fatalf("create first group: %v", err)
	}
	secondGroup, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "second group",
		Megagroup:     true,
		Date:          1_700_000_192,
	})
	if err != nil {
		t.Fatalf("create second group: %v", err)
	}
	if _, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "broadcast excluded",
		Broadcast:     true,
		Date:          1_700_000_193,
	}); err != nil {
		t.Fatalf("create excluded broadcast: %v", err)
	}

	candidates, err := store.ListDiscussionGroups(ctx, 1, 10)
	if err != nil {
		t.Fatalf("list discussion groups: %v", err)
	}
	if len(candidates) != 2 || candidates[0].ID != secondGroup.Channel.ID || candidates[1].ID != firstGroup.Channel.ID {
		t.Fatalf("discussion candidates = %+v, want creator megagroups newest id first", candidates)
	}

	linked, err := store.SetDiscussionGroup(ctx, 1, broadcast.Channel.ID, firstGroup.Channel.ID)
	if err != nil {
		t.Fatalf("link first group: %v", err)
	}
	if len(linked.Channels) != 2 {
		t.Fatalf("linked changed channels = %+v, want broadcast and group", linked.Channels)
	}
	gotBroadcast, err := store.GetChannelByID(ctx, broadcast.Channel.ID)
	if err != nil {
		t.Fatalf("get linked broadcast: %v", err)
	}
	gotFirst, err := store.GetChannelByID(ctx, firstGroup.Channel.ID)
	if err != nil {
		t.Fatalf("get linked first group: %v", err)
	}
	if gotBroadcast.LinkedChatID != firstGroup.Channel.ID || gotFirst.LinkedChatID != broadcast.Channel.ID {
		t.Fatalf("first link = broadcast %+v group %+v, want bidirectional ids", gotBroadcast, gotFirst)
	}

	replaced, err := store.SetDiscussionGroup(ctx, 1, broadcast.Channel.ID, secondGroup.Channel.ID)
	if err != nil {
		t.Fatalf("replace discussion group: %v", err)
	}
	if len(replaced.Channels) != 3 {
		t.Fatalf("replace changed channels = %+v, want broadcast, old group, new group", replaced.Channels)
	}
	gotBroadcast, _ = store.GetChannelByID(ctx, broadcast.Channel.ID)
	gotFirst, _ = store.GetChannelByID(ctx, firstGroup.Channel.ID)
	gotSecond, err := store.GetChannelByID(ctx, secondGroup.Channel.ID)
	if err != nil {
		t.Fatalf("get linked second group: %v", err)
	}
	if gotBroadcast.LinkedChatID != secondGroup.Channel.ID || gotSecond.LinkedChatID != broadcast.Channel.ID || gotFirst.LinkedChatID != 0 {
		t.Fatalf("replace link = broadcast %d first %d second %d, want old cleared and new bidirectional",
			gotBroadcast.LinkedChatID, gotFirst.LinkedChatID, gotSecond.LinkedChatID)
	}

	unlinked, err := store.SetDiscussionGroup(ctx, 1, 0, secondGroup.Channel.ID)
	if err != nil {
		t.Fatalf("unlink from group side: %v", err)
	}
	if len(unlinked.Channels) != 2 {
		t.Fatalf("unlink changed channels = %+v, want broadcast and group", unlinked.Channels)
	}
	gotBroadcast, _ = store.GetChannelByID(ctx, broadcast.Channel.ID)
	gotSecond, _ = store.GetChannelByID(ctx, secondGroup.Channel.ID)
	if gotBroadcast.LinkedChatID != 0 || gotSecond.LinkedChatID != 0 {
		t.Fatalf("unlink = broadcast %d second %d, want both cleared", gotBroadcast.LinkedChatID, gotSecond.LinkedChatID)
	}
	if _, err := store.SetDiscussionGroup(ctx, 1, 0, secondGroup.Channel.ID); !errors.Is(err, domain.ErrLinkNotModified) {
		t.Fatalf("repeat unlink err = %v, want ErrLinkNotModified", err)
	}
	if _, err := store.SetPreHistoryHidden(ctx, 1, firstGroup.Channel.ID, true); err != nil {
		t.Fatalf("hide first group prehistory: %v", err)
	}
	if _, err := store.SetDiscussionGroup(ctx, 1, broadcast.Channel.ID, firstGroup.Channel.ID); !errors.Is(err, domain.ErrMegagroupPrehistoryHidden) {
		t.Fatalf("hidden group link err = %v, want ErrMegagroupPrehistoryHidden", err)
	}
}

func TestChannelDeleteHistoryCapsHugeMaxID(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "bounded delete history",
		Megagroup:     true,
		Date:          1_700_000_200,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	totalMessages := domain.MaxDeleteHistoryBatch + 2
	for i := 0; i < totalMessages; i++ {
		if _, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
			UserID:    1,
			ChannelID: created.Channel.ID,
			RandomID:  int64(10_000 + i),
			Message:   "bulk",
			Date:      1_700_000_201 + i,
		}); err != nil {
			t.Fatalf("send channel message %d: %v", i, err)
		}
	}

	first, err := store.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:      1,
		ChannelID:   created.Channel.ID,
		MaxID:       int(^uint(0) >> 1),
		ForEveryone: true,
		Date:        1_700_001_300,
	})
	if err != nil {
		t.Fatalf("delete first batch: %v", err)
	}
	if first.Offset != 1 || len(first.DeletedIDs) != domain.MaxDeleteHistoryBatch || first.Event.PtsCount != domain.MaxDeleteHistoryBatch {
		t.Fatalf("first batch = %+v, want capped page with offset", first)
	}

	second, err := store.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:      1,
		ChannelID:   created.Channel.ID,
		MaxID:       int(^uint(0) >> 1),
		ForEveryone: true,
		Date:        1_700_001_301,
	})
	if err != nil {
		t.Fatalf("delete second batch: %v", err)
	}
	if second.Offset != 0 || len(second.DeletedIDs) != 3 || second.Event.PtsCount != 3 {
		t.Fatalf("second batch = %+v, want final bounded page", second)
	}
}

func TestChannelDeleteHistoryLocalClearReturnsMonotonicAvailableMinID(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "monotonic local clear",
		Megagroup:     true,
		Date:          1_700_000_250,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	first, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		RandomID:  30_001,
		Message:   "first visible",
		Date:      1_700_000_251,
	})
	if err != nil {
		t.Fatalf("send first message: %v", err)
	}
	second, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		RandomID:  30_002,
		Message:   "second visible",
		Date:      1_700_000_252,
	})
	if err != nil {
		t.Fatalf("send second message: %v", err)
	}

	high, err := store.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		MaxID:     second.Message.ID,
		Date:      1_700_000_253,
	})
	if err != nil {
		t.Fatalf("clear high watermark: %v", err)
	}
	if high.AvailableMinID != second.Message.ID {
		t.Fatalf("high available_min_id = %d, want %d", high.AvailableMinID, second.Message.ID)
	}

	stale, err := store.DeleteChannelHistory(ctx, domain.DeleteChannelHistoryRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		MaxID:     first.Message.ID,
		Date:      1_700_000_254,
	})
	if err != nil {
		t.Fatalf("clear stale low watermark: %v", err)
	}
	if stale.AvailableMinID != second.Message.ID {
		t.Fatalf("stale available_min_id = %d, want monotonic %d", stale.AvailableMinID, second.Message.ID)
	}

	history, err := store.ListChannelHistory(ctx, 1, domain.ChannelHistoryFilter{ChannelID: created.Channel.ID, Limit: 10})
	if err != nil {
		t.Fatalf("list history: %v", err)
	}
	if len(history.Messages) != 0 {
		t.Fatalf("history after stale clear = %+v, want no visible messages", history.Messages)
	}
	dialogs, err := store.GetChannelDialogs(ctx, 1, []int64{created.Channel.ID})
	if err != nil {
		t.Fatalf("get channel dialog: %v", err)
	}
	if len(dialogs.Dialogs) != 1 {
		t.Fatalf("dialogs = %+v, want one dialog", dialogs.Dialogs)
	}
	if dialogs.Dialogs[0].TopMessage != 0 || dialogs.Dialogs[0].ReadInboxMaxID != second.Message.ID || dialogs.Dialogs[0].UnreadCount != 0 {
		t.Fatalf("dialog after stale clear = %+v, want top=0 read=%d unread=0", dialogs.Dialogs[0], second.Message.ID)
	}
}

func TestChannelListDialogsDerivesRecipientTopWithoutWriteFanout(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "single copy dialog top",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_300,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := store.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    2,
		ChannelID: created.Channel.ID,
		MaxID:     created.Message.ID,
		Date:      1_700_000_301,
	}); err != nil {
		t.Fatalf("read initial service message: %v", err)
	}

	sent, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		RandomID:  88,
		Message:   "visible without write fanout",
		Date:      1_700_000_302,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	list, err := store.ListChannelDialogs(ctx, 2, domain.DialogFilter{Limit: 10})
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

func TestChannelUnreadExcludesOwnOutgoing(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "own outgoing unread",
		Megagroup:     true,
		Date:          1_700_000_360,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	sent, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		RandomID:  36_001,
		Message:   "own outgoing only",
		Date:      1_700_000_361,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}

	store.mu.Lock()
	member := store.members[created.Channel.ID][1]
	member.ReadInboxMaxID = sent.Message.ID - 1
	store.members[created.Channel.ID][1] = member
	dialog := store.dialogs[1][created.Channel.ID]
	dialog.ReadInboxMaxID = sent.Message.ID - 1
	dialog.UnreadCount = 99
	store.dialogs[1][created.Channel.ID] = dialog
	store.mu.Unlock()

	dialogs, err := store.GetChannelDialogs(ctx, 1, []int64{created.Channel.ID})
	if err != nil {
		t.Fatalf("get channel dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].UnreadCount != 0 {
		t.Fatalf("dialogs = %+v, want own outgoing excluded from unread", dialogs.Dialogs)
	}
	read, err := store.ReadChannelHistory(ctx, domain.ReadChannelHistoryRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		MaxID:     sent.Message.ID,
		Date:      1_700_000_362,
	})
	if err != nil {
		t.Fatalf("read channel history: %v", err)
	}
	if read.StillUnreadCount != 0 || read.Dialog.UnreadCount != 0 {
		t.Fatalf("read result = %+v, want no own-outgoing unread", read)
	}
}

func TestChannelReadMessageContentsClearsVisibleUnreadReactions(t *testing.T) {
	ctx := context.Background()
	store := NewChannelStore()
	created, err := store.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 1,
		Title:         "visible unread reaction",
		Megagroup:     true,
		MemberUserIDs: []int64{2},
		Date:          1_700_000_400,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	sent, err := store.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		RandomID:  40_001,
		Message:   "react to this",
		Date:      1_700_000_401,
	})
	if err != nil {
		t.Fatalf("send channel message: %v", err)
	}
	if _, err := store.SetChannelMessageReactions(ctx, domain.SetChannelMessageReactionsRequest{
		UserID:    2,
		ChannelID: created.Channel.ID,
		MessageID: sent.Message.ID,
		Reactions: []domain.MessageReaction{{
			Type:     domain.MessageReactionEmoji,
			Emoticon: "\U0001f525",
		}},
		Date: 1_700_000_402,
	}); err != nil {
		t.Fatalf("set channel reaction: %v", err)
	}
	dialogs, err := store.GetChannelDialogs(ctx, 1, []int64{created.Channel.ID})
	if err != nil {
		t.Fatalf("get owner channel dialogs: %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].UnreadReactions != 1 {
		t.Fatalf("owner dialogs = %+v, want one unread reaction", dialogs.Dialogs)
	}
	unread, err := store.ListChannelUnreadReactions(ctx, 1, domain.ChannelUnreadReactionsFilter{
		ChannelID: created.Channel.ID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list unread reactions: %v", err)
	}
	if len(unread.Messages) != 1 || unread.Messages[0].ID != sent.Message.ID {
		t.Fatalf("unread reactions = %+v, want sent message", unread.Messages)
	}
	if unread.Messages[0].Reactions == nil || !hasUnreadChannelReaction(*unread.Messages[0].Reactions) {
		t.Fatalf("unread message reactions = %+v, want unread recent reaction", unread.Messages[0].Reactions)
	}

	read, err := store.ReadChannelMessageContents(ctx, domain.ReadChannelMessageContentsRequest{
		UserID:    1,
		ChannelID: created.Channel.ID,
		IDs:       []int{sent.Message.ID},
	})
	if err != nil {
		t.Fatalf("read channel message contents: %v", err)
	}
	if !reflect.DeepEqual(read.ClearedUnreadReactionMessageIDs, []int{sent.Message.ID}) {
		t.Fatalf("cleared reaction ids = %+v, want [%d]", read.ClearedUnreadReactionMessageIDs, sent.Message.ID)
	}
	if len(read.Messages) != 1 || read.Messages[0].Reactions == nil || hasUnreadChannelReaction(*read.Messages[0].Reactions) {
		t.Fatalf("read messages = %+v, want reaction returned as read", read.Messages)
	}
	unreadAfter, err := store.ListChannelUnreadReactions(ctx, 1, domain.ChannelUnreadReactionsFilter{
		ChannelID: created.Channel.ID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list unread reactions after read contents: %v", err)
	}
	if len(unreadAfter.Messages) != 0 {
		t.Fatalf("unread reactions after read contents = %+v, want empty", unreadAfter.Messages)
	}
	dialogsAfter, err := store.GetChannelDialogs(ctx, 1, []int64{created.Channel.ID})
	if err != nil {
		t.Fatalf("get dialogs after read contents: %v", err)
	}
	if len(dialogsAfter.Dialogs) != 1 || dialogsAfter.Dialogs[0].UnreadReactions != 0 {
		t.Fatalf("dialogs after read contents = %+v, want unread reactions 0", dialogsAfter.Dialogs)
	}
}

func hasUnreadChannelReaction(reactions domain.ChannelMessageReactions) bool {
	for _, recent := range reactions.Recent {
		if recent.Unread {
			return true
		}
	}
	return false
}
