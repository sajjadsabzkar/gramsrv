package dialogs

import (
	"context"
	"errors"
	"testing"

	appchannels "telesrv/internal/app/channels"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestGetDialogsIncludesChannelReadOutboxAfterOfflineRead(t *testing.T) {
	ctx := context.Background()
	channelStore := memory.NewChannelStore()
	channels := appchannels.NewService(channelStore)
	dialogs := NewService(nil, channelStore)

	created, err := channels.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title:         "Offline Read",
		MemberUserIDs: []int64{1002},
		Date:          10,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat: %v", err)
	}
	sent, err := channels.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: created.Channel.ID,
		RandomID:  42,
		Message:   "restore read outbox",
		Date:      11,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := channels.ReadHistory(ctx, 1002, domain.ReadChannelHistoryRequest{
		ChannelID: created.Channel.ID,
		MaxID:     sent.Message.ID,
		Date:      12,
	}); err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}

	list, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetDialogs: %v", err)
	}
	dialog := findChannelDialog(t, list, created.Channel.ID)
	if dialog.ReadOutboxMaxID != sent.Message.ID {
		t.Fatalf("getDialogs read_outbox = %d, want %d", dialog.ReadOutboxMaxID, sent.Message.ID)
	}

	peerList, err := dialogs.GetPeerDialogs(ctx, 1001, []domain.Peer{
		{Type: domain.PeerTypeChannel, ID: created.Channel.ID},
	})
	if err != nil {
		t.Fatalf("GetPeerDialogs: %v", err)
	}
	peerDialog := findChannelDialog(t, peerList, created.Channel.ID)
	if peerDialog.ReadOutboxMaxID != sent.Message.ID {
		t.Fatalf("getPeerDialogs read_outbox = %d, want %d", peerDialog.ReadOutboxMaxID, sent.Message.ID)
	}
}

func TestChannelDialogSettingsPersistThroughUnifiedDialogService(t *testing.T) {
	ctx := context.Background()
	channelStore := memory.NewChannelStore()
	channels := appchannels.NewService(channelStore)
	dialogs := NewService(nil, channelStore)

	first, err := channels.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Pinned One",
		Date:  20,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat first: %v", err)
	}
	second, err := channels.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Pinned Two",
		Date:  21,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat second: %v", err)
	}
	firstPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: first.Channel.ID}
	secondPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: second.Channel.ID}

	if changed, err := dialogs.TogglePinned(ctx, 1001, firstPeer, true); err != nil || !changed {
		t.Fatalf("TogglePinned first = changed %v err %v, want changed", changed, err)
	}
	if changed, err := dialogs.TogglePinned(ctx, 1001, secondPeer, true); err != nil || !changed {
		t.Fatalf("TogglePinned second = changed %v err %v, want changed", changed, err)
	}
	if err := dialogs.ReorderPinned(ctx, 1001, []domain.Peer{secondPeer, firstPeer}, true); err != nil {
		t.Fatalf("ReorderPinned: %v", err)
	}
	if changed, err := dialogs.MarkUnread(ctx, 1001, firstPeer, true); err != nil || !changed {
		t.Fatalf("MarkUnread = changed %v err %v, want changed", changed, err)
	}
	if err := dialogs.EditPeerFolders(ctx, 1001, []domain.FolderPeerUpdate{
		{Peer: firstPeer, FolderID: domain.DialogArchiveFolderID},
	}); err != nil {
		t.Fatalf("EditPeerFolders: %v", err)
	}

	list, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetDialogs: %v", err)
	}
	firstDialog := findChannelDialog(t, list, first.Channel.ID)
	if !firstDialog.Pinned || firstDialog.PinnedOrder != 1 || !firstDialog.UnreadMark || firstDialog.FolderID != domain.DialogArchiveFolderID {
		t.Fatalf("first dialog = %+v, want pinned order 1, unread mark, archived", firstDialog)
	}
	secondDialog := findChannelDialog(t, list, second.Channel.ID)
	if !secondDialog.Pinned || secondDialog.PinnedOrder != 2 {
		t.Fatalf("second dialog = %+v, want pinned order 2", secondDialog)
	}
	marks, err := dialogs.UnreadMarks(ctx, 1001)
	if err != nil {
		t.Fatalf("UnreadMarks: %v", err)
	}
	if len(marks) != 1 || marks[0] != firstPeer {
		t.Fatalf("unread marks = %+v, want first channel", marks)
	}
	archived, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{
		HasFolderID: true,
		FolderID:    domain.DialogArchiveFolderID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("GetDialogs archive: %v", err)
	}
	if got := findChannelDialog(t, archived, first.Channel.ID); got.FolderID != domain.DialogArchiveFolderID {
		t.Fatalf("archived dialog = %+v, want archive folder", got)
	}

	custom, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{
		HasFolderID: true,
		FolderID:    domain.DialogCustomFolderMinID,
		Folder:      &domain.DialogFolder{ID: domain.DialogCustomFolderMinID, Groups: true},
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("GetDialogs custom groups: %v", err)
	}
	if got := findChannelDialog(t, custom, first.Channel.ID); got.Peer.ID != first.Channel.ID {
		t.Fatalf("custom group dialog = %+v, want first channel", got)
	}
}

func TestGetDialogsAppliesChannelDialogOffset(t *testing.T) {
	ctx := context.Background()
	channelStore := memory.NewChannelStore()
	channels := appchannels.NewService(channelStore)
	dialogs := NewService(nil, channelStore)

	old, err := channels.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Older Channel",
		Date:  20,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat old: %v", err)
	}
	newer, err := channels.CreateMegagroupFromCreateChat(ctx, 1001, domain.CreateChannelRequest{
		Title: "Newer Channel",
		Date:  30,
	})
	if err != nil {
		t.Fatalf("CreateMegagroupFromCreateChat newer: %v", err)
	}

	first, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{Limit: 1})
	if err != nil {
		t.Fatalf("GetDialogs first: %v", err)
	}
	if len(first.Dialogs) != 1 || first.Dialogs[0].Peer.ID != newer.Channel.ID {
		t.Fatalf("first page dialogs = %+v, want newer channel", first.Dialogs)
	}

	next, err := dialogs.GetDialogs(ctx, 1001, domain.DialogFilter{
		OffsetDate:    first.Dialogs[0].TopMessageDate,
		OffsetID:      first.Dialogs[0].TopMessage,
		HasOffsetPeer: true,
		OffsetPeer:    first.Dialogs[0].Peer,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("GetDialogs next: %v", err)
	}
	if len(next.Dialogs) != 1 || next.Dialogs[0].Peer.ID != old.Channel.ID {
		t.Fatalf("next page dialogs = %+v, want only older channel", next.Dialogs)
	}
}

func TestGetPeerDialogsRejectsHugeVector(t *testing.T) {
	dialogs := NewService(nil, memory.NewChannelStore())
	peers := make([]domain.Peer, domain.MaxDialogFolderPeers+1)
	for i := range peers {
		peers[i] = domain.Peer{Type: domain.PeerTypeChannel, ID: int64(i + 1)}
	}
	if _, err := dialogs.GetPeerDialogs(context.Background(), 1001, peers); !errors.Is(err, domain.ErrChannelInvalid) {
		t.Fatalf("GetPeerDialogs huge vector err = %v, want ErrChannelInvalid", err)
	}
}

func TestGetPeerDialogsIncludesPublicChannelPreviewForNonMember(t *testing.T) {
	ctx := context.Background()
	channelStore := memory.NewChannelStore()
	channels := appchannels.NewService(channelStore)
	dialogs := NewService(nil, channelStore)

	public, err := channels.CreateChannel(ctx, 1001, domain.CreateChannelRequest{
		Title:     "Public Peer Dialog",
		Broadcast: true,
		Date:      1700002000,
	})
	if err != nil {
		t.Fatalf("CreateChannel public: %v", err)
	}
	if _, err := channels.UpdateUsername(ctx, 1001, domain.UpdateChannelUsernameRequest{
		UserID:    1001,
		ChannelID: public.Channel.ID,
		Username:  "public_peer_dialog",
	}); err != nil {
		t.Fatalf("UpdateUsername public: %v", err)
	}
	sent, err := channels.SendMessage(ctx, 1001, domain.SendChannelMessageRequest{
		ChannelID: public.Channel.ID,
		RandomID:  99,
		Message:   "public peer dialog top",
		Date:      1700002010,
	})
	if err != nil {
		t.Fatalf("SendMessage public: %v", err)
	}
	private, err := channels.CreateChannel(ctx, 1001, domain.CreateChannelRequest{
		Title:     "Private Peer Dialog",
		Broadcast: true,
		Date:      1700002020,
	})
	if err != nil {
		t.Fatalf("CreateChannel private: %v", err)
	}

	list, err := dialogs.GetPeerDialogs(ctx, 1002, []domain.Peer{
		{Type: domain.PeerTypeChannel, ID: public.Channel.ID},
		{Type: domain.PeerTypeChannel, ID: private.Channel.ID},
	})
	if err != nil {
		t.Fatalf("GetPeerDialogs public preview: %v", err)
	}
	if len(list.Dialogs) != 1 {
		t.Fatalf("dialogs = %+v, want only public preview dialog", list.Dialogs)
	}
	dialog := findChannelDialog(t, list, public.Channel.ID)
	if dialog.TopMessage != sent.Message.ID || dialog.TopMessageDate != sent.Message.Date {
		t.Fatalf("preview dialog top = id %d date %d, want %d/%d", dialog.TopMessage, dialog.TopMessageDate, sent.Message.ID, sent.Message.Date)
	}
	if !dialog.ChannelLeft {
		t.Fatalf("preview dialog ChannelLeft = false, want read-only left preview")
	}
	if dialog.UnreadCount != 0 || dialog.ReadInboxMaxID < sent.Message.ID || dialog.ReadOutboxMaxID < sent.Message.ID {
		t.Fatalf("preview dialog read/unread = %+v, want read through top and no unread", dialog)
	}
	if len(list.ChannelMessages) != 1 || list.ChannelMessages[0].Body != "public peer dialog top" {
		t.Fatalf("channel messages = %+v, want public top message", list.ChannelMessages)
	}
	if len(list.Channels) != 1 || list.Channels[0].ID != public.Channel.ID {
		t.Fatalf("channels = %+v, want public channel shell", list.Channels)
	}
}

func findChannelDialog(t *testing.T, list domain.DialogList, channelID int64) domain.Dialog {
	t.Helper()
	for _, dialog := range list.Dialogs {
		if dialog.Peer.Type == domain.PeerTypeChannel && dialog.Peer.ID == channelID {
			return dialog
		}
	}
	t.Fatalf("channel dialog %d not found in %+v", channelID, list.Dialogs)
	return domain.Dialog{}
}
