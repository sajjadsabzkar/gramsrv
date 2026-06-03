package dialogs

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sort"
	"unicode/utf8"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service 提供会话列表查询。
type Service struct {
	dialogs  store.DialogStore
	channels store.ChannelStore
}

// NewService 创建 dialogs 服务。
func NewService(dialogs store.DialogStore, channels ...store.ChannelStore) *Service {
	s := &Service{dialogs: dialogs}
	if len(channels) > 0 {
		s.channels = channels[0]
	}
	return s
}

// GetDialogs 返回当前登录账号的会话摘要。未登录或无持久化实现时按空账号处理。
func (s *Service) GetDialogs(ctx context.Context, userID int64, filter domain.DialogFilter) (domain.DialogList, error) {
	if s == nil || userID == 0 {
		return domain.DialogList{}, nil
	}
	if filter.HasFolderID && filter.FolderID >= domain.DialogCustomFolderMinID && filter.Folder == nil {
		if s.dialogs == nil {
			return domain.DialogList{}, nil
		}
		folder, found, err := s.dialogs.GetFolder(ctx, userID, filter.FolderID)
		if err != nil {
			return domain.DialogList{}, err
		}
		if !found {
			return domain.DialogList{}, nil
		}
		filter.Folder = &folder
	}
	var out domain.DialogList
	if s.dialogs != nil {
		list, err := s.dialogs.ListByUser(ctx, userID, filter)
		if err != nil {
			return domain.DialogList{}, err
		}
		out = mergeDialogLists(out, list)
	}
	if s.channels != nil {
		list, err := s.channels.ListChannelDialogs(ctx, userID, filter)
		if err != nil {
			return domain.DialogList{}, err
		}
		out = mergeChannelDialogs(out, list)
	}
	sortDialogList(out.Dialogs)
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if len(out.Dialogs) > limit {
		keep := make(map[domain.Peer]struct{}, limit)
		for _, d := range out.Dialogs[:limit] {
			keep[d.Peer] = struct{}{}
		}
		out.Dialogs = out.Dialogs[:limit]
		out.Messages = filterPrivateMessagesByPeer(out.Messages, keep)
		out.ChannelMessages = filterChannelMessagesByPeer(out.ChannelMessages, keep)
		out.Channels = filterChannelsByPeer(out.Channels, keep)
	}
	if out.Count == 0 {
		out.Count = len(out.Dialogs)
	}
	if err := s.attachDrafts(ctx, userID, &out); err != nil {
		return domain.DialogList{}, err
	}
	return out, nil
}

// GetPeerDialogs 返回指定 peer 的会话摘要。缺失的 peer 由 store 按空会话占位返回。
func (s *Service) GetPeerDialogs(ctx context.Context, userID int64, peers []domain.Peer) (domain.DialogList, error) {
	if s == nil || userID == 0 || len(peers) == 0 {
		return domain.DialogList{}, nil
	}
	if len(peers) > domain.MaxDialogFolderPeers {
		return domain.DialogList{}, domain.ErrChannelInvalid
	}
	userPeers := make([]domain.Peer, 0, len(peers))
	channelIDs := make([]int64, 0, len(peers))
	for _, peer := range peers {
		switch peer.Type {
		case domain.PeerTypeUser:
			userPeers = append(userPeers, peer)
		case domain.PeerTypeChannel:
			channelIDs = append(channelIDs, peer.ID)
		}
	}
	var out domain.DialogList
	if len(userPeers) > 0 && s.dialogs != nil {
		list, err := s.dialogs.ListByPeers(ctx, userID, userPeers)
		if err != nil {
			return domain.DialogList{}, err
		}
		out = mergeDialogLists(out, list)
	}
	if len(channelIDs) > 0 && s.channels != nil {
		list, err := s.channels.GetChannelDialogs(ctx, userID, channelIDs)
		if err != nil {
			return domain.DialogList{}, err
		}
		out = mergeChannelDialogs(out, list)
		out, err = s.appendMissingChannelPeerPreviews(ctx, userID, channelIDs, out)
		if err != nil {
			return domain.DialogList{}, err
		}
	}
	if err := s.attachDrafts(ctx, userID, &out); err != nil {
		return domain.DialogList{}, err
	}
	return out, nil
}

func (s *Service) appendMissingChannelPeerPreviews(ctx context.Context, userID int64, channelIDs []int64, out domain.DialogList) (domain.DialogList, error) {
	if s == nil || s.channels == nil || userID == 0 || len(channelIDs) == 0 {
		return out, nil
	}
	present := make(map[int64]struct{}, len(out.Dialogs))
	for _, dialog := range out.Dialogs {
		if dialog.Peer.Type == domain.PeerTypeChannel && dialog.Peer.ID != 0 {
			present[dialog.Peer.ID] = struct{}{}
		}
	}
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID == 0 {
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		if _, ok := present[channelID]; ok {
			continue
		}

		view, err := s.channels.GetChannel(ctx, userID, channelID)
		if err != nil {
			if isChannelPreviewAccessError(err) {
				continue
			}
			return domain.DialogList{}, err
		}
		history, err := s.channels.ListChannelHistory(ctx, userID, domain.ChannelHistoryFilter{
			ChannelID: channelID,
			Limit:     1,
		})
		if err != nil {
			if isChannelPreviewAccessError(err) {
				continue
			}
			return domain.DialogList{}, err
		}

		dialog := dialogFromChannelView(view)
		if len(history.Messages) > 0 {
			top := history.Messages[0]
			dialog.TopMessage = top.ID
			dialog.TopMessageDate = top.Date
			out.ChannelMessages = append(out.ChannelMessages, top)
		}
		out.Dialogs = append(out.Dialogs, dialog)
		out.Channels = append(out.Channels, view.Channel)
		out.Channels = append(out.Channels, history.Channels...)
		out.Users = append(out.Users, history.Users...)
		out.Count++
		present[channelID] = struct{}{}
	}
	return out, nil
}

func isChannelPreviewAccessError(err error) bool {
	return errors.Is(err, domain.ErrChannelPrivate) ||
		errors.Is(err, domain.ErrChannelUserBanned) ||
		errors.Is(err, domain.ErrChannelInvalid)
}

func dialogFromChannelView(view domain.ChannelView) domain.Dialog {
	dialog := view.Dialog
	return domain.Dialog{
		Peer:                domain.Peer{Type: domain.PeerTypeChannel, ID: dialog.ChannelID},
		ChannelLeft:         view.Self.Status == domain.ChannelMemberLeft,
		FolderID:            dialog.FolderID,
		TopMessage:          dialog.TopMessageID,
		TopMessageDate:      dialog.TopMessageDate,
		ReadInboxMaxID:      dialog.ReadInboxMaxID,
		ReadOutboxMaxID:     dialog.ReadOutboxMaxID,
		UnreadCount:         dialog.UnreadCount,
		UnreadMentions:      dialog.UnreadMentions,
		Pinned:              dialog.Pinned,
		PinnedOrder:         dialog.PinnedOrder,
		UnreadMark:          dialog.UnreadMark,
		ViewForumAsMessages: dialog.ViewForumAsMessages,
	}
}

// SaveDraft stores or clears a cloud draft for one peer/topic.
func (s *Service) SaveDraft(ctx context.Context, userID int64, draft domain.DialogDraft) error {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil
	}
	if err := validateDraft(draft); err != nil {
		return err
	}
	if draft.Empty() {
		_, err := s.dialogs.DeleteDraft(ctx, userID, draft.Peer, draft.TopMessageID)
		return err
	}
	return s.dialogs.SaveDraft(ctx, userID, draft)
}

// DeleteDraft clears one cloud draft.
func (s *Service) DeleteDraft(ctx context.Context, userID int64, peer domain.Peer, topMessageID int) (bool, error) {
	if s == nil || s.dialogs == nil || userID == 0 {
		return false, nil
	}
	if err := validateDraftKey(peer, topMessageID); err != nil {
		return false, err
	}
	return s.dialogs.DeleteDraft(ctx, userID, peer, topMessageID)
}

// ListDrafts returns bounded cloud drafts for messages.getAllDrafts.
func (s *Service) ListDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error) {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil, nil
	}
	return s.dialogs.ListDrafts(ctx, userID, clampDraftLimit(limit))
}

// ClearDrafts deletes bounded cloud drafts for messages.clearAllDrafts.
func (s *Service) ClearDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error) {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil, nil
	}
	return s.dialogs.ClearDrafts(ctx, userID, clampDraftLimit(limit))
}

func (s *Service) TogglePinned(ctx context.Context, userID int64, peer domain.Peer, pinned bool) (bool, error) {
	if s == nil || userID == 0 || peer.Type == "" || peer.ID == 0 {
		return false, nil
	}
	switch peer.Type {
	case domain.PeerTypeChannel:
		if s.channels == nil {
			return false, nil
		}
		return s.channels.SetChannelDialogPinned(ctx, userID, peer.ID, pinned)
	default:
		if s.dialogs == nil {
			return false, nil
		}
		return s.dialogs.SetPinned(ctx, userID, peer, pinned)
	}
}

func (s *Service) ReorderPinned(ctx context.Context, userID int64, order []domain.Peer, force bool) error {
	if s == nil || userID == 0 {
		return nil
	}
	if s.dialogs != nil {
		if err := s.dialogs.ReorderPinned(ctx, userID, order, force); err != nil {
			return err
		}
	}
	if s.channels != nil {
		if err := s.channels.ReorderChannelPinnedDialogs(ctx, userID, order, force); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) MarkUnread(ctx context.Context, userID int64, peer domain.Peer, unread bool) (bool, error) {
	if s == nil || userID == 0 || peer.Type == "" || peer.ID == 0 {
		return false, nil
	}
	switch peer.Type {
	case domain.PeerTypeChannel:
		if s.channels == nil {
			return false, nil
		}
		return s.channels.SetChannelDialogUnreadMark(ctx, userID, peer.ID, unread)
	default:
		if s.dialogs == nil {
			return false, nil
		}
		return s.dialogs.SetUnreadMark(ctx, userID, peer, unread)
	}
}

func (s *Service) UnreadMarks(ctx context.Context, userID int64) ([]domain.Peer, error) {
	if s == nil || userID == 0 {
		return nil, nil
	}
	var out []domain.Peer
	if s.dialogs != nil {
		peers, err := s.dialogs.ListUnreadMarked(ctx, userID)
		if err != nil {
			return nil, err
		}
		out = append(out, peers...)
	}
	if s.channels != nil {
		peers, err := s.channels.ListChannelUnreadMarked(ctx, userID)
		if err != nil {
			return nil, err
		}
		out = append(out, peers...)
	}
	return out, nil
}

func (s *Service) HidePeerSettingsBar(ctx context.Context, userID int64, peer domain.Peer) (bool, error) {
	if s == nil || s.dialogs == nil || userID == 0 || peer.Type == "" || peer.ID == 0 {
		return false, nil
	}
	return s.dialogs.SetPeerSettingsBarHidden(ctx, userID, peer)
}

func (s *Service) PeerSettingsBarHidden(ctx context.Context, userID int64, peer domain.Peer) (bool, error) {
	if s == nil || s.dialogs == nil || userID == 0 || peer.Type == "" || peer.ID == 0 {
		return false, nil
	}
	return s.dialogs.PeerSettingsBarHidden(ctx, userID, peer)
}

func (s *Service) GetDialogFolders(ctx context.Context, userID int64) (domain.DialogFolderList, error) {
	if s == nil || s.dialogs == nil || userID == 0 {
		return domain.DialogFolderList{}, nil
	}
	return s.dialogs.ListFolders(ctx, userID)
}

func (s *Service) SaveDialogFolder(ctx context.Context, userID int64, folder domain.DialogFolder) error {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil
	}
	return s.dialogs.UpsertFolder(ctx, userID, folder)
}

func (s *Service) DeleteDialogFolder(ctx context.Context, userID int64, folderID int) error {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil
	}
	return s.dialogs.DeleteFolder(ctx, userID, folderID)
}

func (s *Service) ReorderDialogFolders(ctx context.Context, userID int64, order []int) error {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil
	}
	return s.dialogs.ReorderFolders(ctx, userID, order)
}

func (s *Service) ToggleDialogFolderTags(ctx context.Context, userID int64, enabled bool) error {
	if s == nil || s.dialogs == nil || userID == 0 {
		return nil
	}
	return s.dialogs.SetFolderTagsEnabled(ctx, userID, enabled)
}

func (s *Service) EditPeerFolders(ctx context.Context, userID int64, peers []domain.FolderPeerUpdate) error {
	if s == nil || userID == 0 || len(peers) == 0 {
		return nil
	}
	privatePeers := make([]domain.FolderPeerUpdate, 0, len(peers))
	channelPeers := make([]domain.FolderPeerUpdate, 0, len(peers))
	for _, peer := range peers {
		if peer.Peer.Type == domain.PeerTypeChannel {
			channelPeers = append(channelPeers, peer)
		} else {
			privatePeers = append(privatePeers, peer)
		}
	}
	if len(privatePeers) > 0 && s.dialogs != nil {
		if err := s.dialogs.EditPeerFolders(ctx, userID, privatePeers); err != nil {
			return err
		}
	}
	if len(channelPeers) > 0 && s.channels != nil {
		if err := s.channels.EditChannelPeerFolders(ctx, userID, channelPeers); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) attachDrafts(ctx context.Context, userID int64, list *domain.DialogList) error {
	if s == nil || s.dialogs == nil || userID == 0 || list == nil || len(list.Dialogs) == 0 {
		return nil
	}
	drafts, err := s.dialogs.ListDrafts(ctx, userID, domain.MaxDialogDraftsPerUser)
	if err != nil {
		return err
	}
	if len(drafts) == 0 {
		return nil
	}
	byPeer := make(map[domain.Peer]domain.DialogDraft, len(drafts))
	for _, draft := range drafts {
		if draft.TopMessageID != 0 {
			continue
		}
		byPeer[draft.Peer] = cloneDraft(draft)
	}
	if len(byPeer) == 0 {
		return nil
	}
	attached := false
	for i := range list.Dialogs {
		draft, ok := byPeer[list.Dialogs[i].Peer]
		if !ok {
			continue
		}
		d := cloneDraft(draft)
		list.Dialogs[i].Draft = &d
		attached = true
	}
	if attached {
		list.Hash = dialogHashWithDrafts(list.Hash, list.Dialogs)
	}
	return nil
}

func validateDraft(draft domain.DialogDraft) error {
	if err := validateDraftKey(draft.Peer, draft.TopMessageID); err != nil {
		return err
	}
	if len(draft.Entities) > domain.MaxMessageEntityCount {
		return domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(draft.Message) > domain.MaxMessageTextLength {
		return domain.ErrChannelInvalid
	}
	if domain.ValidateMessageReplyBounds(draft.ReplyTo) != nil {
		return domain.ErrReplyMessageIDInvalid
	}
	return nil
}

func validateDraftKey(peer domain.Peer, topMessageID int) error {
	if peer.ID == 0 || (peer.Type != domain.PeerTypeUser && peer.Type != domain.PeerTypeChannel) {
		return domain.ErrChannelInvalid
	}
	if topMessageID < 0 || topMessageID > domain.MaxMessageBoxID {
		return domain.ErrReplyMessageIDInvalid
	}
	return nil
}

func clampDraftLimit(limit int) int {
	if limit <= 0 || limit > domain.MaxDialogDraftsPerUser {
		return domain.MaxDialogDraftsPerUser
	}
	return limit
}

func cloneDraft(draft domain.DialogDraft) domain.DialogDraft {
	draft.Entities = append([]domain.MessageEntity(nil), draft.Entities...)
	if draft.ReplyTo != nil {
		reply := *draft.ReplyTo
		reply.QuoteEntities = append([]domain.MessageEntity(nil), draft.ReplyTo.QuoteEntities...)
		draft.ReplyTo = &reply
	}
	if draft.WebPage != nil {
		webpage := *draft.WebPage
		draft.WebPage = &webpage
	}
	return draft
}

func dialogHashWithDrafts(base int64, dialogs []domain.Dialog) int64 {
	if len(dialogs) == 0 {
		return base
	}
	h := fnv.New64a()
	var buf [48]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(base))
	_, _ = h.Write(buf[:8])
	for _, d := range dialogs {
		if d.Draft == nil {
			continue
		}
		binary.LittleEndian.PutUint64(buf[:8], uint64(d.Peer.ID))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(d.Draft.TopMessageID))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(d.Draft.Date))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(len(d.Draft.Message)))
		if d.Draft.NoWebpage {
			buf[24] = 1
		} else {
			buf[24] = 0
		}
		if d.Draft.InvertMedia {
			buf[25] = 1
		} else {
			buf[25] = 0
		}
		binary.LittleEndian.PutUint64(buf[26:34], uint64(len(d.Draft.Entities)))
		binary.LittleEndian.PutUint64(buf[34:42], uint64(d.Draft.Effect))
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(d.Draft.Message))
		if d.Draft.WebPage != nil {
			_, _ = h.Write([]byte(d.Draft.WebPage.URL))
		}
	}
	return int64(h.Sum64())
}

func mergeDialogLists(out, in domain.DialogList) domain.DialogList {
	out.Dialogs = append(out.Dialogs, in.Dialogs...)
	out.Messages = append(out.Messages, in.Messages...)
	out.Users = append(out.Users, in.Users...)
	out.Count += in.Count
	out.Hash ^= in.Hash
	return out
}

func mergeChannelDialogs(out domain.DialogList, in domain.ChannelDialogList) domain.DialogList {
	out.Dialogs = append(out.Dialogs, in.Dialogs...)
	out.ChannelMessages = append(out.ChannelMessages, in.Messages...)
	out.Channels = append(out.Channels, in.Channels...)
	out.Users = append(out.Users, in.Users...)
	out.Count += in.Count
	out.Hash ^= in.Hash
	return out
}

func sortDialogList(dialogs []domain.Dialog) {
	sort.SliceStable(dialogs, func(i, j int) bool {
		if dialogs[i].Pinned != dialogs[j].Pinned {
			return dialogs[i].Pinned
		}
		if dialogs[i].PinnedOrder != dialogs[j].PinnedOrder {
			return dialogs[i].PinnedOrder > dialogs[j].PinnedOrder
		}
		if dialogs[i].TopMessageDate != dialogs[j].TopMessageDate {
			return dialogs[i].TopMessageDate > dialogs[j].TopMessageDate
		}
		if dialogs[i].TopMessage != dialogs[j].TopMessage {
			return dialogs[i].TopMessage > dialogs[j].TopMessage
		}
		return dialogs[i].Peer.ID > dialogs[j].Peer.ID
	})
}

func filterPrivateMessagesByPeer(messages []domain.Message, keep map[domain.Peer]struct{}) []domain.Message {
	out := messages[:0]
	for _, msg := range messages {
		if _, ok := keep[msg.Peer]; ok {
			out = append(out, msg)
		}
	}
	return out
}

func filterChannelMessagesByPeer(messages []domain.ChannelMessage, keep map[domain.Peer]struct{}) []domain.ChannelMessage {
	out := messages[:0]
	for _, msg := range messages {
		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: msg.ChannelID}
		if _, ok := keep[peer]; ok {
			out = append(out, msg)
		}
	}
	return out
}

func filterChannelsByPeer(channels []domain.Channel, keep map[domain.Peer]struct{}) []domain.Channel {
	out := channels[:0]
	for _, ch := range channels {
		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: ch.ID}
		if _, ok := keep[peer]; ok {
			out = append(out, ch)
		}
	}
	return out
}
