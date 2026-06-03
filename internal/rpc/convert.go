package rpc

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

// authzFromCtx 从连接上下文组装一条待绑定的设备授权（UserID 由业务层填充）。
func (r *Router) authzFromCtx(ctx context.Context) domain.Authorization {
	id, _ := AuthKeyIDFrom(ctx)
	a := domain.Authorization{AuthKeyID: id, Layer: LayerFrom(ctx)}
	if ci, ok := ClientInfoFrom(ctx); ok {
		a.DeviceModel = ci.DeviceModel
		a.SystemVersion = ci.SystemVersion
		a.AppVersion = ci.AppVersion
		a.APIID = ci.APIID
	}
	return a
}

// currentUserID 返回当前连接已登录的 user_id。
//
// 优先使用 active session 缓存；若新连接尚未绑定但 auth_key 已授权，则只在这里
// 查询一次授权表并回填 session，避免各业务 service 每个 RPC 重复 authKey→userID。
func (r *Router) currentUserID(ctx context.Context) (int64, bool, error) {
	if userID, ok := UserIDFrom(ctx); ok {
		return userID, true, nil
	}
	if r.deps.Sessions != nil {
		if sessionID, ok := SessionIDFrom(ctx); ok {
			if scoped, ok := r.scopedSessions(); ok {
				if rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx); ok {
					if userID, resolved := scoped.UserIDResolvedForAuthKey(rawAuthKeyID, sessionID); resolved {
						return userID, userID != 0, nil
					}
				}
			} else if userID, resolved := r.deps.Sessions.UserIDResolved(sessionID); resolved {
				return userID, userID != 0, nil
			}
		}
	}
	if r.deps.Auth == nil {
		return 0, false, nil
	}
	authKeyID, ok := AuthKeyIDFrom(ctx)
	if !ok {
		return 0, false, nil
	}
	userID, found, err := r.deps.Auth.UserID(ctx, authKeyID)
	if err != nil || !found {
		if err == nil && !found {
			r.bindSessionUser(ctx, 0)
		}
		return 0, found, err
	}
	r.bindSessionUser(ctx, userID)
	return userID, true, nil
}

// tgSelfUser 把 domain.User 转为 self 标记的 tg.User（optional 字段由 Encode 自动 SetFlags）。
func tgSelfUser(u domain.User) *tg.User {
	out := &tg.User{
		ID:            u.ID,
		AccessHash:    u.AccessHash,
		FirstName:     u.FirstName,
		LastName:      u.LastName,
		Username:      u.Username,
		Phone:         u.Phone,
		Self:          true,
		Verified:      u.Verified,
		Support:       u.Support,
		Contact:       u.Contact,
		MutualContact: u.Mutual,
		Usernames:     tgUsernames(u.Username),
		Status:        tgUserStatus(u.Status),
	}
	if photo := tgUserProfilePhoto(u); photo != nil {
		out.Photo = photo
	}
	return out
}

func tgUser(u domain.User) *tg.User {
	out := &tg.User{
		ID:            u.ID,
		AccessHash:    u.AccessHash,
		FirstName:     u.FirstName,
		LastName:      u.LastName,
		Username:      u.Username,
		Phone:         u.Phone,
		Verified:      u.Verified,
		Support:       u.Support,
		Contact:       u.Contact,
		MutualContact: u.Mutual,
		Usernames:     tgUsernames(u.Username),
		Status:        tgUserStatus(u.Status),
	}
	if photo := tgUserProfilePhoto(u); photo != nil {
		out.Photo = photo
	}
	return out
}

// tgUserProfilePhoto 由 domain.User 反范式头像字段构造 UserProfilePhoto；无头像返回 nil（Encode 时为 empty）。
func tgUserProfilePhoto(u domain.User) tg.UserProfilePhotoClass {
	if u.PhotoID == 0 {
		return nil
	}
	photo := &tg.UserProfilePhoto{PhotoID: u.PhotoID, DCID: u.PhotoDCID}
	if len(u.PhotoStripped) > 0 {
		photo.SetStrippedThumb(u.PhotoStripped)
	}
	return photo
}

func tgUserStatus(status domain.UserStatus) tg.UserStatusClass {
	switch status.Kind {
	case domain.UserStatusOnline:
		if status.Expires > 0 {
			return &tg.UserStatusOnline{Expires: status.Expires}
		}
	case domain.UserStatusOffline:
		if status.WasOnline > 0 {
			return &tg.UserStatusOffline{WasOnline: status.WasOnline}
		}
	case domain.UserStatusLastWeek:
		return &tg.UserStatusLastWeek{}
	case domain.UserStatusLastMonth:
		return &tg.UserStatusLastMonth{}
	case domain.UserStatusEmpty:
		return &tg.UserStatusEmpty{}
	case domain.UserStatusRecently, domain.UserStatusUnknown:
		return &tg.UserStatusRecently{}
	}
	return &tg.UserStatusRecently{}
}

func tgUsernames(username string) []tg.Username {
	if username == "" {
		return nil
	}
	return []tg.Username{{Editable: true, Active: true, Username: username}}
}

func tgUpdateState(st domain.UpdateState) tg.UpdatesState {
	return tg.UpdatesState{Pts: st.Pts, Qts: st.Qts, Date: st.Date, Seq: st.Seq}
}

func tgContacts(list domain.ContactList) tg.ContactsContactsClass {
	out := &tg.ContactsContacts{
		Contacts:   make([]tg.Contact, 0, len(list.Contacts)),
		Users:      make([]tg.UserClass, 0, len(list.Contacts)),
		SavedCount: len(list.Contacts),
	}
	for _, c := range list.Contacts {
		out.Contacts = append(out.Contacts, tg.Contact{UserID: c.User.ID, Mutual: c.Mutual})
		out.Users = append(out.Users, tgUser(c.User))
	}
	return out
}

func tgContactsFound(viewerUserID int64, res domain.UserSearchResult) *tg.ContactsFound {
	out := &tg.ContactsFound{
		MyResults: make([]tg.PeerClass, 0, len(res.MyResults)+len(res.MyChannelResults)),
		Results:   make([]tg.PeerClass, 0, len(res.Results)+len(res.ChannelResults)),
		Chats:     make([]tg.ChatClass, 0, len(res.MyChannelResults)+len(res.ChannelResults)),
		Users:     make([]tg.UserClass, 0, len(res.MyResults)+len(res.Results)),
	}
	seen := make(map[int64]struct{}, len(res.MyResults)+len(res.Results))
	appendUser := func(u domain.User) {
		if u.ID == 0 {
			return
		}
		if _, ok := seen[u.ID]; ok {
			return
		}
		seen[u.ID] = struct{}{}
		out.Users = append(out.Users, tgUser(u))
	}
	seenChannels := make(map[int64]struct{}, len(res.MyChannelResults)+len(res.ChannelResults))
	appendChannel := func(ch domain.Channel, self *domain.ChannelMember) {
		if ch.ID == 0 {
			return
		}
		if _, ok := seenChannels[ch.ID]; ok {
			return
		}
		seenChannels[ch.ID] = struct{}{}
		out.Chats = append(out.Chats, tgChannelChat(viewerUserID, ch, self))
	}
	for _, u := range res.MyResults {
		out.MyResults = append(out.MyResults, &tg.PeerUser{UserID: u.ID})
		appendUser(u)
	}
	for _, ch := range res.MyChannelResults {
		out.MyResults = append(out.MyResults, &tg.PeerChannel{ChannelID: ch.ID})
		appendChannel(ch, nil)
	}
	for _, u := range res.Results {
		out.Results = append(out.Results, &tg.PeerUser{UserID: u.ID})
		appendUser(u)
	}
	for _, ch := range res.ChannelResults {
		out.Results = append(out.Results, &tg.PeerChannel{ChannelID: ch.ID})
		appendChannel(ch, &domain.ChannelMember{ChannelID: ch.ID, UserID: viewerUserID, Status: domain.ChannelMemberLeft})
	}
	return out
}

func tgMessagesDialogs(viewerUserID int64, list domain.DialogList) tg.MessagesDialogsClass {
	dialogs := make([]tg.DialogClass, 0, len(list.Dialogs))
	for _, d := range list.Dialogs {
		if dialog := tgDialog(d); dialog != nil {
			dialogs = append(dialogs, dialog)
		}
	}
	messages := make([]tg.MessageClass, 0, len(list.Messages))
	for _, msg := range list.Messages {
		if item := tgMessage(msg); item != nil {
			messages = append(messages, item)
		}
	}
	for _, msg := range list.ChannelMessages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			messages = append(messages, item)
		}
	}
	users := tgUsers(list.Users)
	chats := tgChannelsForDialogs(viewerUserID, list.Channels, list.Dialogs)
	if list.Count > len(dialogs) {
		return &tg.MessagesDialogsSlice{
			Count:    list.Count,
			Dialogs:  dialogs,
			Messages: messages,
			Chats:    chats,
			Users:    users,
		}
	}
	return &tg.MessagesDialogs{
		Dialogs:  dialogs,
		Messages: messages,
		Chats:    chats,
		Users:    users,
	}
}

func tgMessagesMessages(list domain.MessageList) tg.MessagesMessagesClass {
	messages := make([]tg.MessageClass, 0, len(list.Messages))
	for _, msg := range list.Messages {
		if item := tgMessage(msg); item != nil {
			messages = append(messages, item)
		}
	}
	users := tgUsers(list.Users)
	if list.Count > len(messages) {
		return &tg.MessagesMessagesSlice{
			Count:    list.Count,
			Messages: messages,
			Users:    users,
		}
	}
	return &tg.MessagesMessages{
		Messages: messages,
		Users:    users,
	}
}

func tgUsers(users []domain.User) []tg.UserClass {
	out := make([]tg.UserClass, 0, len(users))
	for _, u := range users {
		out = append(out, tgUser(u))
	}
	return out
}

func tgPeerDialogs(viewerUserID int64, list domain.DialogList, st domain.UpdateState) *tg.MessagesPeerDialogs {
	out := &tg.MessagesPeerDialogs{
		Dialogs:  make([]tg.DialogClass, 0, len(list.Dialogs)),
		Messages: make([]tg.MessageClass, 0, len(list.Messages)+len(list.ChannelMessages)),
		Users:    make([]tg.UserClass, 0, len(list.Users)),
		Chats:    make([]tg.ChatClass, 0, len(list.Channels)),
		State:    tgUpdateState(st),
	}
	for _, d := range list.Dialogs {
		if dialog := tgDialog(d); dialog != nil {
			out.Dialogs = append(out.Dialogs, dialog)
		}
	}
	for _, msg := range list.Messages {
		if item := tgMessage(msg); item != nil {
			out.Messages = append(out.Messages, item)
		}
	}
	for _, msg := range list.ChannelMessages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			out.Messages = append(out.Messages, item)
		}
	}
	for _, u := range list.Users {
		out.Users = append(out.Users, tgUser(u))
	}
	out.Chats = append(out.Chats, tgChannelsForDialogs(viewerUserID, list.Channels, list.Dialogs)...)
	return out
}

func tgUpdatesDifference(diff domain.UpdateDifference) tg.UpdatesDifferenceClass {
	out := &tg.UpdatesDifference{
		NewMessages:  make([]tg.MessageClass, 0, len(diff.Events)),
		OtherUpdates: make([]tg.UpdateClass, 0, len(diff.Events)),
		Users:        make([]tg.UserClass, 0, 1),
		Chats:        make([]tg.ChatClass, 0, 1),
	}
	seenUsers := make(map[int64]struct{})
	seenChats := make(map[int64]struct{})
	for _, event := range diff.Events {
		addUsers(out, seenUsers, event.Users)
		addChannels(out, seenChats, event.UserID, event.Channels)
		switch event.Type {
		case domain.UpdateEventNewMessage:
			if msg := tgMessage(event.Message); msg != nil {
				out.NewMessages = append(out.NewMessages, msg)
				addMessageUsers(out, seenUsers, event.Message)
			}
		case domain.UpdateEventReadHistoryInbox:
			if update := tgReadHistoryInboxUpdate(event); update != nil {
				out.OtherUpdates = append(out.OtherUpdates, update)
			}
		case domain.UpdateEventReadHistoryOutbox:
			if update := tgReadHistoryOutboxUpdate(event); update != nil {
				out.OtherUpdates = append(out.OtherUpdates, update)
			}
		case domain.UpdateEventMessageReactions:
			if msg := tgMessage(event.Message); msg != nil {
				out.NewMessages = append(out.NewMessages, msg)
				addMessageUsers(out, seenUsers, event.Message)
			}
			if update := tgOtherUpdateFromEvent(event); update != nil {
				out.OtherUpdates = append(out.OtherUpdates, update)
			}
		default:
			if update := tgOtherUpdateFromEvent(event); update != nil {
				out.OtherUpdates = append(out.OtherUpdates, update)
			}
		}
	}
	// Partial：连续事件被 limit 截断、后面还有 → updates.differenceSlice，客户端据 IntermediateState 续拉。
	if diff.Partial {
		return &tg.UpdatesDifferenceSlice{
			NewMessages:       out.NewMessages,
			OtherUpdates:      out.OtherUpdates,
			Chats:             out.Chats,
			Users:             out.Users,
			IntermediateState: tgUpdateState(diff.State),
		}
	}
	out.State = tgUpdateState(diff.State)
	return out
}

func tgChannelDifference(viewerUserID int64, diff domain.ChannelDifference) tg.UpdatesChannelDifferenceClass {
	if diff.TooLong {
		messages := make([]tg.MessageClass, 0, len(diff.NewMessages))
		for _, msg := range diff.NewMessages {
			if item := tgChannelMessage(viewerUserID, msg); item != nil {
				messages = append(messages, item)
			}
		}
		dialog := tgDialog(domain.Dialog{
			Peer:            domain.Peer{Type: domain.PeerTypeChannel, ID: diff.Channel.ID},
			TopMessage:      diff.Dialog.TopMessageID,
			TopMessageDate:  diff.Dialog.TopMessageDate,
			ReadInboxMaxID:  diff.Dialog.ReadInboxMaxID,
			ReadOutboxMaxID: diff.Dialog.ReadOutboxMaxID,
			UnreadCount:     diff.Dialog.UnreadCount,
			UnreadMark:      diff.Dialog.UnreadMark,
			FolderID:        diff.Dialog.FolderID,
		})
		if dialog != nil {
			dialog.SetPts(diff.Pts)
		}
		return &tg.UpdatesChannelDifferenceTooLong{
			Final:    diff.Final,
			Timeout:  diff.Timeout,
			Dialog:   dialog,
			Messages: messages,
			Chats:    tgChannelDifferenceChats(viewerUserID, diff),
			Users:    tgUsers(diff.Users),
		}
	}
	if len(diff.Events) == 0 && len(diff.NewMessages) == 0 && len(diff.OtherUpdates) == 0 {
		return &tg.UpdatesChannelDifferenceEmpty{
			Final:   diff.Final,
			Pts:     diff.Pts,
			Timeout: diff.Timeout,
		}
	}
	messages := make([]tg.MessageClass, 0, len(diff.NewMessages))
	for _, msg := range diff.NewMessages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			messages = append(messages, item)
		}
	}
	updates := make([]tg.UpdateClass, 0, len(diff.OtherUpdates))
	for _, event := range diff.OtherUpdates {
		if update := tgChannelUpdate(viewerUserID, event); update != nil {
			updates = append(updates, update)
		}
	}
	chats := tgChannelDifferenceChats(viewerUserID, diff)
	users := tgUsers(diff.Users)
	return &tg.UpdatesChannelDifference{
		Final:        diff.Final,
		Pts:          diff.Pts,
		Timeout:      diff.Timeout,
		NewMessages:  messages,
		OtherUpdates: updates,
		Chats:        chats,
		Users:        users,
	}
}

func tgChannelDifferenceChats(viewerUserID int64, diff domain.ChannelDifference) []tg.ChatClass {
	return tgChannelChatsWithPrimarySelf(viewerUserID, diff.Channel, diff.Channels, diff.Self)
}

func tgChannelChatsWithPrimarySelf(viewerUserID int64, primary domain.Channel, extras []domain.Channel, primarySelf domain.ChannelMember) []tg.ChatClass {
	channels := make([]domain.Channel, 0, len(extras)+1)
	channels = append(channels, primary)
	channels = append(channels, extras...)
	out := make([]tg.ChatClass, 0, len(channels))
	seen := make(map[int64]struct{}, len(channels))
	for _, ch := range channels {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		var self *domain.ChannelMember
		if ch.ID == primary.ID && primarySelf.ChannelID == ch.ID && primarySelf.UserID == viewerUserID {
			self = &primarySelf
		}
		out = append(out, tgChannelChat(viewerUserID, ch, self))
	}
	return out
}

func tgChannelUpdate(viewerUserID int64, event domain.ChannelUpdateEvent) tg.UpdateClass {
	switch event.Type {
	case domain.ChannelUpdateNewMessage:
		msg := tgChannelMessage(viewerUserID, event.Message)
		if msg == nil {
			return nil
		}
		return &tg.UpdateNewChannelMessage{Message: msg, Pts: event.Pts, PtsCount: event.PtsCount}
	case domain.ChannelUpdateEditMessage:
		msg := tgChannelMessage(viewerUserID, event.Message)
		if msg == nil {
			return nil
		}
		return &tg.UpdateEditChannelMessage{Message: msg, Pts: event.Pts, PtsCount: event.PtsCount}
	case domain.ChannelUpdateDeleteMessages:
		if len(event.MessageIDs) == 0 {
			return nil
		}
		return &tg.UpdateDeleteChannelMessages{
			ChannelID: event.ChannelID,
			Messages:  append([]int(nil), event.MessageIDs...),
			Pts:       event.Pts,
			PtsCount:  event.PtsCount,
		}
	case domain.ChannelUpdatePinnedMessages:
		if len(event.MessageIDs) == 0 {
			return nil
		}
		return &tg.UpdatePinnedChannelMessages{
			Pinned:    event.Pinned,
			ChannelID: event.ChannelID,
			Messages:  append([]int(nil), event.MessageIDs...),
			Pts:       event.Pts,
			PtsCount:  event.PtsCount,
		}
	case domain.ChannelUpdateParticipant:
		userID := event.Participant.UserID
		if userID == 0 {
			userID = event.Previous.UserID
		}
		if userID == 0 {
			return nil
		}
		actorID := event.SenderUserID
		if actorID == 0 {
			actorID = userID
		}
		update := &tg.UpdateChannelParticipant{
			ChannelID: event.ChannelID,
			Date:      event.Date,
			ActorID:   actorID,
			UserID:    userID,
		}
		if event.Previous.UserID != 0 {
			update.SetPrevParticipant(tgChannelParticipantForUpdate(viewerUserID, event.Previous))
		}
		if event.Participant.UserID != 0 {
			update.SetNewParticipant(tgChannelParticipantForUpdate(viewerUserID, event.Participant))
		}
		return update
	default:
		return nil
	}
}

func tgOtherUpdateFromEvent(event domain.UpdateEvent) tg.UpdateClass {
	switch event.Type {
	case domain.UpdateEventContactsReset:
		return &tg.UpdateContactsReset{}
	case domain.UpdateEventDialogPinned:
		peer := tgDialogPeer(event.Peer)
		if peer == nil {
			return nil
		}
		return &tg.UpdateDialogPinned{Pinned: event.Bool, Peer: peer}
	case domain.UpdateEventPinnedDialogs:
		update := &tg.UpdatePinnedDialogs{}
		if len(event.Peers) > 0 {
			update.Order = tgDialogPeers(event.Peers)
		}
		return update
	case domain.UpdateEventDialogUnreadMark:
		peer := tgDialogPeer(event.Peer)
		if peer == nil {
			return nil
		}
		return &tg.UpdateDialogUnreadMark{Unread: event.Bool, Peer: peer}
	case domain.UpdateEventChannelViewForum:
		if event.Peer.Type != domain.PeerTypeChannel || event.Peer.ID == 0 {
			return nil
		}
		return &tg.UpdateChannelViewForumAsMessages{ChannelID: event.Peer.ID, Enabled: event.Bool}
	case domain.UpdateEventPeerSettings:
		peer := tgPeer(event.Peer)
		if peer == nil {
			return nil
		}
		return &tg.UpdatePeerSettings{Peer: peer, Settings: tgPeerSettings(event.Settings)}
	case domain.UpdateEventDeleteMessages:
		if len(event.MessageIDs) == 0 {
			return nil
		}
		return &tg.UpdateDeleteMessages{
			Messages: append([]int(nil), event.MessageIDs...),
			Pts:      event.Pts,
			PtsCount: event.PtsCount,
		}
	case domain.UpdateEventReadHistoryOutbox:
		return tgReadHistoryOutbox(event)
	case domain.UpdateEventEditMessage:
		msg := tgMessage(event.Message)
		if msg == nil {
			return nil
		}
		return &tg.UpdateEditMessage{
			Message:  msg,
			Pts:      event.Pts,
			PtsCount: event.PtsCount,
		}
	case domain.UpdateEventMessageReactions:
		if event.Message.ID <= 0 || event.Message.ID > domain.MaxMessageBoxID {
			return nil
		}
		peer := event.Message.Peer
		if peer.Type == "" || peer.ID == 0 {
			peer = event.Peer
		}
		outPeer := tgPeer(peer)
		if outPeer == nil {
			return nil
		}
		reactions := event.Message.Reactions
		if reactions == nil {
			empty := domain.ChannelMessageReactions{CanSeeList: true, Results: []domain.ChannelMessageReactionCount{}, Recent: []domain.ChannelMessagePeerReaction{}}
			reactions = &empty
		}
		converted := tgMessageReactions(event.UserID, reactions)
		if converted == nil {
			converted = &tg.MessageReactions{Results: []tg.ReactionCount{}}
		}
		return &tg.UpdateMessageReactions{
			Peer:      outPeer,
			MsgID:     event.Message.ID,
			Reactions: *converted,
		}
	case domain.UpdateEventDialogFilter:
		update := &tg.UpdateDialogFilter{ID: event.FilterID}
		if event.DialogFilter != nil {
			update.SetFilter(tgDialogFilter(*event.DialogFilter))
		}
		return update
	case domain.UpdateEventDialogFilterOrder:
		return &tg.UpdateDialogFilterOrder{Order: append([]int(nil), event.FilterOrder...)}
	case domain.UpdateEventDialogFilters:
		return &tg.UpdateDialogFilters{}
	case domain.UpdateEventFolderPeers:
		if len(event.FolderPeers) == 0 {
			return nil
		}
		return &tg.UpdateFolderPeers{
			FolderPeers: tgFolderPeers(event.FolderPeers),
			Pts:         event.Pts,
			PtsCount:    event.PtsCount,
		}
	case domain.UpdateEventChannelAvailable:
		if event.Peer.Type != domain.PeerTypeChannel || event.Peer.ID == 0 || event.MaxID <= 0 {
			return nil
		}
		return &tg.UpdateChannelAvailableMessages{
			ChannelID:      event.Peer.ID,
			AvailableMinID: event.MaxID,
		}
	default:
		return nil
	}
}

func addUsers(out *tg.UpdatesDifference, seen map[int64]struct{}, users []domain.User) {
	for _, u := range users {
		if u.ID == 0 {
			continue
		}
		if _, ok := seen[u.ID]; ok {
			continue
		}
		seen[u.ID] = struct{}{}
		out.Users = append(out.Users, tgUser(u))
	}
}

func addChannels(out *tg.UpdatesDifference, seen map[int64]struct{}, viewerUserID int64, channels []domain.Channel) {
	for _, ch := range channels {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		out.Chats = append(out.Chats, tgChannelChat(viewerUserID, ch, nil))
	}
}

func addMessageUsers(out *tg.UpdatesDifference, seen map[int64]struct{}, msg domain.Message) {
	for _, peer := range []domain.Peer{msg.From, msg.Peer} {
		if peer.Type != domain.PeerTypeUser {
			continue
		}
		if _, ok := seen[peer.ID]; ok {
			continue
		}
		seen[peer.ID] = struct{}{}
		switch peer.ID {
		case domain.OfficialSystemUserID:
			out.Users = append(out.Users, tgUser(domain.OfficialSystemUser()))
		}
	}
}

func tgDialog(d domain.Dialog) *tg.Dialog {
	peer := tgPeer(d.Peer)
	if peer == nil {
		return nil
	}
	out := &tg.Dialog{
		Pinned:               d.Pinned,
		UnreadMark:           d.UnreadMark,
		ViewForumAsMessages:  d.ViewForumAsMessages,
		Peer:                 peer,
		TopMessage:           d.TopMessage,
		ReadInboxMaxID:       d.ReadInboxMaxID,
		ReadOutboxMaxID:      d.ReadOutboxMaxID,
		UnreadCount:          d.UnreadCount,
		UnreadMentionsCount:  d.UnreadMentions,
		UnreadReactionsCount: d.UnreadReactions,
		NotifySettings:       *tdesktop.NotifySettings(),
	}
	if d.FolderID != domain.DialogMainFolderID {
		out.SetFolderID(d.FolderID)
	}
	if d.Draft != nil {
		out.SetDraft(tgDialogDraft(*d.Draft))
	}
	return out
}

func tgDialogDraft(d domain.DialogDraft) tg.DraftMessageClass {
	if d.Empty() {
		out := &tg.DraftMessageEmpty{}
		out.SetDate(d.Date)
		return out
	}
	out := &tg.DraftMessage{
		NoWebpage:   d.NoWebpage,
		InvertMedia: d.InvertMedia,
		ReplyTo:     tgDraftReplyTo(d),
		Message:     d.Message,
		Entities:    tgMessageEntities(d.Entities),
		Media:       tgDraftWebPage(d.WebPage),
		Date:        d.Date,
		Effect:      d.Effect,
	}
	return out
}

func tgDraftReplyTo(d domain.DialogDraft) tg.InputReplyToClass {
	if d.ReplyTo == nil {
		return nil
	}
	reply := &tg.InputReplyToMessage{ReplyToMsgID: d.ReplyTo.MessageID}
	if d.ReplyTo.TopMessageID > 0 {
		reply.SetTopMsgID(d.ReplyTo.TopMessageID)
	}
	if d.ReplyTo.QuoteText != "" {
		reply.SetQuoteText(d.ReplyTo.QuoteText)
	}
	if len(d.ReplyTo.QuoteEntities) > 0 {
		reply.SetQuoteEntities(tgMessageEntities(d.ReplyTo.QuoteEntities))
	}
	if d.ReplyTo.QuoteOffset > 0 {
		reply.SetQuoteOffset(d.ReplyTo.QuoteOffset)
	}
	return reply
}

func tgDraftWebPage(webpage *domain.DialogDraftWebPage) tg.InputMediaClass {
	if webpage == nil || webpage.URL == "" {
		return nil
	}
	return &tg.InputMediaWebPage{
		ForceLargeMedia: webpage.ForceLargeMedia,
		ForceSmallMedia: webpage.ForceSmallMedia,
		Optional:        webpage.Optional,
		URL:             webpage.URL,
	}
}

func tgDialogPeer(p domain.Peer) tg.DialogPeerClass {
	peer := tgPeer(p)
	if peer == nil {
		return nil
	}
	return &tg.DialogPeer{Peer: peer}
}

func tgDialogPeers(peers []domain.Peer) []tg.DialogPeerClass {
	out := make([]tg.DialogPeerClass, 0, len(peers))
	for _, peer := range peers {
		item := tgDialogPeer(peer)
		if item != nil {
			out = append(out, item)
		}
	}
	return out
}

func tgDialogFilters(list domain.DialogFolderList) *tg.MessagesDialogFilters {
	filters := make([]tg.DialogFilterClass, 0, len(list.Folders)+1)
	filters = append(filters, &tg.DialogFilterDefault{})
	for _, folder := range list.Folders {
		if item := tgDialogFilter(folder); item != nil {
			filters = append(filters, item)
		}
	}
	return &tg.MessagesDialogFilters{TagsEnabled: list.TagsEnabled, Filters: filters}
}

func tgDialogFilter(folder domain.DialogFolder) tg.DialogFilterClass {
	title := tg.TextWithEntities{Text: folder.Title, Entities: tgMessageEntities(folder.TitleEntities)}
	if folder.IsChatlist {
		out := &tg.DialogFilterChatlist{
			TitleNoanimate: folder.TitleNoanimate,
			ID:             folder.ID,
			Title:          title,
			PinnedPeers:    tgDialogFolderInputPeers(folder.PinnedPeers),
			IncludePeers:   tgDialogFolderInputPeers(folder.IncludePeers),
		}
		if folder.HasEmoticon {
			out.SetEmoticon(folder.Emoticon)
		}
		if folder.HasColor {
			out.SetColor(folder.Color)
		}
		return out
	}
	out := &tg.DialogFilter{
		Contacts:        folder.Contacts,
		NonContacts:     folder.NonContacts,
		Groups:          folder.Groups,
		Broadcasts:      folder.Broadcasts,
		Bots:            folder.Bots,
		ExcludeMuted:    folder.ExcludeMuted,
		ExcludeRead:     folder.ExcludeRead,
		ExcludeArchived: folder.ExcludeArchived,
		TitleNoanimate:  folder.TitleNoanimate,
		ID:              folder.ID,
		Title:           title,
		PinnedPeers:     tgDialogFolderInputPeers(folder.PinnedPeers),
		IncludePeers:    tgDialogFolderInputPeers(folder.IncludePeers),
		ExcludePeers:    tgDialogFolderInputPeers(folder.ExcludePeers),
	}
	if folder.HasEmoticon {
		out.SetEmoticon(folder.Emoticon)
	}
	if folder.HasColor {
		out.SetColor(folder.Color)
	}
	return out
}

func tgDialogFolderInputPeers(peers []domain.DialogFolderPeer) []tg.InputPeerClass {
	if len(peers) == 0 {
		return nil
	}
	out := make([]tg.InputPeerClass, 0, len(peers))
	for _, item := range peers {
		switch item.Peer.Type {
		case domain.PeerTypeUser:
			out = append(out, &tg.InputPeerUser{UserID: item.Peer.ID, AccessHash: item.AccessHash})
		case domain.PeerTypeChannel:
			out = append(out, &tg.InputPeerChannel{ChannelID: item.Peer.ID, AccessHash: item.AccessHash})
		}
	}
	return out
}

func tgFolderPeers(peers []domain.FolderPeerUpdate) []tg.FolderPeer {
	out := make([]tg.FolderPeer, 0, len(peers))
	for _, item := range peers {
		peer := tgPeer(item.Peer)
		if peer == nil {
			continue
		}
		out = append(out, tg.FolderPeer{Peer: peer, FolderID: item.FolderID})
	}
	return out
}

func tgMessage(m domain.Message) tg.MessageClass {
	peer := tgPeer(m.Peer)
	if peer == nil || m.ID == 0 {
		return nil
	}
	msg := &tg.Message{
		Out:      m.Out,
		ID:       m.ID,
		PeerID:   peer,
		Date:     m.Date,
		Message:  m.Body,
		Entities: tgMessageEntities(m.Entities),
	}
	if m.EditDate != 0 {
		msg.SetEditDate(m.EditDate)
	}
	if m.Silent {
		msg.SetSilent(true)
	}
	if m.NoForwards {
		msg.SetNoforwards(true)
	}
	if reply := tgMessageReplyHeader(m); reply != nil {
		msg.SetReplyTo(reply)
	}
	if fwd := tgMessageFwdHeader(m.Forward); fwd != nil {
		msg.SetFwdFrom(*fwd)
	}
	if from := tgPeer(m.From); from != nil {
		msg.FromID = from
	}
	if !m.Media.IsZero() {
		msg.SetMedia(tgMessageMedia(m.Media))
	}
	if reactions := tgMessageReactions(m.OwnerUserID, m.Reactions); reactions != nil {
		msg.SetReactions(*reactions)
	}
	return msg
}

func tgChannelHistoryMessages(viewerUserID int64, list domain.ChannelHistory) tg.MessagesMessagesClass {
	messages := make([]tg.MessageClass, 0, len(list.Messages))
	for _, msg := range list.Messages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			messages = append(messages, item)
		}
	}
	topics := make([]tg.ForumTopicClass, 0, len(list.Topics))
	for _, topic := range list.Topics {
		item := tgForumTopicFromDomain(viewerUserID, topic)
		item.SetShort(true)
		topics = append(topics, item)
	}
	users := tgUsers(list.Users)
	chats := tgChannelChatsWithPrimarySelf(viewerUserID, list.Channel, list.Channels, list.Self)
	if list.Channel.ID != 0 {
		return &tg.MessagesChannelMessages{
			Pts:      list.Channel.Pts,
			Count:    list.Count,
			Messages: messages,
			Topics:   topics,
			Chats:    chats,
			Users:    users,
		}
	}
	if list.Count > len(messages) {
		return &tg.MessagesMessagesSlice{
			Count:    list.Count,
			Messages: messages,
			Topics:   topics,
			Chats:    chats,
			Users:    users,
		}
	}
	return &tg.MessagesMessages{Messages: messages, Topics: topics, Chats: chats, Users: users}
}

func tgMessagesDiscussionMessage(viewerUserID int64, in domain.ChannelDiscussionMessage) *tg.MessagesDiscussionMessage {
	messages := make([]tg.MessageClass, 0, len(in.Messages))
	for _, msg := range in.Messages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			messages = append(messages, item)
		}
	}
	channels := make([]domain.Channel, 0, 2+len(in.Channels))
	if in.PostChannel.ID != 0 {
		channels = append(channels, in.PostChannel)
	}
	if in.DiscussionChannel.ID != 0 && in.DiscussionChannel.ID != in.PostChannel.ID {
		channels = append(channels, in.DiscussionChannel)
	}
	channels = append(channels, in.Channels...)
	out := &tg.MessagesDiscussionMessage{
		Messages:    messages,
		Chats:       tgChannels(viewerUserID, channels),
		Users:       tgUsers(in.Users),
		UnreadCount: in.UnreadCount,
	}
	if in.MaxID > 0 {
		out.SetMaxID(in.MaxID)
	}
	if in.ReadInboxMaxID > 0 {
		out.SetReadInboxMaxID(in.ReadInboxMaxID)
	}
	if in.ReadOutboxMaxID > 0 {
		out.SetReadOutboxMaxID(in.ReadOutboxMaxID)
	}
	return out
}

func tgChannelMessage(viewerUserID int64, m domain.ChannelMessage) tg.MessageClass {
	if m.ID == 0 || m.ChannelID == 0 {
		return nil
	}
	peer := &tg.PeerChannel{ChannelID: m.ChannelID}
	outgoing := m.SenderUserID == viewerUserID && viewerUserID != 0 && m.From.Type != domain.PeerTypeChannel
	from := tg.PeerClass(nil)
	if !m.Post && m.SendAs != nil && m.SendAs.ID != 0 {
		from = tgPeer(*m.SendAs)
	}
	if !m.Post && from == nil && m.From.ID != 0 {
		from = tgPeer(m.From)
	}
	if from == nil && m.SenderUserID != 0 && !m.Post {
		from = &tg.PeerUser{UserID: m.SenderUserID}
	}
	if m.Action != nil {
		msg := &tg.MessageService{
			Out:    outgoing,
			Silent: m.Silent,
			Post:   m.Post,
			ID:     m.ID,
			FromID: from,
			PeerID: peer,
			Date:   m.Date,
			Action: tgChannelMessageAction(*m.Action),
		}
		if msg.Action == nil {
			msg.Action = &tg.MessageActionEmpty{}
		}
		return msg
	}
	msg := &tg.Message{
		Out:        outgoing,
		Silent:     m.Silent,
		Post:       m.Post,
		Noforwards: m.NoForwards,
		ID:         m.ID,
		FromID:     from,
		PeerID:     peer,
		Date:       m.Date,
		Message:    m.Body,
		Entities:   tgMessageEntities(m.Entities),
	}
	if m.EditDate != 0 {
		msg.SetEditDate(m.EditDate)
	}
	if reply := tgMessageReplyHeader(domain.Message{
		Peer:    domain.Peer{Type: domain.PeerTypeChannel, ID: m.ChannelID},
		ReplyTo: m.ReplyTo,
	}); reply != nil {
		msg.SetReplyTo(reply)
	}
	if fwd := tgMessageFwdHeader(m.Forward); fwd != nil {
		msg.SetFwdFrom(*fwd)
	}
	if replies := tgChannelMessageReplies(m.Replies); replies != nil {
		msg.SetReplies(*replies)
	}
	if reactions := tgMessageReactions(viewerUserID, m.Reactions); reactions != nil {
		msg.SetReactions(*reactions)
	}
	if !m.Media.IsZero() {
		msg.SetMedia(tgMessageMedia(m.Media))
	}
	return msg
}

func tgChannelMessageAction(action domain.ChannelMessageAction) tg.MessageActionClass {
	switch action.Type {
	case domain.ChannelActionCreate:
		return &tg.MessageActionChannelCreate{Title: action.Title}
	case domain.ChannelActionChatAddUser, domain.ChannelActionChatJoined:
		return &tg.MessageActionChatAddUser{Users: append([]int64(nil), action.UserIDs...)}
	case domain.ChannelActionChatDelete:
		var userID int64
		if len(action.UserIDs) > 0 {
			userID = action.UserIDs[0]
		}
		return &tg.MessageActionChatDeleteUser{UserID: userID}
	case domain.ChannelActionEditTitle:
		return &tg.MessageActionChatEditTitle{Title: action.Title}
	case domain.ChannelActionTopicCreate:
		return &tg.MessageActionTopicCreate{
			TitleMissing: action.TitleMissing,
			Title:        action.Title,
			IconColor:    action.IconColor,
			IconEmojiID:  action.IconEmojiID,
		}
	case domain.ChannelActionTopicEdit:
		out := &tg.MessageActionTopicEdit{}
		if action.Title != "" {
			out.SetTitle(action.Title)
		}
		if action.IconEmojiIDSet {
			out.SetIconEmojiID(action.IconEmojiID)
		}
		if action.Closed != nil {
			out.SetClosed(*action.Closed)
		}
		if action.Hidden != nil {
			out.SetHidden(*action.Hidden)
		}
		return out
	default:
		return nil
	}
}

func tgMessageReplyHeader(m domain.Message) tg.MessageReplyHeaderClass {
	if m.ReplyTo == nil || (m.ReplyTo.MessageID <= 0 && m.ReplyTo.TopMessageID <= 0) {
		return nil
	}
	header := &tg.MessageReplyHeader{}
	if m.ReplyTo.ForumTopic {
		header.SetForumTopic(true)
	}
	if m.ReplyTo.MessageID > 0 {
		header.SetReplyToMsgID(m.ReplyTo.MessageID)
	}
	if m.ReplyTo.TopMessageID > 0 {
		header.SetReplyToTopID(m.ReplyTo.TopMessageID)
	}
	if m.ReplyTo.Peer.ID != 0 && m.ReplyTo.Peer != m.Peer {
		if peer := tgPeer(m.ReplyTo.Peer); peer != nil {
			header.SetReplyToPeerID(peer)
		}
	}
	if m.ReplyTo.QuoteText != "" {
		header.SetQuote(true)
		header.SetQuoteText(m.ReplyTo.QuoteText)
		header.SetQuoteEntities(tgMessageEntities(m.ReplyTo.QuoteEntities))
		header.SetQuoteOffset(m.ReplyTo.QuoteOffset)
	}
	return header
}

func tgMessageFwdHeader(fwd *domain.MessageForward) *tg.MessageFwdHeader {
	if fwd == nil || (fwd.Date == 0 && fwd.From.ID == 0 && fwd.FromName == "" && fwd.ChannelPost == 0 && fwd.SavedFrom.ID == 0 && fwd.SavedFromMsgID == 0) {
		return nil
	}
	header := &tg.MessageFwdHeader{Date: fwd.Date}
	if peer := tgPeer(fwd.From); peer != nil {
		header.SetFromID(peer)
	}
	if fwd.FromName != "" {
		header.SetFromName(fwd.FromName)
	}
	if fwd.ChannelPost > 0 {
		header.SetChannelPost(fwd.ChannelPost)
	}
	if peer := tgPeer(fwd.SavedFrom); peer != nil {
		header.SetSavedFromPeer(peer)
	}
	if fwd.SavedFromMsgID > 0 {
		header.SetSavedFromMsgID(fwd.SavedFromMsgID)
	}
	return header
}

func tgChannelMessageReplies(in *domain.ChannelMessageReplies) *tg.MessageReplies {
	if in == nil {
		return nil
	}
	out := &tg.MessageReplies{
		Comments:   in.Comments,
		Replies:    in.Replies,
		RepliesPts: in.RepliesPts,
	}
	if len(in.RecentRepliers) > 0 {
		repliers := make([]tg.PeerClass, 0, len(in.RecentRepliers))
		for _, peer := range in.RecentRepliers {
			if converted := tgPeer(peer); converted != nil {
				repliers = append(repliers, converted)
			}
		}
		if len(repliers) > 0 {
			out.SetRecentRepliers(repliers)
		}
	}
	if in.ChannelID != 0 {
		out.SetChannelID(in.ChannelID)
	}
	if in.MaxID > 0 {
		out.SetMaxID(in.MaxID)
	}
	if in.ReadMaxID > 0 {
		out.SetReadMaxID(in.ReadMaxID)
	}
	return out
}

func tgMessageReactions(viewerUserID int64, in *domain.ChannelMessageReactions) *tg.MessageReactions {
	if in == nil {
		return nil
	}
	out := &tg.MessageReactions{
		Results: make([]tg.ReactionCount, 0, len(in.Results)),
	}
	if in.CanSeeList {
		out.SetCanSeeList(true)
	}
	for _, item := range in.Results {
		reaction := tgMessageReaction(item.Reaction)
		if reaction == nil || item.Count <= 0 {
			continue
		}
		count := tg.ReactionCount{
			Reaction: reaction,
			Count:    item.Count,
		}
		if item.ChosenOrder > 0 {
			count.SetChosenOrder(item.ChosenOrder)
		}
		out.Results = append(out.Results, count)
	}
	if len(in.Recent) > 0 {
		recent := make([]tg.MessagePeerReaction, 0, len(in.Recent))
		for _, item := range in.Recent {
			if converted := tgMessagePeerReaction(viewerUserID, item); converted != nil {
				recent = append(recent, *converted)
			}
		}
		if len(recent) > 0 {
			out.SetRecentReactions(recent)
		}
	}
	if out.Results == nil {
		out.Results = []tg.ReactionCount{}
	}
	return out
}

func tgMessagePeerReaction(viewerUserID int64, in domain.ChannelMessagePeerReaction) *tg.MessagePeerReaction {
	reaction := tgMessageReaction(in.Reaction)
	if reaction == nil || in.UserID == 0 {
		return nil
	}
	out := &tg.MessagePeerReaction{
		PeerID:   &tg.PeerUser{UserID: in.UserID},
		Date:     in.Date,
		Reaction: reaction,
	}
	if in.Big {
		out.SetBig(true)
	}
	if in.Unread && in.SenderUserID == viewerUserID {
		out.SetUnread(true)
	}
	if in.My || in.UserID == viewerUserID {
		out.SetMy(true)
	}
	return out
}

func tgMessageReaction(in domain.MessageReaction) tg.ReactionClass {
	switch in.Type {
	case domain.MessageReactionEmoji:
		if in.Emoticon == "" {
			return nil
		}
		return &tg.ReactionEmoji{Emoticon: in.Emoticon}
	default:
		return nil
	}
}

func tgMessageEntities(entities []domain.MessageEntity) []tg.MessageEntityClass {
	if len(entities) == 0 {
		return nil
	}
	out := make([]tg.MessageEntityClass, 0, len(entities))
	for _, entity := range entities {
		switch entity.Type {
		case domain.MessageEntityBold:
			out = append(out, &tg.MessageEntityBold{Offset: entity.Offset, Length: entity.Length})
		}
	}
	return out
}

func domainMessageEntities(entities []tg.MessageEntityClass) []domain.MessageEntity {
	if len(entities) == 0 {
		return nil
	}
	out := make([]domain.MessageEntity, 0, len(entities))
	for _, entity := range entities {
		switch e := entity.(type) {
		case *tg.MessageEntityBold:
			out = append(out, domain.MessageEntity{
				Type:   domain.MessageEntityBold,
				Offset: e.Offset,
				Length: e.Length,
			})
		}
	}
	return out
}

func tgPeer(p domain.Peer) tg.PeerClass {
	switch p.Type {
	case domain.PeerTypeUser:
		return &tg.PeerUser{UserID: p.ID}
	case domain.PeerTypeChannel:
		return &tg.PeerChannel{ChannelID: p.ID}
	default:
		return nil
	}
}

func tgChannels(viewerUserID int64, channels []domain.Channel) []tg.ChatClass {
	out := make([]tg.ChatClass, 0, len(channels))
	seen := make(map[int64]struct{}, len(channels))
	for _, ch := range channels {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		out = append(out, tgChannelChat(viewerUserID, ch, nil))
	}
	return out
}

func tgChannelsForDialogs(viewerUserID int64, channels []domain.Channel, dialogs []domain.Dialog) []tg.ChatClass {
	left := make(map[int64]struct{})
	for _, dialog := range dialogs {
		if dialog.Peer.Type == domain.PeerTypeChannel && dialog.Peer.ID != 0 && dialog.ChannelLeft {
			left[dialog.Peer.ID] = struct{}{}
		}
	}
	out := make([]tg.ChatClass, 0, len(channels))
	seen := make(map[int64]struct{}, len(channels))
	for _, ch := range channels {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		var self *domain.ChannelMember
		if _, ok := left[ch.ID]; ok {
			self = &domain.ChannelMember{
				ChannelID: ch.ID,
				UserID:    viewerUserID,
				Status:    domain.ChannelMemberLeft,
			}
		}
		out = append(out, tgChannelChat(viewerUserID, ch, self))
	}
	return out
}

func tgChannelChat(viewerUserID int64, ch domain.Channel, self *domain.ChannelMember) tg.ChatClass {
	if ch.Deleted {
		return tgChannelForbidden(ch)
	}
	return tgChannel(viewerUserID, ch, self)
}

func tgMigratedLegacyChat(viewerUserID int64, ch domain.Channel, self *domain.ChannelMember) *tg.Chat {
	if ch.ID == 0 || ch.Deleted {
		return nil
	}
	out := &tg.Chat{
		Creator:           ch.CreatorUserID == viewerUserID && viewerUserID != 0,
		Deactivated:       true,
		Noforwards:        ch.NoForwards,
		ID:                ch.ID,
		Title:             ch.Title,
		Photo:             tgChatPhoto(ch),
		ParticipantsCount: ch.ParticipantsCount,
		Date:              ch.Date,
		Version:           ch.Pts,
	}
	out.SetMigratedTo(&tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash})
	if self != nil {
		if self.Status == domain.ChannelMemberLeft {
			out.Left = true
		}
		switch self.Role {
		case domain.ChannelRoleCreator:
			out.Creator = true
			out.SetAdminRights(tgChatAdminRights(self.AdminRights))
		case domain.ChannelRoleAdmin:
			out.SetAdminRights(tgChatAdminRights(self.AdminRights))
		}
	}
	if !zeroChannelBannedRights(ch.DefaultBannedRights) {
		out.SetDefaultBannedRights(tgChatBannedRights(ch.DefaultBannedRights))
	}
	return out
}

func tgChannelForbidden(ch domain.Channel) *tg.ChannelForbidden {
	return &tg.ChannelForbidden{
		Broadcast:  ch.Broadcast,
		Megagroup:  ch.Megagroup,
		ID:         ch.ID,
		AccessHash: ch.AccessHash,
		Title:      ch.Title,
	}
}

func tgChannel(viewerUserID int64, ch domain.Channel, self *domain.ChannelMember) *tg.Channel {
	out := &tg.Channel{
		Creator:    ch.CreatorUserID == viewerUserID && viewerUserID != 0,
		Broadcast:  ch.Broadcast,
		Megagroup:  ch.Megagroup,
		Forum:      ch.Forum,
		ForumTabs:  ch.ForumTabs,
		Noforwards: ch.NoForwards,
		Signatures: ch.Signatures,
		ID:         ch.ID,
		Title:      ch.Title,
		Photo:      tgChatPhoto(ch),
		Date:       ch.Date,
	}
	out.SetAccessHash(ch.AccessHash)
	out.SetJoinToSend(ch.JoinToSend)
	out.SetJoinRequest(ch.JoinRequest)
	out.SetAutotranslation(ch.Autotranslation)
	out.SetBroadcastMessagesAllowed(ch.BroadcastMessagesAllowed)
	if ch.SendPaidMessagesStars > 0 || ch.BroadcastMessagesAllowed {
		out.SetSendPaidMessagesStars(ch.SendPaidMessagesStars)
	}
	if ch.LinkedChatID != 0 {
		out.SetHasLink(true)
	}
	if ch.Username != "" {
		out.SetUsername(ch.Username)
		out.SetUsernames(tgUsernames(ch.Username))
	}
	if color := tgPeerColor(ch.Color); color != nil {
		out.SetColor(color)
	}
	if profileColor := tgPeerColor(ch.ProfileColor); profileColor != nil {
		out.SetProfileColor(profileColor)
	}
	if status := tgChannelEmojiStatus(ch.EmojiStatus); status != nil {
		out.SetEmojiStatus(status)
	}
	if ch.SlowmodeSeconds > 0 {
		out.SetSlowmodeEnabled(true)
	}
	if ch.ParticipantsCount > 0 {
		out.SetParticipantsCount(ch.ParticipantsCount)
	}
	if !zeroChannelBannedRights(ch.DefaultBannedRights) {
		out.SetDefaultBannedRights(tgChatBannedRights(ch.DefaultBannedRights))
	}
	if self != nil {
		if self.Status == domain.ChannelMemberLeft {
			out.Left = true
		}
		switch self.Role {
		case domain.ChannelRoleCreator:
			out.Creator = true
			out.SetAdminRights(tgChatAdminRights(self.AdminRights))
		case domain.ChannelRoleAdmin:
			out.SetAdminRights(tgChatAdminRights(self.AdminRights))
		}
		if !zeroChannelBannedRights(self.BannedRights) {
			out.SetBannedRights(tgChatBannedRights(self.BannedRights))
		}
	}
	return out
}

func tgChannelFull(view domain.ChannelView) *tg.ChannelFull {
	ch := view.Channel
	full := &tg.ChannelFull{
		CanViewParticipants: !ch.ParticipantsHidden || channelMemberIsAdmin(view.Self),
		CanDeleteChannel:    view.Self.Role == domain.ChannelRoleCreator,
		ID:                  ch.ID,
		About:               ch.About,
		ReadInboxMaxID:      view.Dialog.ReadInboxMaxID,
		ReadOutboxMaxID:     view.Dialog.ReadOutboxMaxID,
		UnreadCount:         view.Dialog.UnreadCount,
		ChatPhoto:           tgChannelChatPhotoFull(ch),
		NotifySettings:      *tdesktop.NotifySettings(),
		Pts:                 ch.Pts,
	}
	if ch.ParticipantsCount > 0 {
		full.SetParticipantsCount(ch.ParticipantsCount)
	}
	if ch.AdminsCount > 0 {
		full.SetAdminsCount(ch.AdminsCount)
	}
	if ch.KickedCount > 0 {
		full.SetKickedCount(ch.KickedCount)
	}
	if ch.BannedCount > 0 {
		full.SetBannedCount(ch.BannedCount)
	}
	if view.Dialog.FolderID != domain.DialogMainFolderID {
		full.SetFolderID(view.Dialog.FolderID)
	}
	if ch.TTLPeriod > 0 {
		full.SetTTLPeriod(ch.TTLPeriod)
	}
	if ch.PreHistoryHidden {
		full.SetHiddenPrehistory(true)
	}
	if ch.ParticipantsHidden {
		full.SetParticipantsHidden(true)
	}
	if ch.AntiSpam {
		full.SetAntispam(true)
	}
	if ch.RestrictedSponsored {
		full.SetRestrictedSponsored(true)
	}
	if ch.SendPaidMessagesStars > 0 || ch.BroadcastMessagesAllowed {
		full.SetPaidMessagesAvailable(true)
		full.SetSendPaidMessagesStars(ch.SendPaidMessagesStars)
	}
	if view.Dialog.ViewForumAsMessages {
		full.SetViewForumAsMessages(true)
	}
	if ch.LinkedChatID != 0 {
		full.SetLinkedChatID(ch.LinkedChatID)
	}
	if defaultSendAs := validDefaultSendAsPeer(view); defaultSendAs != nil {
		full.SetDefaultSendAs(tgPeer(*defaultSendAs))
	}
	if ch.SlowmodeSeconds > 0 {
		full.SetSlowmodeSeconds(ch.SlowmodeSeconds)
		if view.Self.SlowmodeLastSendDate > 0 {
			full.SetSlowmodeNextSendDate(view.Self.SlowmodeLastSendDate + ch.SlowmodeSeconds)
		}
	}
	if ch.PinnedMessageID > 0 {
		full.SetPinnedMsgID(ch.PinnedMessageID)
	}
	if reactions := tgChannelReactionPolicy(ch.ReactionPolicy); reactions != nil {
		full.SetAvailableReactions(reactions)
	}
	if ch.ReactionPolicy.Limit > 0 {
		full.SetReactionsLimit(ch.ReactionPolicy.Limit)
	}
	if ch.ReactionPolicy.PaidEnabled {
		full.SetPaidReactionsAvailable(true)
	}
	return full
}

func channelMemberIsAdmin(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator || member.Role == domain.ChannelRoleAdmin
}

func tgChannelReactionPolicy(policy domain.ChannelReactionPolicy) tg.ChatReactionsClass {
	switch policy.Type {
	case domain.ChannelReactionPolicyNone:
		return &tg.ChatReactionsNone{}
	case domain.ChannelReactionPolicyAll:
		out := &tg.ChatReactionsAll{}
		if policy.AllowCustom {
			out.SetAllowCustom(true)
		}
		return out
	case domain.ChannelReactionPolicySome:
		reactions := make([]tg.ReactionClass, 0, len(policy.Emoticons)+len(policy.CustomEmojiIDs))
		for _, emoticon := range policy.Emoticons {
			if emoticon == "" {
				continue
			}
			reactions = append(reactions, &tg.ReactionEmoji{Emoticon: emoticon})
		}
		for _, id := range policy.CustomEmojiIDs {
			if id <= 0 {
				continue
			}
			reactions = append(reactions, &tg.ReactionCustomEmoji{DocumentID: id})
		}
		return &tg.ChatReactionsSome{Reactions: reactions}
	default:
		return nil
	}
}

func tgPeerColor(color domain.ChannelPeerColor) tg.PeerColorClass {
	if color.Empty() {
		return nil
	}
	out := &tg.PeerColor{}
	if color.HasColor {
		out.SetColor(color.Color)
	}
	if color.BackgroundEmojiID != 0 {
		out.SetBackgroundEmojiID(color.BackgroundEmojiID)
	}
	return out
}

func tgChannelEmojiStatus(status domain.ChannelEmojiStatus) tg.EmojiStatusClass {
	if status.Empty() {
		return nil
	}
	out := &tg.EmojiStatus{DocumentID: status.DocumentID}
	if status.Until > 0 {
		out.SetUntil(status.Until)
	}
	return out
}

func tgChannelParticipant(selfUserID int64, member domain.ChannelMember) tg.ChannelParticipantClass {
	switch member.Role {
	case domain.ChannelRoleCreator:
		out := &tg.ChannelParticipantCreator{
			UserID:      member.UserID,
			AdminRights: tgChatAdminRights(member.AdminRights),
		}
		if member.Rank != "" {
			out.SetRank(member.Rank)
		}
		return out
	case domain.ChannelRoleAdmin:
		out := &tg.ChannelParticipantAdmin{
			Self:        member.UserID == selfUserID,
			UserID:      member.UserID,
			PromotedBy:  member.InviterUserID,
			Date:        member.JoinedAt,
			AdminRights: tgChatAdminRights(member.AdminRights),
		}
		if member.InviterUserID != 0 {
			out.SetInviterID(member.InviterUserID)
		}
		if member.Rank != "" {
			out.SetRank(member.Rank)
		}
		return out
	default:
		if member.Status != domain.ChannelMemberActive {
			return &tg.ChannelParticipantLeft{Peer: &tg.PeerUser{UserID: member.UserID}}
		}
		if member.UserID == selfUserID {
			out := &tg.ChannelParticipantSelf{
				UserID:    member.UserID,
				InviterID: member.InviterUserID,
				Date:      member.JoinedAt,
			}
			if member.Rank != "" {
				out.SetRank(member.Rank)
			}
			return out
		}
		out := &tg.ChannelParticipant{UserID: member.UserID, Date: member.JoinedAt}
		if member.Rank != "" {
			out.SetRank(member.Rank)
		}
		return out
	}
}

func tgChannelParticipantForUpdate(selfUserID int64, member domain.ChannelMember) tg.ChannelParticipantClass {
	if member.UserID == 0 {
		return nil
	}
	if member.Status == domain.ChannelMemberKicked || member.Status == domain.ChannelMemberBanned || member.BannedRights.ViewMessages {
		return &tg.ChannelParticipantBanned{
			Left:         member.Status != domain.ChannelMemberActive,
			Peer:         &tg.PeerUser{UserID: member.UserID},
			KickedBy:     member.InviterUserID,
			Date:         member.LeftAt,
			BannedRights: tgChatBannedRights(member.BannedRights),
		}
	}
	if member.Status == domain.ChannelMemberLeft {
		return &tg.ChannelParticipantLeft{Peer: &tg.PeerUser{UserID: member.UserID}}
	}
	return tgChannelParticipant(selfUserID, member)
}

func tgChannelAdminLogEvents(viewerUserID int64, events []domain.ChannelAdminLogEvent) []tg.ChannelAdminLogEvent {
	out := make([]tg.ChannelAdminLogEvent, 0, len(events))
	for _, event := range events {
		if item, ok := tgChannelAdminLogEvent(viewerUserID, event); ok {
			out = append(out, item)
		}
	}
	return out
}

func tgChannelAdminLogEvent(viewerUserID int64, event domain.ChannelAdminLogEvent) (tg.ChannelAdminLogEvent, bool) {
	action := tg.ChannelAdminLogEventActionClass(nil)
	switch event.Type {
	case domain.ChannelAdminLogChangeTitle:
		action = &tg.ChannelAdminLogEventActionChangeTitle{PrevValue: event.PrevString, NewValue: event.NewString}
	case domain.ChannelAdminLogChangeUsername:
		action = &tg.ChannelAdminLogEventActionChangeUsername{PrevValue: event.PrevString, NewValue: event.NewString}
	case domain.ChannelAdminLogChangeLinkedChat:
		action = &tg.ChannelAdminLogEventActionChangeLinkedChat{PrevValue: int64(event.PrevInt), NewValue: int64(event.NewInt)}
	case domain.ChannelAdminLogToggleSignatures:
		action = &tg.ChannelAdminLogEventActionToggleSignatures{NewValue: event.NewBool}
	case domain.ChannelAdminLogTogglePreHistoryHidden:
		action = &tg.ChannelAdminLogEventActionTogglePreHistoryHidden{NewValue: event.NewBool}
	case domain.ChannelAdminLogToggleForum:
		action = &tg.ChannelAdminLogEventActionToggleForum{NewValue: event.NewBool}
	case domain.ChannelAdminLogToggleAutotranslation:
		action = &tg.ChannelAdminLogEventActionToggleAutotranslation{NewValue: event.NewBool}
	case domain.ChannelAdminLogToggleAntiSpam:
		action = &tg.ChannelAdminLogEventActionToggleAntiSpam{NewValue: event.NewBool}
	case domain.ChannelAdminLogToggleSlowMode:
		action = &tg.ChannelAdminLogEventActionToggleSlowMode{PrevValue: event.PrevInt, NewValue: event.NewInt}
	case domain.ChannelAdminLogParticipantJoin:
		action = &tg.ChannelAdminLogEventActionParticipantJoin{}
	case domain.ChannelAdminLogParticipantLeave:
		action = &tg.ChannelAdminLogEventActionParticipantLeave{}
	case domain.ChannelAdminLogParticipantInvite:
		if event.Participant == nil {
			return tg.ChannelAdminLogEvent{}, false
		}
		action = &tg.ChannelAdminLogEventActionParticipantInvite{
			Participant: tgChannelParticipantForUpdate(viewerUserID, *event.Participant),
		}
	case domain.ChannelAdminLogParticipantPromote, domain.ChannelAdminLogParticipantDemote:
		if event.PrevParticipant == nil || event.NewParticipant == nil {
			return tg.ChannelAdminLogEvent{}, false
		}
		action = &tg.ChannelAdminLogEventActionParticipantToggleAdmin{
			PrevParticipant: tgChannelParticipantForUpdate(viewerUserID, *event.PrevParticipant),
			NewParticipant:  tgChannelParticipantForUpdate(viewerUserID, *event.NewParticipant),
		}
	case domain.ChannelAdminLogParticipantBan, domain.ChannelAdminLogParticipantUnban, domain.ChannelAdminLogParticipantKick, domain.ChannelAdminLogParticipantUnkick:
		if event.PrevParticipant == nil || event.NewParticipant == nil {
			return tg.ChannelAdminLogEvent{}, false
		}
		action = &tg.ChannelAdminLogEventActionParticipantToggleBan{
			PrevParticipant: tgChannelParticipantForUpdate(viewerUserID, *event.PrevParticipant),
			NewParticipant:  tgChannelParticipantForUpdate(viewerUserID, *event.NewParticipant),
		}
	case domain.ChannelAdminLogUpdatePinned:
		action = &tg.ChannelAdminLogEventActionUpdatePinned{Message: tgAdminLogMessage(viewerUserID, event.ChannelID, event.Message)}
	case domain.ChannelAdminLogSendMessage:
		action = &tg.ChannelAdminLogEventActionSendMessage{Message: tgAdminLogMessage(viewerUserID, event.ChannelID, event.Message)}
	case domain.ChannelAdminLogEditMessage:
		action = &tg.ChannelAdminLogEventActionEditMessage{
			PrevMessage: tgAdminLogMessage(viewerUserID, event.ChannelID, event.PrevMessage),
			NewMessage:  tgAdminLogMessage(viewerUserID, event.ChannelID, event.NewMessage),
		}
	case domain.ChannelAdminLogDeleteMessage:
		action = &tg.ChannelAdminLogEventActionDeleteMessage{Message: tgAdminLogMessage(viewerUserID, event.ChannelID, event.Message)}
	default:
		return tg.ChannelAdminLogEvent{}, false
	}
	if action == nil {
		return tg.ChannelAdminLogEvent{}, false
	}
	return tg.ChannelAdminLogEvent{
		ID:     event.ID,
		Date:   event.Date,
		UserID: event.UserID,
		Action: action,
	}, true
}

func tgAdminLogMessage(viewerUserID, channelID int64, msg *domain.ChannelMessage) tg.MessageClass {
	if msg == nil || msg.ID == 0 {
		out := &tg.MessageEmpty{ID: 0}
		out.SetPeerID(&tg.PeerChannel{ChannelID: channelID})
		return out
	}
	return tgChannelMessage(viewerUserID, *msg)
}

func tgChatAdminRights(rights domain.ChannelAdminRights) tg.ChatAdminRights {
	return tg.ChatAdminRights{
		ChangeInfo:     rights.ChangeInfo,
		PostMessages:   rights.PostMessages,
		EditMessages:   rights.EditMessages,
		DeleteMessages: rights.DeleteMessages,
		BanUsers:       rights.BanUsers,
		InviteUsers:    rights.InviteUsers,
		PinMessages:    rights.PinMessages,
		AddAdmins:      rights.AddAdmins,
		Anonymous:      rights.Anonymous,
		ManageCall:     rights.ManageCall,
		Other:          true,
	}
}

func domainChannelAdminRights(rights tg.ChatAdminRights) domain.ChannelAdminRights {
	return domain.ChannelAdminRights{
		ChangeInfo:     rights.ChangeInfo,
		PostMessages:   rights.PostMessages,
		EditMessages:   rights.EditMessages,
		DeleteMessages: rights.DeleteMessages,
		BanUsers:       rights.BanUsers,
		InviteUsers:    rights.InviteUsers,
		PinMessages:    rights.PinMessages,
		AddAdmins:      rights.AddAdmins,
		Anonymous:      rights.Anonymous,
		ManageCall:     rights.ManageCall,
	}
}

func tgChatBannedRights(rights domain.ChannelBannedRights) tg.ChatBannedRights {
	return tg.ChatBannedRights{
		ViewMessages: rights.ViewMessages,
		SendMessages: rights.SendMessages,
		SendMedia:    rights.SendMedia,
		SendStickers: rights.SendStickers,
		SendGifs:     rights.SendGifs,
		SendGames:    rights.SendGames,
		SendInline:   rights.SendInline,
		EmbedLinks:   rights.EmbedLinks,
		SendPolls:    rights.SendPolls,
		ChangeInfo:   rights.ChangeInfo,
		InviteUsers:  rights.InviteUsers,
		PinMessages:  rights.PinMessages,
		UntilDate:    rights.UntilDate,
	}
}

func domainChannelBannedRights(rights tg.ChatBannedRights) domain.ChannelBannedRights {
	return domain.ChannelBannedRights{
		ViewMessages: rights.ViewMessages,
		SendMessages: rights.SendMessages,
		SendMedia:    rights.SendMedia,
		SendStickers: rights.SendStickers,
		SendGifs:     rights.SendGifs,
		SendGames:    rights.SendGames,
		SendInline:   rights.SendInline,
		EmbedLinks:   rights.EmbedLinks,
		SendPolls:    rights.SendPolls,
		ChangeInfo:   rights.ChangeInfo,
		InviteUsers:  rights.InviteUsers,
		PinMessages:  rights.PinMessages,
		UntilDate:    rights.UntilDate,
	}
}

func zeroChannelBannedRights(rights domain.ChannelBannedRights) bool {
	return rights == domain.ChannelBannedRights{}
}

func tgLangPackDifference(pack domain.LangPack) *tg.LangPackDifference {
	return &tg.LangPackDifference{
		LangCode:    pack.LangCode,
		FromVersion: pack.FromVersion,
		Version:     pack.Version,
		Strings:     tgLangPackStrings(pack.Strings),
	}
}

func tgLangPackStrings(items []domain.LangPackString) []tg.LangPackStringClass {
	out := make([]tg.LangPackStringClass, 0, len(items))
	for _, item := range items {
		if item.Deleted {
			out = append(out, &tg.LangPackStringDeleted{Key: item.Key})
			continue
		}
		if item.Pluralized {
			out = append(out, &tg.LangPackStringPluralized{
				Key:        item.Key,
				ZeroValue:  item.ZeroValue,
				OneValue:   item.OneValue,
				TwoValue:   item.TwoValue,
				FewValue:   item.FewValue,
				ManyValue:  item.ManyValue,
				OtherValue: item.OtherValue,
			})
			continue
		}
		out = append(out, &tg.LangPackString{Key: item.Key, Value: item.Value})
	}
	return out
}

func tgPassword(settings domain.PasswordSettings) *tg.AccountPassword {
	if len(settings.SecureRandom) == 0 {
		settings.SecureRandom = []byte("telesrv-tdesktop-dev-secure-rand")
	}
	return &tg.AccountPassword{
		HasRecovery:             settings.HasRecovery,
		HasSecureValues:         settings.HasSecureValues,
		HasPassword:             settings.HasPassword,
		Hint:                    settings.Hint,
		EmailUnconfirmedPattern: settings.EmailUnconfirmedPattern,
		NewAlgo:                 &tg.PasswordKdfAlgoUnknown{},
		NewSecureAlgo:           &tg.SecurePasswordKdfAlgoUnknown{},
		SecureRandom:            settings.SecureRandom,
		LoginEmailPattern:       settings.LoginEmailPattern,
	}
}

func tgCountriesList(list domain.CountriesList) tg.HelpCountriesListClass {
	out := &tg.HelpCountriesList{
		Hash:      list.Hash,
		Countries: make([]tg.HelpCountry, 0, len(list.Countries)),
	}
	for _, country := range list.Countries {
		item := tg.HelpCountry{
			Hidden:       country.Hidden,
			ISO2:         country.ISO2,
			DefaultName:  country.DefaultName,
			Name:         country.Name,
			CountryCodes: make([]tg.HelpCountryCode, 0, len(country.CountryCodes)),
		}
		for _, code := range country.CountryCodes {
			item.CountryCodes = append(item.CountryCodes, tg.HelpCountryCode{
				CountryCode: code.CountryCode,
				Prefixes:    code.Prefixes,
				Patterns:    code.Patterns,
			})
		}
		out.Countries = append(out.Countries, item)
	}
	return out
}

func tgJSONValue(data []byte) tg.JSONValueClass {
	if len(data) == 0 {
		return &tg.JSONObject{}
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return &tg.JSONObject{}
	}
	return tgJSON(v)
}

func tgJSON(v any) tg.JSONValueClass {
	switch x := v.(type) {
	case nil:
		return &tg.JSONNull{}
	case bool:
		return &tg.JSONBool{Value: x}
	case float64:
		return &tg.JSONNumber{Value: x}
	case string:
		return &tg.JSONString{Value: x}
	case []any:
		arr := &tg.JSONArray{Value: make([]tg.JSONValueClass, 0, len(x))}
		for _, item := range x {
			arr.Value = append(arr.Value, tgJSON(item))
		}
		return arr
	case map[string]any:
		obj := &tg.JSONObject{Value: make([]tg.JSONObjectValue, 0, len(x))}
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			obj.Value = append(obj.Value, tg.JSONObjectValue{Key: key, Value: tgJSON(x[key])})
		}
		return obj
	default:
		return &tg.JSONString{Value: ""}
	}
}
