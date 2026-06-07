package rpc

import (
	"context"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"

	"telesrv/internal/domain"
)

// 本文件按「消费者定义接口」惯例，在 rpc 包定义 Router 依赖的业务服务接口。
// app/* 的 Service 实现它们；微服务化时 gRPC client 同样可实现，rpc 层无需改动。
// 接口方法只用 domain 类型与基本类型，不依赖 app 具体包——这是 rpc↔业务的契约边界。

// AuthService 抽象登录/注册业务。
type AuthService interface {
	BindTempAuthKey(ctx context.Context, sessionID int64, binding domain.TempAuthKeyBinding) error
	ResolveAuthKey(ctx context.Context, authKeyID [8]byte) ([8]byte, bool, error)
	UserID(ctx context.Context, authKeyID [8]byte) (int64, bool, error)
	SendCode(ctx context.Context, phone string) (string, error)
	SignIn(ctx context.Context, a domain.Authorization, phone, phoneCodeHash, code string) (domain.User, domain.Message, bool, error)
	SignUp(ctx context.Context, a domain.Authorization, phone, phoneCodeHash, firstName, lastName string) (domain.User, domain.Message, error)
	LogOut(ctx context.Context, authKeyID [8]byte) error
}

// SessionBinder 抽象登录后 session 与 user 的在线绑定。
type SessionBinder interface {
	BindAuthKey(sessionID int64, authKeyID [8]byte)
	AuthKeyID(sessionID int64) ([8]byte, bool)
	BindUser(sessionID, userID int64)
	UserID(sessionID int64) (int64, bool)
	UserIDResolved(sessionID int64) (userID int64, resolved bool)
	UnbindAuthKey(authKeyID [8]byte) int
	SetReceivesUpdates(sessionID int64, receives bool)
	PushToSession(ctx context.Context, sessionID int64, t proto.MessageType, msg bin.Encoder) error
	PushToUserExceptSession(ctx context.Context, userID, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error)
}

// ScopedSessionBinder 是 SessionBinder 的精确版本：所有定位都带 raw auth_key_id + session_id。
// 生产 mtprotoedge.SessionManager 实现它；测试替身和旧实现可以只实现 SessionBinder。
type ScopedSessionBinder interface {
	BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte)
	AuthKeyIDForSession(rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool)
	BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64)
	UserIDForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (int64, bool)
	UserIDResolvedForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (userID int64, resolved bool)
	SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool)
	PushToSessionForAuthKey(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg bin.Encoder) error
	PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error)
}

// BestEffortSessionBinder 是 updates fanout 的短超时推送接口；不用于 RPC result/ack。
type BestEffortSessionBinder interface {
	PushToUserExceptSessionBestEffort(ctx context.Context, userID, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error)
}

// ScopedBestEffortSessionBinder 是带 raw auth_key_id 精确排除当前设备的 best-effort 版本。
type ScopedBestEffortSessionBinder interface {
	PushToUserExceptAuthKeySessionBestEffort(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error)
}

// OnlineUserProvider exposes a bounded runtime snapshot for best-effort fanout.
type OnlineUserProvider interface {
	OnlineUserIDs(limit int) []int64
	IsUserOnline(userID int64) bool
	OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64
	TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64)
	ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64)
	OnlineChannelUserIDs(channelID int64, limit int) []int64
	SetSessionChannelMemberships(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64)
	AddUserChannelMembership(userID, channelID int64)
	RemoveUserChannelMembership(userID, channelID int64)
	OnlineChannelMemberUserIDs(channelID int64, limit int) []int64
}

// RateLimiter 抽象 RPC 高频写操作限流。
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (allowed bool, retryAfterSeconds int, err error)
}

// UsersService 抽象用户查询。
type UsersService interface {
	Self(ctx context.Context, userID int64) (domain.User, error)
	ByID(ctx context.Context, currentUserID, userID int64) (domain.User, bool, error)
	ByIDs(ctx context.Context, currentUserID int64, userIDs []int64) ([]domain.User, error)
}

// UserIdentityService 是 UsersService 的资料扩展能力，用于 username/phone 解析。
type UserIdentityService interface {
	CheckUsername(ctx context.Context, userID int64, username string) (bool, error)
	UpdateProfile(ctx context.Context, userID int64, update domain.UserProfileUpdate) (domain.User, error)
	UpdateUsername(ctx context.Context, userID int64, username string) (domain.User, error)
	ResolveUsername(ctx context.Context, currentUserID int64, username string) (domain.User, bool, error)
	ResolvePhone(ctx context.Context, currentUserID int64, phone string) (domain.User, bool, error)
}

// AccountService 抽象账号设置查询。
type AccountService interface {
	GetPassword(ctx context.Context, userID int64) (domain.PasswordSettings, error)
}

// HelpService 抽象启动配置与国家区号目录。
type HelpService interface {
	GetAppConfig(ctx context.Context, hash int) (domain.AppConfig, bool, error)
	GetCountries(ctx context.Context, langCode string, hash int) (domain.CountriesList, bool, error)
}

// UpdatesService 抽象 update 状态查询。
type UpdatesService interface {
	GetState(ctx context.Context, authKeyID [8]byte, userID int64) (domain.UpdateState, error)
	CurrentState(ctx context.Context, userID int64) (domain.UpdateState, error)
	GetDifference(ctx context.Context, authKeyID [8]byte, userID int64, from domain.UpdateState) (domain.UpdateDifference, error)
	ClearAuthKey(ctx context.Context, authKeyID [8]byte) error
	RecordNewMessage(ctx context.Context, authKeyID [8]byte, userID int64, msg domain.Message) (domain.UpdateEvent, domain.UpdateState, error)
	RecordReadHistory(ctx context.Context, authKeyID [8]byte, userID int64, read domain.ReadHistoryResult, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordContactsReset(ctx context.Context, authKeyID [8]byte, userID int64, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordDialogPinned(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, pinned bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordPinnedDialogs(ctx context.Context, authKeyID [8]byte, userID int64, order []domain.Peer, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordDialogUnreadMark(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, unread bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordPeerSettings(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, settings domain.PeerSettings, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordDialogFilter(ctx context.Context, authKeyID [8]byte, userID int64, folderID int, folder *domain.DialogFolder, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordDialogFilterOrder(ctx context.Context, authKeyID [8]byte, userID int64, order []int, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordDialogFiltersReload(ctx context.Context, authKeyID [8]byte, userID int64, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordFolderPeers(ctx context.Context, authKeyID [8]byte, userID int64, peers []domain.FolderPeerUpdate, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordChannelAvailableMessages(ctx context.Context, authKeyID [8]byte, userID, channelID int64, availableMinID int, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
	RecordChannelViewForumAsMessages(ctx context.Context, authKeyID [8]byte, userID, channelID int64, enabled bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error)
}

// ContactsService 抽象通讯录查询。
type ContactsService interface {
	GetContacts(ctx context.Context, userID int64, hash int64) (domain.ContactList, bool, error)
	ContactIDs(ctx context.Context, userID int64, hash int64) ([]int, bool, error)
	AddContact(ctx context.Context, userID int64, input domain.ContactInput) (domain.Contact, error)
	ImportContacts(ctx context.Context, userID int64, inputs []domain.ContactInput) (domain.ImportContactsResult, error)
	Search(ctx context.Context, userID int64, query string, limit int) (domain.UserSearchResult, error)
	DeleteContacts(ctx context.Context, userID int64, contactUserIDs []int64) (int, error)
	UpdateContactNote(ctx context.Context, userID, contactUserID int64, note string, entities []domain.MessageEntity) (domain.Contact, error)
	GetPeerSettings(ctx context.Context, userID int64, peer domain.Peer) (domain.PeerSettings, error)
	BlockContact(ctx context.Context, userID, peerUserID int64, date int) (bool, error)
	UnblockContact(ctx context.Context, userID, peerUserID int64) (bool, error)
	IsBlocked(ctx context.Context, userID, peerUserID int64) (bool, error)
	GetBlocked(ctx context.Context, userID int64, offset, limit int) (domain.BlockedContactList, error)
}

// DialogsService 抽象会话列表查询。
type DialogsService interface {
	GetDialogs(ctx context.Context, userID int64, filter domain.DialogFilter) (domain.DialogList, error)
	GetPeerDialogs(ctx context.Context, userID int64, peers []domain.Peer) (domain.DialogList, error)
	SaveDraft(ctx context.Context, userID int64, draft domain.DialogDraft) error
	DeleteDraft(ctx context.Context, userID int64, peer domain.Peer, topMessageID int) (bool, error)
	ListDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error)
	ClearDrafts(ctx context.Context, userID int64, limit int) ([]domain.DialogDraft, error)
	TogglePinned(ctx context.Context, userID int64, peer domain.Peer, pinned bool) (bool, error)
	ReorderPinned(ctx context.Context, userID int64, order []domain.Peer, force bool) error
	MarkUnread(ctx context.Context, userID int64, peer domain.Peer, unread bool) (bool, error)
	UnreadMarks(ctx context.Context, userID int64) ([]domain.Peer, error)
	HidePeerSettingsBar(ctx context.Context, userID int64, peer domain.Peer) (bool, error)
	PeerSettingsBarHidden(ctx context.Context, userID int64, peer domain.Peer) (bool, error)
	GetDialogFolders(ctx context.Context, userID int64) (domain.DialogFolderList, error)
	SaveDialogFolder(ctx context.Context, userID int64, folder domain.DialogFolder) error
	DeleteDialogFolder(ctx context.Context, userID int64, folderID int) error
	ReorderDialogFolders(ctx context.Context, userID int64, order []int) error
	ToggleDialogFolderTags(ctx context.Context, userID int64, enabled bool) error
	EditPeerFolders(ctx context.Context, userID int64, peers []domain.FolderPeerUpdate) error
}

// MessagesService 抽象消息历史、搜索与已读。
type MessagesService interface {
	SendPrivateText(ctx context.Context, userID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error)
	ForwardPrivateMessages(ctx context.Context, userID int64, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error)
	GetMessages(ctx context.Context, userID int64, ids []int) (domain.MessageList, error)
	GetHistory(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error)
	Search(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error)
	ReadHistory(ctx context.Context, userID int64, req domain.ReadHistoryRequest) (domain.ReadHistoryResult, error)
	ReadMessageContents(ctx context.Context, userID int64, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error)
	GetOutboxReadDate(ctx context.Context, userID int64, req domain.OutboxReadDateRequest) (int, error)
	SetMessageReactions(ctx context.Context, userID int64, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error)
	GetMessageReactions(ctx context.Context, userID int64, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error)
	EditMessage(ctx context.Context, userID int64, req domain.EditMessageRequest) (domain.EditMessageResult, error)
	DeleteMessages(ctx context.Context, userID int64, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error)
	DeleteHistory(ctx context.Context, userID int64, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error)
}

// ChannelsService 抽象超级群/频道业务。
type ChannelsService interface {
	CreateMegagroupFromCreateChat(ctx context.Context, userID int64, req domain.CreateChannelRequest) (domain.CreateChannelResult, error)
	CreateChannel(ctx context.Context, userID int64, req domain.CreateChannelRequest) (domain.CreateChannelResult, error)
	GetChannel(ctx context.Context, userID, channelID int64) (domain.ChannelView, error)
	GetJoinableChannel(ctx context.Context, userID, channelID int64) (domain.Channel, error)
	GetParticipants(ctx context.Context, userID, channelID int64, filter domain.ChannelParticipantsFilter, offset, limit int) (domain.ChannelParticipantList, error)
	GetParticipant(ctx context.Context, userID, channelID, participantUserID int64) (domain.ChannelMember, error)
	InviteToChannel(ctx context.Context, userID, channelID int64, userIDs []int64, date int) (domain.CreateChannelResult, error)
	JoinChannel(ctx context.Context, userID, channelID int64, date int) (domain.CreateChannelResult, error)
	LeaveChannel(ctx context.Context, userID, channelID int64, date int) (domain.CreateChannelResult, error)
	EditTitle(ctx context.Context, userID int64, req domain.EditChannelTitleRequest) (domain.EditChannelTitleResult, error)
	EditAbout(ctx context.Context, userID int64, req domain.EditChannelAboutRequest) (domain.Channel, error)
	EditAdmin(ctx context.Context, userID int64, req domain.EditChannelAdminRequest) (domain.EditChannelAdminResult, error)
	EditBanned(ctx context.Context, userID int64, req domain.EditChannelBannedRequest) (domain.EditChannelBannedResult, error)
	EditDefaultBannedRights(ctx context.Context, userID int64, req domain.EditChannelDefaultBannedRightsRequest) (domain.Channel, error)
	DeleteChannel(ctx context.Context, userID int64, req domain.DeleteChannelRequest) (domain.DeleteChannelResult, error)
	CheckUsername(ctx context.Context, userID, channelID int64, username string) (bool, error)
	UpdateUsername(ctx context.Context, userID int64, req domain.UpdateChannelUsernameRequest) (domain.Channel, error)
	ListAdminedPublicChannels(ctx context.Context, userID int64) ([]domain.Channel, error)
	ResolvePublicUsername(ctx context.Context, userID int64, username string) (domain.Channel, bool, error)
	SearchPublicChannels(ctx context.Context, userID int64, query string, limit int) (domain.PublicChannelSearchResult, error)
	SetSignatures(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetPhoto(ctx context.Context, userID, channelID int64, photo *domain.Photo) (domain.Channel, error)
	SetPreHistoryHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetParticipantsHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetForum(ctx context.Context, userID, channelID int64, enabled, tabs bool) (domain.Channel, error)
	SetAutotranslation(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetRestrictedSponsored(ctx context.Context, userID, channelID int64, restricted bool) (domain.Channel, error)
	SetPaidMessagesPrice(ctx context.Context, userID, channelID int64, stars int64, broadcastMessagesAllowed bool) (domain.Channel, error)
	SetAntiSpam(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetSlowMode(ctx context.Context, userID, channelID int64, seconds int) (domain.Channel, error)
	SetNoForwards(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetJoinToSend(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetJoinRequest(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetAvailableReactions(ctx context.Context, userID, channelID int64, policy domain.ChannelReactionPolicy) (domain.Channel, error)
	SetColor(ctx context.Context, userID, channelID int64, forProfile bool, color domain.ChannelPeerColor) (domain.Channel, error)
	SetEmojiStatus(ctx context.Context, userID, channelID int64, status domain.ChannelEmojiStatus) (domain.Channel, error)
	ListAdminLog(ctx context.Context, userID int64, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error)
	GetChannelForChangeInfo(ctx context.Context, userID, channelID int64) (domain.ChannelView, error)
	SaveDefaultSendAs(ctx context.Context, userID int64, req domain.SaveChannelDefaultSendAsRequest) (domain.ChannelView, error)
	GetMessageViews(ctx context.Context, userID int64, req domain.ChannelMessageViewsRequest) (domain.ChannelMessageViewsResult, error)
	SetMessageReactions(ctx context.Context, userID int64, req domain.SetChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error)
	GetMessageReactions(ctx context.Context, userID int64, req domain.ChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error)
	ListMessageReactions(ctx context.Context, userID int64, req domain.ChannelMessageReactionsListRequest) (domain.ChannelMessageReactionsList, error)
	TopReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error)
	RecentReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error)
	ClearRecentReactions(ctx context.Context, userID int64) error
	SavedReactionTags(ctx context.Context, userID int64, limit int) ([]domain.SavedReactionTag, error)
	UpdateSavedReactionTag(ctx context.Context, userID int64, tag domain.SavedReactionTag) error
	ReadMessageContents(ctx context.Context, userID int64, req domain.ReadChannelMessageContentsRequest) (domain.ReadChannelMessageContentsResult, error)
	GetMessageAuthor(ctx context.Context, userID int64, req domain.GetChannelMessageAuthorRequest) (domain.GetChannelMessageAuthorResult, error)
	CreateForumTopic(ctx context.Context, userID int64, req domain.CreateChannelForumTopicRequest) (domain.CreateChannelForumTopicResult, error)
	EditForumTopic(ctx context.Context, userID int64, req domain.EditChannelForumTopicRequest) (domain.EditChannelForumTopicResult, error)
	UpdatePinnedForumTopic(ctx context.Context, userID int64, req domain.UpdateChannelForumTopicPinnedRequest) (domain.UpdateChannelForumTopicPinnedResult, error)
	ReorderPinnedForumTopics(ctx context.Context, userID int64, req domain.ReorderChannelPinnedForumTopicsRequest) (domain.ReorderChannelPinnedForumTopicsResult, error)
	DeleteForumTopicHistory(ctx context.Context, userID int64, req domain.DeleteChannelForumTopicHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	GetForumTopics(ctx context.Context, userID int64, filter domain.ChannelForumTopicFilter) (domain.ChannelForumTopicList, error)
	GetForumTopicsByID(ctx context.Context, userID, channelID int64, ids []int) (domain.ChannelForumTopicList, error)
	SendMessage(ctx context.Context, userID int64, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error)
	EditMessage(ctx context.Context, userID int64, req domain.EditChannelMessageRequest) (domain.EditChannelMessageResult, error)
	DeleteMessages(ctx context.Context, userID int64, req domain.DeleteChannelMessagesRequest) (domain.DeleteChannelMessagesResult, error)
	DeleteHistory(ctx context.Context, userID int64, req domain.DeleteChannelHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	DeleteParticipantHistory(ctx context.Context, userID int64, req domain.DeleteChannelParticipantHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	UpdatePinnedMessage(ctx context.Context, userID int64, req domain.UpdateChannelPinnedMessageRequest) (domain.UpdateChannelPinnedMessageResult, error)
	ExportInvite(ctx context.Context, userID int64, req domain.ExportChannelInviteRequest) (domain.ExportChannelInviteResult, error)
	CheckInvite(ctx context.Context, userID int64, hash string, date int) (domain.CheckChannelInviteResult, error)
	ImportInvite(ctx context.Context, userID int64, req domain.ImportChannelInviteRequest) (domain.CreateChannelResult, error)
	ListExportedInvites(ctx context.Context, userID int64, req domain.ChannelInviteListRequest) (domain.ChannelInviteList, error)
	GetExportedInvite(ctx context.Context, userID int64, req domain.GetChannelInviteRequest) (domain.ChannelInvite, error)
	EditExportedInvite(ctx context.Context, userID int64, req domain.EditChannelInviteRequest) (domain.EditChannelInviteResult, error)
	DeleteExportedInvite(ctx context.Context, userID int64, req domain.DeleteChannelInviteRequest) error
	DeleteRevokedExportedInvites(ctx context.Context, userID int64, req domain.DeleteRevokedChannelInvitesRequest) error
	ListAdminsWithInvites(ctx context.Context, userID, channelID int64) ([]domain.ChannelAdminInviteCount, error)
	ListInviteImporters(ctx context.Context, userID int64, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error)
	PendingJoinRequests(ctx context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error)
	HideChatJoinRequest(ctx context.Context, userID int64, req domain.HideChannelJoinRequestRequest) (domain.CreateChannelResult, error)
	HideAllChatJoinRequests(ctx context.Context, userID int64, req domain.HideChannelJoinRequestsRequest) (domain.CreateChannelResult, error)
	CommonChannels(ctx context.Context, userID int64, req domain.CommonChannelsRequest) (domain.CommonChannelsResult, error)
	LeftChannels(ctx context.Context, userID int64, offset, limit int) (domain.LeftChannelsResult, error)
	InactiveChannels(ctx context.Context, userID int64, limit int) (domain.ChannelDialogList, error)
	ChannelRecommendations(ctx context.Context, userID int64, req domain.ChannelRecommendationsRequest) (domain.ChannelRecommendationsResult, error)
	DiscussionGroups(ctx context.Context, userID int64, limit int) ([]domain.Channel, error)
	SetDiscussionGroup(ctx context.Context, userID, broadcastID, groupID int64) (domain.DiscussionGroupUpdateResult, error)
	SetViewForumAsMessages(ctx context.Context, userID, channelID int64, enabled bool) (bool, error)
	GetHistory(ctx context.Context, userID int64, filter domain.ChannelHistoryFilter) (domain.ChannelHistory, error)
	SearchPosts(ctx context.Context, userID int64, req domain.ChannelSearchPostsRequest) (domain.ChannelHistory, error)
	SearchJoinedMessages(ctx context.Context, userID int64, req domain.ChannelGlobalSearchRequest) (domain.ChannelHistory, error)
	GetMessages(ctx context.Context, userID, channelID int64, ids []int) (domain.ChannelHistory, error)
	GetReplies(ctx context.Context, userID int64, filter domain.ChannelRepliesFilter) (domain.ChannelHistory, error)
	GetUnreadMentions(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter) (domain.ChannelHistory, error)
	ReadMentions(ctx context.Context, userID int64, req domain.ReadChannelMentionsRequest) (domain.ReadChannelMentionsResult, error)
	GetUnreadReactions(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error)
	ReadReactions(ctx context.Context, userID int64, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error)
	GetDiscussionMessage(ctx context.Context, userID, channelID int64, msgID int) (domain.ChannelDiscussionMessage, error)
	ReadHistory(ctx context.Context, userID int64, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error)
	GetMessageReadParticipants(ctx context.Context, userID int64, req domain.ChannelReadParticipantsRequest) (domain.ChannelReadParticipantsResult, error)
	GetDifference(ctx context.Context, userID int64, req domain.ChannelDifferenceRequest) (domain.ChannelDifference, error)
	ActiveChannelIDsForUser(ctx context.Context, userID, afterChannelID int64, limit int) ([]int64, error)
	DirtyActiveChannelsForUser(ctx context.Context, userID int64, sinceDate int, afterChannelID int64, limit int) ([]domain.DirtyChannel, error)
	ActiveMemberIDs(ctx context.Context, userID, channelID int64, limit int) ([]int64, error)
	InviteAdminMemberIDs(ctx context.Context, channelID int64, limit int) ([]int64, error)
	FilterActiveMemberIDs(ctx context.Context, channelID int64, userIDs []int64) ([]int64, error)
}

// FilesService 抽象文件上传分片、下载与媒体（document/photo）组装。
// 方法只用 domain 类型；rpc 层负责 tg.InputFileLocation / InputMedia ↔ domain 转换。
type FilesService interface {
	SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, bytes []byte) (bool, error)
	SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, bytes []byte) (bool, error)
	GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error)
	// 资源读取（reaction / sticker / document）。
	ListAvailableReactions(ctx context.Context) ([]domain.AvailableReaction, error)
	GetDocuments(ctx context.Context, ids []int64) ([]domain.Document, error)
	ResolveStickerSet(ctx context.Context, ref domain.StickerSetRef) (set domain.StickerSet, documents []domain.Document, found bool, err error)
	ListStickerSets(ctx context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error)
	// 头像（profile photo）与消息媒体组装。
	CreatePhotoFromUpload(ctx context.Context, file domain.UploadedFileRef) (domain.Photo, error)
	CreateAvatarFromUpload(ctx context.Context, file domain.UploadedFileRef) (domain.Photo, error)
	CreateDocumentFromUpload(ctx context.Context, file domain.UploadedFileRef, spec domain.DocumentSpec) (domain.Document, error)
	GetPhoto(ctx context.Context, id int64) (domain.Photo, bool, error)
	GetDocument(ctx context.Context, id int64) (domain.Document, bool, error)
	UploadProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64, file domain.UploadedFileRef, date int) (domain.Photo, error)
	SetCurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID, photoID int64, date int) (domain.Photo, bool, error)
	CurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64) (domain.Photo, bool, error)
	GetProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, offset, limit int, maxID int64) (photos []domain.Photo, total int, err error)
	DeleteProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, photoIDs []int64) (int, error)
}

// LangPackService 抽象客户端语言包查询。
type LangPackService interface {
	GetLangPack(ctx context.Context, langPack, langCode string) (domain.LangPack, error)
	GetDifference(ctx context.Context, langPack, langCode string, fromVersion int) (domain.LangPack, error)
	GetStrings(ctx context.Context, langPack, langCode string, keys []string) (domain.LangPack, error)
}

// Deps 按业务域注入服务接口。各域的 handler 注册见对应文件（auth.go / users.go / updates.go）。
type Deps struct {
	Auth     AuthService
	Account  AccountService
	Help     HelpService
	Users    UsersService
	Updates  UpdatesService
	Contacts ContactsService
	Dialogs  DialogsService
	Messages MessagesService
	Channels ChannelsService
	Files    FilesService
	LangPack LangPackService
	Sessions SessionBinder
	Limiter  RateLimiter
	Metrics  Metrics
}
