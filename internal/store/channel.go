package store

import (
	"context"

	"telesrv/internal/domain"
)

// ChannelStore persists Telegram channels/supergroups and their single-copy messages.
type ChannelStore interface {
	CreateChannel(ctx context.Context, req domain.CreateChannelRequest) (domain.CreateChannelResult, error)
	GetChannel(ctx context.Context, viewerUserID, channelID int64) (domain.ChannelView, error)
	GetChannelByID(ctx context.Context, channelID int64) (domain.Channel, error)
	SaveChannelDefaultSendAs(ctx context.Context, req domain.SaveChannelDefaultSendAsRequest) (domain.ChannelView, error)
	GetParticipants(ctx context.Context, viewerUserID, channelID int64, filter domain.ChannelParticipantsFilter, offset, limit int) (domain.ChannelParticipantList, error)
	GetParticipant(ctx context.Context, viewerUserID, channelID, participantUserID int64) (domain.ChannelMember, error)
	InviteToChannel(ctx context.Context, channelID, inviterUserID int64, userIDs []int64, date int) (domain.CreateChannelResult, error)
	JoinChannel(ctx context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error)
	LeaveChannel(ctx context.Context, channelID, userID int64, date int) (domain.CreateChannelResult, error)
	EditChannelTitle(ctx context.Context, req domain.EditChannelTitleRequest) (domain.EditChannelTitleResult, error)
	EditChannelAbout(ctx context.Context, req domain.EditChannelAboutRequest) (domain.Channel, error)
	EditChannelAdmin(ctx context.Context, req domain.EditChannelAdminRequest) (domain.EditChannelAdminResult, error)
	EditChannelBanned(ctx context.Context, req domain.EditChannelBannedRequest) (domain.EditChannelBannedResult, error)
	EditChannelDefaultBannedRights(ctx context.Context, req domain.EditChannelDefaultBannedRightsRequest) (domain.Channel, error)
	DeleteChannel(ctx context.Context, req domain.DeleteChannelRequest) (domain.DeleteChannelResult, error)
	CheckUsername(ctx context.Context, userID, channelID int64, username string) (bool, error)
	UpdateUsername(ctx context.Context, req domain.UpdateChannelUsernameRequest) (domain.Channel, error)
	ListAdminedPublicChannels(ctx context.Context, userID int64) ([]domain.Channel, error)
	ResolvePublicChannelUsername(ctx context.Context, viewerUserID int64, username string) (domain.Channel, bool, error)
	SearchPublicChannels(ctx context.Context, viewerUserID int64, query string, limit int) (domain.PublicChannelSearchResult, error)
	SetSignatures(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error)
	SetChannelPhoto(ctx context.Context, userID, channelID int64, photo *domain.Photo) (domain.Channel, error)
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
	ListAdminLog(ctx context.Context, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error)
	GetChannelMessageViews(ctx context.Context, req domain.ChannelMessageViewsRequest) (domain.ChannelMessageViewsResult, error)
	SetChannelMessageReactions(ctx context.Context, req domain.SetChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error)
	GetChannelMessageReactions(ctx context.Context, req domain.ChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error)
	ListChannelMessageReactions(ctx context.Context, req domain.ChannelMessageReactionsListRequest) (domain.ChannelMessageReactionsList, error)
	ListTopMessageReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error)
	ListRecentMessageReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error)
	ClearRecentMessageReactions(ctx context.Context, userID int64) error
	ListSavedReactionTags(ctx context.Context, userID int64, limit int) ([]domain.SavedReactionTag, error)
	UpsertSavedReactionTag(ctx context.Context, tag domain.SavedReactionTag) error
	CreateForumTopic(ctx context.Context, req domain.CreateChannelForumTopicRequest) (domain.CreateChannelForumTopicResult, error)
	EditForumTopic(ctx context.Context, req domain.EditChannelForumTopicRequest) (domain.EditChannelForumTopicResult, error)
	UpdatePinnedForumTopic(ctx context.Context, req domain.UpdateChannelForumTopicPinnedRequest) (domain.UpdateChannelForumTopicPinnedResult, error)
	ReorderPinnedForumTopics(ctx context.Context, req domain.ReorderChannelPinnedForumTopicsRequest) (domain.ReorderChannelPinnedForumTopicsResult, error)
	DeleteForumTopicHistory(ctx context.Context, req domain.DeleteChannelForumTopicHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	ListForumTopics(ctx context.Context, viewerUserID int64, filter domain.ChannelForumTopicFilter) (domain.ChannelForumTopicList, error)
	GetForumTopicsByID(ctx context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelForumTopicList, error)
	SendChannelMessage(ctx context.Context, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error)
	EditChannelMessage(ctx context.Context, req domain.EditChannelMessageRequest) (domain.EditChannelMessageResult, error)
	DeleteChannelMessages(ctx context.Context, req domain.DeleteChannelMessagesRequest) (domain.DeleteChannelMessagesResult, error)
	DeleteChannelHistory(ctx context.Context, req domain.DeleteChannelHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	DeleteChannelParticipantHistory(ctx context.Context, req domain.DeleteChannelParticipantHistoryRequest) (domain.DeleteChannelHistoryResult, error)
	UpdatePinnedMessage(ctx context.Context, req domain.UpdateChannelPinnedMessageRequest) (domain.UpdateChannelPinnedMessageResult, error)
	ExportInvite(ctx context.Context, req domain.ExportChannelInviteRequest) (domain.ExportChannelInviteResult, error)
	CheckInvite(ctx context.Context, userID int64, hash string, date int) (domain.CheckChannelInviteResult, error)
	ImportInvite(ctx context.Context, req domain.ImportChannelInviteRequest) (domain.CreateChannelResult, error)
	ListExportedInvites(ctx context.Context, req domain.ChannelInviteListRequest) (domain.ChannelInviteList, error)
	GetExportedInvite(ctx context.Context, req domain.GetChannelInviteRequest) (domain.ChannelInvite, error)
	EditExportedInvite(ctx context.Context, req domain.EditChannelInviteRequest) (domain.EditChannelInviteResult, error)
	DeleteExportedInvite(ctx context.Context, req domain.DeleteChannelInviteRequest) error
	DeleteRevokedExportedInvites(ctx context.Context, req domain.DeleteRevokedChannelInvitesRequest) error
	ListAdminsWithInvites(ctx context.Context, userID, channelID int64) ([]domain.ChannelAdminInviteCount, error)
	ListInviteImporters(ctx context.Context, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error)
	PendingJoinRequests(ctx context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error)
	HideChatJoinRequest(ctx context.Context, req domain.HideChannelJoinRequestRequest) (domain.CreateChannelResult, error)
	HideAllChatJoinRequests(ctx context.Context, req domain.HideChannelJoinRequestsRequest) (domain.CreateChannelResult, error)
	ListChannelDialogs(ctx context.Context, viewerUserID int64, filter domain.DialogFilter) (domain.ChannelDialogList, error)
	GetChannelDialogs(ctx context.Context, viewerUserID int64, channelIDs []int64) (domain.ChannelDialogList, error)
	ListCommonChannels(ctx context.Context, req domain.CommonChannelsRequest) (domain.CommonChannelsResult, error)
	ListLeftChannels(ctx context.Context, userID int64, offset, limit int) (domain.LeftChannelsResult, error)
	ListInactiveChannels(ctx context.Context, userID int64, limit int) (domain.ChannelDialogList, error)
	ListChannelRecommendations(ctx context.Context, req domain.ChannelRecommendationsRequest) (domain.ChannelRecommendationsResult, error)
	ListDiscussionGroups(ctx context.Context, userID int64, limit int) ([]domain.Channel, error)
	SetDiscussionGroup(ctx context.Context, userID, broadcastID, groupID int64) (domain.DiscussionGroupUpdateResult, error)
	SetChannelDialogPinned(ctx context.Context, userID, channelID int64, pinned bool) (bool, error)
	ReorderChannelPinnedDialogs(ctx context.Context, userID int64, order []domain.Peer, force bool) error
	SetChannelDialogUnreadMark(ctx context.Context, userID, channelID int64, unread bool) (bool, error)
	SetChannelViewForumAsMessages(ctx context.Context, userID, channelID int64, enabled bool) (bool, error)
	ListChannelUnreadMarked(ctx context.Context, userID int64) ([]domain.Peer, error)
	EditChannelPeerFolders(ctx context.Context, userID int64, peers []domain.FolderPeerUpdate) error
	ListChannelHistory(ctx context.Context, viewerUserID int64, filter domain.ChannelHistoryFilter) (domain.ChannelHistory, error)
	SearchPublicPosts(ctx context.Context, viewerUserID int64, req domain.ChannelSearchPostsRequest) (domain.ChannelHistory, error)
	SearchJoinedMessages(ctx context.Context, viewerUserID int64, req domain.ChannelGlobalSearchRequest) (domain.ChannelHistory, error)
	GetChannelMessages(ctx context.Context, viewerUserID, channelID int64, ids []int) (domain.ChannelHistory, error)
	ReadChannelMessageContents(ctx context.Context, req domain.ReadChannelMessageContentsRequest) (domain.ReadChannelMessageContentsResult, error)
	ListChannelReplies(ctx context.Context, viewerUserID int64, filter domain.ChannelRepliesFilter) (domain.ChannelHistory, error)
	ListChannelUnreadMentions(ctx context.Context, viewerUserID int64, filter domain.ChannelUnreadMentionsFilter) (domain.ChannelHistory, error)
	ReadChannelMentions(ctx context.Context, req domain.ReadChannelMentionsRequest) (domain.ReadChannelMentionsResult, error)
	ListChannelUnreadReactions(ctx context.Context, viewerUserID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error)
	ReadChannelReactions(ctx context.Context, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error)
	GetDiscussionMessage(ctx context.Context, viewerUserID, channelID int64, msgID int) (domain.ChannelDiscussionMessage, error)
	ReadChannelHistory(ctx context.Context, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error)
	ListMessageReadParticipants(ctx context.Context, req domain.ChannelReadParticipantsRequest) (domain.ChannelReadParticipantsResult, error)
	ListChannelDifference(ctx context.Context, req domain.ChannelDifferenceRequest) (domain.ChannelDifference, error)
	ListActiveChannelIDsForUser(ctx context.Context, userID, afterChannelID int64, limit int) ([]int64, error)
	ListDirtyActiveChannelsForUser(ctx context.Context, userID int64, sinceDate int, afterChannelID int64, limit int) ([]domain.DirtyChannel, error)
	ListActiveChannelMemberIDs(ctx context.Context, viewerUserID, channelID int64, limit int) ([]int64, error)
	ListChannelInviteAdminMemberIDs(ctx context.Context, channelID int64, limit int) ([]int64, error)
	FilterActiveChannelMemberIDs(ctx context.Context, channelID int64, userIDs []int64) ([]int64, error)
	MaxChannelPts(ctx context.Context, channelID int64) (int, error)
	MaxChannelMessageID(ctx context.Context, channelID int64) (int, error)
}

// ChannelIDAllocator allocates channel IDs.
type ChannelIDAllocator interface {
	NextChannelID(ctx context.Context) (int64, error)
	CurrentChannelID(ctx context.Context) (int64, error)
}

// ChannelPtsAllocator allocates channel-scoped pts.
type ChannelPtsAllocator interface {
	NextChannelPts(ctx context.Context, channelID int64) (int, error)
	CurrentChannelPts(ctx context.Context, channelID int64) (int, error)
}

// ChannelPtsRangeAllocator allocates a range of channel pts and returns the final pts.
type ChannelPtsRangeAllocator interface {
	NextChannelPtsN(ctx context.Context, channelID int64, count int) (int, error)
}

// ChannelMessageIDAllocator allocates channel-scoped message IDs.
type ChannelMessageIDAllocator interface {
	NextChannelMessageID(ctx context.Context, channelID int64) (int, error)
	CurrentChannelMessageID(ctx context.Context, channelID int64) (int, error)
}
