package channels

import (
	"context"
	"strings"
	"unicode/utf8"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service exposes channel/supergroup business operations.
type Service struct {
	channels store.ChannelStore
}

// NewService creates a channel service.
func NewService(channels store.ChannelStore) *Service {
	return &Service{channels: channels}
}

// CreateMegagroupFromCreateChat handles messages.createChat by directly creating a megagroup.
func (s *Service) CreateMegagroupFromCreateChat(ctx context.Context, userID int64, req domain.CreateChannelRequest) (domain.CreateChannelResult, error) {
	req.CreatorUserID = userID
	req.Broadcast = false
	req.Megagroup = true
	return s.CreateChannel(ctx, userID, req)
}

// CreateChannel creates a broadcast channel or megagroup.
func (s *Service) CreateChannel(ctx context.Context, userID int64, req domain.CreateChannelRequest) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.CreatorUserID == 0 {
		req.CreatorUserID = userID
	}
	if req.CreatorUserID != userID {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if strings.TrimSpace(req.Title) == "" {
		return domain.CreateChannelResult{}, domain.ErrChannelTitleInvalid
	}
	if len(req.MemberUserIDs) > domain.MaxChannelInviteUsers {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if !req.Broadcast && !req.Megagroup {
		req.Broadcast = true
	}
	return s.channels.CreateChannel(ctx, req)
}

// GetChannel returns channel data personalized for userID.
func (s *Service) GetChannel(ctx context.Context, userID, channelID int64) (domain.ChannelView, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	return s.channels.GetChannel(ctx, userID, channelID)
}

// GetJoinableChannel returns a channel shell so RPC can verify access hash before join.
func (s *Service) GetJoinableChannel(ctx context.Context, userID, channelID int64) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.GetChannelByID(ctx, channelID)
}

// GetParticipants returns a bounded participants page.
func (s *Service) GetParticipants(ctx context.Context, userID, channelID int64, filter domain.ChannelParticipantsFilter, offset, limit int) (domain.ChannelParticipantList, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.ChannelParticipantList{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(filter.Query) > domain.MaxChannelParticipantsQueryLength {
		return domain.ChannelParticipantList{}, domain.ErrChannelInvalid
	}
	return s.channels.GetParticipants(ctx, userID, channelID, filter, offset, capLimit(limit, domain.MaxChannelParticipantsLimit))
}

// GetParticipant returns one participant.
func (s *Service) GetParticipant(ctx context.Context, userID, channelID, participantUserID int64) (domain.ChannelMember, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 || participantUserID == 0 {
		return domain.ChannelMember{}, domain.ErrChannelInvalid
	}
	return s.channels.GetParticipant(ctx, userID, channelID, participantUserID)
}

// InviteToChannel invites users to a channel/supergroup.
func (s *Service) InviteToChannel(ctx context.Context, userID, channelID int64, userIDs []int64, date int) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 || len(userIDs) == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if len(userIDs) > domain.MaxChannelInviteUsers {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	return s.channels.InviteToChannel(ctx, channelID, userID, userIDs, date)
}

// JoinChannel joins current user to a channel/supergroup.
func (s *Service) JoinChannel(ctx context.Context, userID, channelID int64, date int) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	return s.channels.JoinChannel(ctx, channelID, userID, date)
}

// LeaveChannel leaves current user from a channel/supergroup.
func (s *Service) LeaveChannel(ctx context.Context, userID, channelID int64, date int) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	return s.channels.LeaveChannel(ctx, channelID, userID, date)
}

// EditTitle edits a channel/supergroup title.
func (s *Service) EditTitle(ctx context.Context, userID int64, req domain.EditChannelTitleRequest) (domain.EditChannelTitleResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelTitleResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || strings.TrimSpace(req.Title) == "" {
		return domain.EditChannelTitleResult{}, domain.ErrChannelTitleInvalid
	}
	return s.channels.EditChannelTitle(ctx, req)
}

// EditAbout edits a channel/supergroup description.
func (s *Service) EditAbout(ctx context.Context, userID int64, req domain.EditChannelAboutRequest) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.EditChannelAbout(ctx, req)
}

// EditAdmin edits a participant's admin rights.
func (s *Service) EditAdmin(ctx context.Context, userID int64, req domain.EditChannelAdminRequest) (domain.EditChannelAdminResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelAdminResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.MemberID == 0 || len(req.Rank) > domain.MaxChannelAdminRankLength {
		return domain.EditChannelAdminResult{}, domain.ErrChannelInvalid
	}
	return s.channels.EditChannelAdmin(ctx, req)
}

// EditBanned edits a participant's banned rights.
func (s *Service) EditBanned(ctx context.Context, userID int64, req domain.EditChannelBannedRequest) (domain.EditChannelBannedResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelBannedResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.Participant.Type != domain.PeerTypeUser || req.Participant.ID == 0 {
		return domain.EditChannelBannedResult{}, domain.ErrChannelInvalid
	}
	return s.channels.EditChannelBanned(ctx, req)
}

// EditDefaultBannedRights edits the channel/supergroup default restrictions.
func (s *Service) EditDefaultBannedRights(ctx context.Context, userID int64, req domain.EditChannelDefaultBannedRightsRequest) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.EditChannelDefaultBannedRights(ctx, req)
}

// DeleteChannel deletes a channel/supergroup.
func (s *Service) DeleteChannel(ctx context.Context, userID int64, req domain.DeleteChannelRequest) (domain.DeleteChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DeleteChannelResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.DeleteChannelResult{}, domain.ErrChannelInvalid
	}
	return s.channels.DeleteChannel(ctx, req)
}

// CheckUsername checks whether a channel username is syntactically valid and free.
func (s *Service) CheckUsername(ctx context.Context, userID, channelID int64, username string) (bool, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return false, domain.ErrChannelInvalid
	}
	username = normalizeChannelUsername(username)
	if !validChannelUsername(username) {
		return false, domain.ErrUsernameInvalid
	}
	return s.channels.CheckUsername(ctx, userID, channelID, username)
}

// UpdateUsername sets or clears a channel public username.
func (s *Service) UpdateUsername(ctx context.Context, userID int64, req domain.UpdateChannelUsernameRequest) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	req.Username = normalizeChannelUsername(req.Username)
	if req.Username != "" && !validChannelUsername(req.Username) {
		return domain.Channel{}, domain.ErrUsernameInvalid
	}
	return s.channels.UpdateUsername(ctx, req)
}

// ListAdminedPublicChannels returns public channels/supergroups administered by user.
func (s *Service) ListAdminedPublicChannels(ctx context.Context, userID int64) ([]domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return nil, nil
	}
	return s.channels.ListAdminedPublicChannels(ctx, userID)
}

// ResolvePublicUsername resolves a public channel/supergroup username visible to userID.
func (s *Service) ResolvePublicUsername(ctx context.Context, userID int64, username string) (domain.Channel, bool, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.Channel{}, false, domain.ErrChannelInvalid
	}
	username = normalizeChannelUsername(username)
	if !validChannelUsername(username) {
		return domain.Channel{}, false, domain.ErrUsernameInvalid
	}
	return s.channels.ResolvePublicChannelUsername(ctx, userID, username)
}

// SearchPublicChannels returns public username channels/supergroups for contacts.search.
func (s *Service) SearchPublicChannels(ctx context.Context, userID int64, query string, limit int) (domain.PublicChannelSearchResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.PublicChannelSearchResult{}, domain.ErrChannelInvalid
	}
	query = normalizeChannelUsername(query)
	if query == "" {
		return domain.PublicChannelSearchResult{}, nil
	}
	if utf8.RuneCountInString(query) > domain.MaxPublicChannelSearchQueryLength {
		return domain.PublicChannelSearchResult{}, domain.ErrChannelInvalid
	}
	return s.channels.SearchPublicChannels(ctx, userID, query, capLimit(limit, domain.MaxPublicChannelSearchLimit))
}

// SetSignatures toggles channel post signatures.
func (s *Service) SetSignatures(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetSignatures(ctx, userID, channelID, enabled)
}

// SetPhoto 设置/清除频道头像（photo==nil 表示清除）。
func (s *Service) SetPhoto(ctx context.Context, userID, channelID int64, photo *domain.Photo) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetChannelPhoto(ctx, userID, channelID, photo)
}

// SetPreHistoryHidden toggles hidden history for new supergroup members.
func (s *Service) SetPreHistoryHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetPreHistoryHidden(ctx, userID, channelID, enabled)
}

// SetParticipantsHidden toggles whether non-admins can view the full member list.
func (s *Service) SetParticipantsHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetParticipantsHidden(ctx, userID, channelID, enabled)
}

// SetForum toggles forum topics and their preferred TDesktop layout for a megagroup.
func (s *Service) SetForum(ctx context.Context, userID, channelID int64, enabled, tabs bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetForum(ctx, userID, channelID, enabled, tabs)
}

// SetAutotranslation toggles channel autotranslation for all users.
func (s *Service) SetAutotranslation(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetAutotranslation(ctx, userID, channelID, enabled)
}

// SetRestrictedSponsored toggles the channelFull restricted_sponsored state.
func (s *Service) SetRestrictedSponsored(ctx context.Context, userID, channelID int64, restricted bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetRestrictedSponsored(ctx, userID, channelID, restricted)
}

// SetPaidMessagesPrice stores the currently advertised paid-message price state.
func (s *Service) SetPaidMessagesPrice(ctx context.Context, userID, channelID int64, stars int64, broadcastMessagesAllowed bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 || stars < 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetPaidMessagesPrice(ctx, userID, channelID, stars, broadcastMessagesAllowed)
}

// SetAntiSpam toggles native antispam mode for a supergroup.
func (s *Service) SetAntiSpam(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetAntiSpam(ctx, userID, channelID, enabled)
}

// SetSlowMode changes the supergroup slow mode interval in seconds.
func (s *Service) SetSlowMode(ctx context.Context, userID, channelID int64, seconds int) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 || !domain.ValidChannelSlowModeSeconds(seconds) {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetSlowMode(ctx, userID, channelID, seconds)
}

// SetNoForwards toggles channel/supergroup content protection.
func (s *Service) SetNoForwards(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetNoForwards(ctx, userID, channelID, enabled)
}

// SetJoinToSend toggles whether non-members must join before sending in a megagroup.
func (s *Service) SetJoinToSend(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetJoinToSend(ctx, userID, channelID, enabled)
}

// SetJoinRequest toggles public join request approval for a megagroup.
func (s *Service) SetJoinRequest(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetJoinRequest(ctx, userID, channelID, enabled)
}

// SetAvailableReactions stores the bounded reaction policy for a channel/supergroup.
func (s *Service) SetAvailableReactions(ctx context.Context, userID, channelID int64, policy domain.ChannelReactionPolicy) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if len(policy.Emoticons)+len(policy.CustomEmojiIDs) > domain.MaxChannelReactionItems ||
		policy.Limit < 0 ||
		policy.Limit > domain.MaxChannelReactionItems {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	for _, emoticon := range policy.Emoticons {
		if strings.TrimSpace(emoticon) == "" || utf8.RuneCountInString(emoticon) > domain.MaxChannelReactionEmoticonLength {
			return domain.Channel{}, domain.ErrChannelInvalid
		}
	}
	return s.channels.SetAvailableReactions(ctx, userID, channelID, policy)
}

// SetColor stores the channel message/profile accent color.
func (s *Service) SetColor(ctx context.Context, userID, channelID int64, forProfile bool, color domain.ChannelPeerColor) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetColor(ctx, userID, channelID, forProfile, color)
}

// SetEmojiStatus stores or clears the channel emoji status.
func (s *Service) SetEmojiStatus(ctx context.Context, userID, channelID int64, status domain.ChannelEmojiStatus) (domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	if status.DocumentID < 0 || status.Until < 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetEmojiStatus(ctx, userID, channelID, status)
}

// ListAdminLog returns one bounded, channel-scoped admin log page.
func (s *Service) ListAdminLog(ctx context.Context, userID int64, req domain.ChannelAdminLogRequest) (domain.ChannelAdminLogResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.MaxID < 0 || req.MinID < 0 {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(req.Query) > domain.MaxChannelAdminLogQueryLength || len(req.AdminUserIDs) > domain.MaxChannelAdminLogAdmins {
		return domain.ChannelAdminLogResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelAdminLogLimit)
	return s.channels.ListAdminLog(ctx, req)
}

// GetChannelForChangeInfo validates the current user can change channel metadata.
func (s *Service) GetChannelForChangeInfo(ctx context.Context, userID, channelID int64) (domain.ChannelView, error) {
	view, err := s.GetChannel(ctx, userID, channelID)
	if err != nil {
		return domain.ChannelView{}, err
	}
	if view.Self.Role == domain.ChannelRoleCreator || (view.Self.Role == domain.ChannelRoleAdmin && view.Self.AdminRights.ChangeInfo) {
		return view, nil
	}
	return domain.ChannelView{}, domain.ErrChannelAdminRequired
}

// SaveDefaultSendAs persists the current user's default send-as peer for one channel/supergroup dialog.
func (s *Service) SaveDefaultSendAs(ctx context.Context, userID int64, req domain.SaveChannelDefaultSendAsRequest) (domain.ChannelView, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.ChannelView{}, domain.ErrChannelInvalid
	}
	return s.channels.SaveChannelDefaultSendAs(ctx, req)
}

// GetMessageViews returns channel-scoped view counters and optionally records first-time views.
func (s *Service) GetMessageViews(ctx context.Context, userID int64, req domain.ChannelMessageViewsRequest) (domain.ChannelMessageViewsResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageViewsResult{}, domain.ErrChannelInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelMessageViewsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.GetChannelMessageViews(ctx, req)
}

// SetMessageReactions replaces the current user's reactions for one channel/supergroup message.
func (s *Service) SetMessageReactions(ctx context.Context, userID int64, req domain.SetChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.MessageID <= 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.MessageID > domain.MaxMessageBoxID || len(req.Reactions) > domain.MaxChannelMessageReactionsPerUser {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	return s.channels.SetChannelMessageReactions(ctx, req)
}

// GetMessageReactions returns reaction summaries for exact channel/supergroup message ids.
func (s *Service) GetMessageReactions(ctx context.Context, userID int64, req domain.ChannelMessageReactionsRequest) (domain.ChannelMessageReactionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.GetChannelMessageReactions(ctx, req)
}

// ListMessageReactions returns a bounded per-peer reaction list for one message.
func (s *Service) ListMessageReactions(ctx context.Context, userID int64, req domain.ChannelMessageReactionsListRequest) (domain.ChannelMessageReactionsList, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.MessageID <= 0 {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsList{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelMessageReactionListLimit)
	return s.channels.ListChannelMessageReactions(ctx, req)
}

type messageReactionUsageStore interface {
	RecordMessageReactionUse(ctx context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error
}

type participantReactionModeratorStore interface {
	DeleteChannelParticipantReaction(ctx context.Context, req domain.DeleteChannelParticipantReactionRequest) (domain.ChannelMessageReactionsResult, error)
	DeleteChannelParticipantReactions(ctx context.Context, req domain.DeleteChannelParticipantReactionsRequest) (domain.DeleteChannelParticipantReactionsResult, error)
}

// RecordMessageReactionUse updates account-level top/recent reaction lists.
func (s *Service) RecordMessageReactionUse(ctx context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error {
	if s == nil || s.channels == nil || userID == 0 || len(reactions) == 0 {
		return nil
	}
	recorder, ok := s.channels.(messageReactionUsageStore)
	if !ok {
		return nil
	}
	return recorder.RecordMessageReactionUse(ctx, userID, reactions, addToRecent, date)
}

// DeleteParticipantReaction removes one participant reaction on a channel message.
func (s *Service) DeleteParticipantReaction(ctx context.Context, userID int64, req domain.DeleteChannelParticipantReactionRequest) (domain.ChannelMessageReactionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.MessageID <= 0 || req.ParticipantUserID == 0 {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	moderator, ok := s.channels.(participantReactionModeratorStore)
	if !ok {
		return domain.ChannelMessageReactionsResult{}, domain.ErrChannelInvalid
	}
	return moderator.DeleteChannelParticipantReaction(ctx, req)
}

// DeleteParticipantReactions removes a bounded page of one participant's reactions.
func (s *Service) DeleteParticipantReactions(ctx context.Context, userID int64, req domain.DeleteChannelParticipantReactionsRequest) (domain.DeleteChannelParticipantReactionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 || req.Limit > domain.MaxDeleteParticipantReactionsBatch {
		req.Limit = domain.MaxDeleteParticipantReactionsBatch
	}
	moderator, ok := s.channels.(participantReactionModeratorStore)
	if !ok {
		return domain.DeleteChannelParticipantReactionsResult{}, domain.ErrChannelInvalid
	}
	return moderator.DeleteChannelParticipantReactions(ctx, req)
}

// TopReactions returns the current account's most frequently used message reactions.
func (s *Service) TopReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxTopMessageReactions {
		limit = domain.MaxTopMessageReactions
	}
	return s.channels.ListTopMessageReactions(ctx, userID, limit)
}

// RecentReactions returns the current account's recently used message reactions.
func (s *Service) RecentReactions(ctx context.Context, userID int64, limit int) ([]domain.MessageReaction, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.MessageReaction{}, nil
	}
	if limit > domain.MaxRecentMessageReactions {
		limit = domain.MaxRecentMessageReactions
	}
	return s.channels.ListRecentMessageReactions(ctx, userID, limit)
}

// ClearRecentReactions clears the current account's recently used message reactions.
func (s *Service) ClearRecentReactions(ctx context.Context, userID int64) error {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ErrChannelInvalid
	}
	return s.channels.ClearRecentMessageReactions(ctx, userID)
}

// SavedReactionTags returns account-level saved-message reaction tag titles.
func (s *Service) SavedReactionTags(ctx context.Context, userID int64, limit int) ([]domain.SavedReactionTag, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 {
		return []domain.SavedReactionTag{}, nil
	}
	if limit > domain.MaxSavedReactionTags {
		limit = domain.MaxSavedReactionTags
	}
	return s.channels.ListSavedReactionTags(ctx, userID, limit)
}

// UpdateSavedReactionTag stores the account-level custom title for one saved-message reaction tag.
func (s *Service) UpdateSavedReactionTag(ctx context.Context, userID int64, tag domain.SavedReactionTag) error {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ErrChannelInvalid
	}
	if tag.UserID == 0 {
		tag.UserID = userID
	}
	if tag.UserID != userID || tag.Reaction.Type != domain.MessageReactionEmoji || tag.Reaction.Emoticon == "" {
		return domain.ErrChannelInvalid
	}
	return s.channels.UpsertSavedReactionTag(ctx, tag)
}

// ReadMessageContents returns visible channel messages whose content-read state can be synced.
func (s *Service) ReadMessageContents(ctx context.Context, userID int64, req domain.ReadChannelMessageContentsRequest) (domain.ReadChannelMessageContentsResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.ReadChannelMessageContentsResult{}, domain.ErrChannelInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ReadChannelMessageContentsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.ReadChannelMessageContents(ctx, req)
}

// GetMessageAuthor resolves the user author for one visible channel/supergroup message.
func (s *Service) GetMessageAuthor(ctx context.Context, userID int64, req domain.GetChannelMessageAuthorRequest) (domain.GetChannelMessageAuthorResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.GetChannelMessageAuthorResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.GetChannelMessageAuthorResult{}, domain.ErrChannelInvalid
	}
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return domain.GetChannelMessageAuthorResult{}, domain.ErrMessageIDInvalid
	}
	history, err := s.channels.GetChannelMessages(ctx, userID, req.ChannelID, []int{req.ID})
	if err != nil {
		return domain.GetChannelMessageAuthorResult{}, err
	}
	if len(history.Messages) != 1 || history.Messages[0].SenderUserID == 0 {
		return domain.GetChannelMessageAuthorResult{}, domain.ErrMessageIDInvalid
	}
	return domain.GetChannelMessageAuthorResult{
		Channel:      history.Channel,
		MessageID:    history.Messages[0].ID,
		SenderUserID: history.Messages[0].SenderUserID,
	}, nil
}

// CreateForumTopic creates one forum topic root service message.
func (s *Service) CreateForumTopic(ctx context.Context, userID int64, req domain.CreateChannelForumTopicRequest) (domain.CreateChannelForumTopicResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || (strings.TrimSpace(req.Title) == "" && !req.TitleMissing) {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(req.Title) > domain.MaxChannelForumTopicTitleLength {
		return domain.CreateChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	return s.channels.CreateForumTopic(ctx, req)
}

// EditForumTopic edits topic metadata and emits a topic edit service message.
func (s *Service) EditForumTopic(ctx context.Context, userID int64, req domain.EditChannelForumTopicRequest) (domain.EditChannelForumTopicResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.TopicID <= 0 || req.TopicID > domain.MaxMessageBoxID {
		return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
	}
	if req.Title != nil {
		title := strings.TrimSpace(*req.Title)
		if title == "" || utf8.RuneCountInString(title) > domain.MaxChannelForumTopicTitleLength {
			return domain.EditChannelForumTopicResult{}, domain.ErrChannelInvalid
		}
		*req.Title = title
	}
	return s.channels.EditForumTopic(ctx, req)
}

// UpdatePinnedForumTopic pins or unpins one forum topic.
func (s *Service) UpdatePinnedForumTopic(ctx context.Context, userID int64, req domain.UpdateChannelForumTopicPinnedRequest) (domain.UpdateChannelForumTopicPinnedResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.TopicID <= 0 || req.TopicID > domain.MaxMessageBoxID {
		return domain.UpdateChannelForumTopicPinnedResult{}, domain.ErrChannelInvalid
	}
	return s.channels.UpdatePinnedForumTopic(ctx, req)
}

// ReorderPinnedForumTopics stores a bounded pinned topic order.
func (s *Service) ReorderPinnedForumTopics(ctx context.Context, userID int64, req domain.ReorderChannelPinnedForumTopicsRequest) (domain.ReorderChannelPinnedForumTopicsResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || len(req.Order) > domain.MaxChannelForumTopicIDs {
		return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrChannelInvalid
	}
	for _, id := range req.Order {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ReorderChannelPinnedForumTopicsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.ReorderPinnedForumTopics(ctx, req)
}

// DeleteForumTopicHistory deletes one bounded topic-history page.
func (s *Service) DeleteForumTopicHistory(ctx context.Context, userID int64, req domain.DeleteChannelForumTopicHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.TopicID <= 0 || req.TopicID > domain.MaxMessageBoxID {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	return s.channels.DeleteForumTopicHistory(ctx, req)
}

// GetForumTopics returns a bounded topic page without SQL OFFSET semantics.
func (s *Service) GetForumTopics(ctx context.Context, userID int64, filter domain.ChannelForumTopicFilter) (domain.ChannelForumTopicList, error) {
	if s == nil || s.channels == nil || userID == 0 || filter.ChannelID == 0 {
		return domain.ChannelForumTopicList{}, domain.ErrChannelInvalid
	}
	if filter.Limit < 0 || filter.Limit > domain.MaxChannelForumTopicsLimit {
		return domain.ChannelForumTopicList{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(filter.Query) > domain.MaxChannelHistoryQueryLength {
		return domain.ChannelForumTopicList{}, domain.ErrChannelInvalid
	}
	return s.channels.ListForumTopics(ctx, userID, filter)
}

// GetForumTopicsByID returns a bounded topic id lookup.
func (s *Service) GetForumTopicsByID(ctx context.Context, userID, channelID int64, ids []int) (domain.ChannelForumTopicList, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 || len(ids) > domain.MaxChannelForumTopicIDs {
		return domain.ChannelForumTopicList{}, domain.ErrChannelInvalid
	}
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelForumTopicList{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.GetForumTopicsByID(ctx, userID, channelID, ids)
}

// SendMessage sends one channel/supergroup message.
func (s *Service) SendMessage(ctx context.Context, userID int64, req domain.SendChannelMessageRequest) (domain.SendChannelMessageResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	return s.channels.SendChannelMessage(ctx, req)
}

// EditMessage edits a channel/supergroup text message.
func (s *Service) EditMessage(ctx context.Context, userID int64, req domain.EditChannelMessageRequest) (domain.EditChannelMessageResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelMessageResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.ID <= 0 {
		return domain.EditChannelMessageResult{}, domain.ErrChannelInvalid
	}
	return s.channels.EditChannelMessage(ctx, req)
}

// DeleteMessages deletes a bounded set of channel/supergroup messages.
func (s *Service) DeleteMessages(ctx context.Context, userID int64, req domain.DeleteChannelMessagesRequest) (domain.DeleteChannelMessagesResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || len(req.IDs) == 0 {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	if len(req.IDs) > domain.MaxDeleteMessageIDs {
		return domain.DeleteChannelMessagesResult{}, domain.ErrChannelInvalid
	}
	return s.channels.DeleteChannelMessages(ctx, req)
}

// DeleteHistory clears the current user's history view or deletes a bounded channel history page for everyone.
func (s *Service) DeleteHistory(ctx context.Context, userID int64, req domain.DeleteChannelHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	return s.channels.DeleteChannelHistory(ctx, req)
}

// DeleteParticipantHistory deletes one bounded page of messages sent by a channel participant.
func (s *Service) DeleteParticipantHistory(ctx context.Context, userID int64, req domain.DeleteChannelParticipantHistoryRequest) (domain.DeleteChannelHistoryResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.ParticipantUserID == 0 {
		return domain.DeleteChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	return s.channels.DeleteChannelParticipantHistory(ctx, req)
}

// UpdatePinnedMessage pins or unpins a channel/supergroup message.
func (s *Service) UpdatePinnedMessage(ctx context.Context, userID int64, req domain.UpdateChannelPinnedMessageRequest) (domain.UpdateChannelPinnedMessageResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.UpdateChannelPinnedMessageResult{}, domain.ErrMessageIDInvalid
	}
	return s.channels.UpdatePinnedMessage(ctx, req)
}

// ExportInvite exports a channel/supergroup invite link.
func (s *Service) ExportInvite(ctx context.Context, userID int64, req domain.ExportChannelInviteRequest) (domain.ExportChannelInviteResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || len(req.Title) > domain.MaxChannelInviteTitleLength {
		return domain.ExportChannelInviteResult{}, domain.ErrChannelInvalid
	}
	return s.channels.ExportInvite(ctx, req)
}

// CheckInvite checks an invite hash.
func (s *Service) CheckInvite(ctx context.Context, userID int64, hash string, date int) (domain.CheckChannelInviteResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CheckChannelInviteResult{}, domain.ErrChannelInvalid
	}
	if strings.TrimSpace(hash) == "" {
		return domain.CheckChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	return s.channels.CheckInvite(ctx, userID, strings.TrimSpace(hash), date)
}

// ImportInvite imports an invite and joins the channel if possible.
func (s *Service) ImportInvite(ctx context.Context, userID int64, req domain.ImportChannelInviteRequest) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || strings.TrimSpace(req.Hash) == "" {
		return domain.CreateChannelResult{}, domain.ErrInviteHashEmpty
	}
	req.Hash = strings.TrimSpace(req.Hash)
	return s.channels.ImportInvite(ctx, req)
}

// ListExportedInvites returns a bounded invite management page.
func (s *Service) ListExportedInvites(ctx context.Context, userID int64, req domain.ChannelInviteListRequest) (domain.ChannelInviteList, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelInviteList{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.AdminUserID == 0 || req.OffsetDate < 0 {
		return domain.ChannelInviteList{}, domain.ErrChannelInvalid
	}
	req.OffsetHash = strings.TrimSpace(req.OffsetHash)
	req.Limit = capLimit(req.Limit, domain.MaxChannelInviteListLimit)
	return s.channels.ListExportedInvites(ctx, req)
}

// GetExportedInvite returns one exported invite by hash.
func (s *Service) GetExportedInvite(ctx context.Context, userID int64, req domain.GetChannelInviteRequest) (domain.ChannelInvite, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelInvite{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	req.Hash = strings.TrimSpace(req.Hash)
	if req.UserID != userID || req.ChannelID == 0 || req.Hash == "" {
		return domain.ChannelInvite{}, domain.ErrInviteHashEmpty
	}
	return s.channels.GetExportedInvite(ctx, req)
}

// EditExportedInvite edits or revokes one exported invite.
func (s *Service) EditExportedInvite(ctx context.Context, userID int64, req domain.EditChannelInviteRequest) (domain.EditChannelInviteResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.EditChannelInviteResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	req.Hash = strings.TrimSpace(req.Hash)
	if req.UserID != userID || req.ChannelID == 0 || req.Hash == "" {
		return domain.EditChannelInviteResult{}, domain.ErrInviteHashEmpty
	}
	if req.ExpireDate < 0 || req.UsageLimit < 0 || len(req.Title) > domain.MaxChannelInviteTitleLength {
		return domain.EditChannelInviteResult{}, domain.ErrChannelInvalid
	}
	return s.channels.EditExportedInvite(ctx, req)
}

// DeleteExportedInvite deletes one exported invite.
func (s *Service) DeleteExportedInvite(ctx context.Context, userID int64, req domain.DeleteChannelInviteRequest) error {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	req.Hash = strings.TrimSpace(req.Hash)
	if req.UserID != userID || req.ChannelID == 0 || req.Hash == "" {
		return domain.ErrInviteHashEmpty
	}
	return s.channels.DeleteExportedInvite(ctx, req)
}

// DeleteRevokedExportedInvites deletes revoked invites for one admin.
func (s *Service) DeleteRevokedExportedInvites(ctx context.Context, userID int64, req domain.DeleteRevokedChannelInvitesRequest) error {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.AdminUserID == 0 {
		return domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelHideJoinRequests)
	return s.channels.DeleteRevokedExportedInvites(ctx, req)
}

// ListAdminsWithInvites returns admins that created invite links.
func (s *Service) ListAdminsWithInvites(ctx context.Context, userID, channelID int64) ([]domain.ChannelAdminInviteCount, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	return s.channels.ListAdminsWithInvites(ctx, userID, channelID)
}

// ListInviteImporters returns users joined/requested via invite links.
func (s *Service) ListInviteImporters(ctx context.Context, userID int64, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	req.Hash = strings.TrimSpace(req.Hash)
	req.Query = strings.TrimSpace(req.Query)
	if req.UserID != userID || req.ChannelID == 0 || req.OffsetDate < 0 {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(req.Query) > domain.MaxChannelParticipantsQueryLength {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelInviteListLimit)
	return s.channels.ListInviteImporters(ctx, req)
}

// PendingJoinRequests returns the bounded admin-facing join request summary.
func (s *Service) PendingJoinRequests(ctx context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error) {
	if s == nil || s.channels == nil || channelID == 0 {
		return domain.ChannelPendingJoinRequests{}, domain.ErrChannelInvalid
	}
	limit = capLimit(limit, domain.MaxChannelPendingJoinRecentRequesters)
	return s.channels.PendingJoinRequests(ctx, channelID, limit)
}

// HideChatJoinRequest approves or dismisses one pending join request.
func (s *Service) HideChatJoinRequest(ctx context.Context, userID int64, req domain.HideChannelJoinRequestRequest) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.ChannelID == 0 || req.TargetUserID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	return s.channels.HideChatJoinRequest(ctx, req)
}

// HideAllChatJoinRequests approves or dismisses pending join requests in a bounded batch.
func (s *Service) HideAllChatJoinRequests(ctx context.Context, userID int64, req domain.HideChannelJoinRequestsRequest) (domain.CreateChannelResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	req.Hash = strings.TrimSpace(req.Hash)
	if req.UserID != userID || req.ChannelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelHideJoinRequests)
	return s.channels.HideAllChatJoinRequests(ctx, req)
}

// ListDialogs returns current user's channel/supergroup dialog page.
func (s *Service) ListDialogs(ctx context.Context, userID int64, filter domain.DialogFilter) (domain.ChannelDialogList, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelDialogList{}, nil
	}
	return s.channels.ListChannelDialogs(ctx, userID, filter)
}

// GetDialogs returns channel/supergroup dialogs by IDs.
func (s *Service) GetDialogs(ctx context.Context, userID int64, channelIDs []int64) (domain.ChannelDialogList, error) {
	if s == nil || s.channels == nil || userID == 0 || len(channelIDs) == 0 {
		return domain.ChannelDialogList{}, nil
	}
	if len(channelIDs) > domain.MaxDialogFolderPeers {
		return domain.ChannelDialogList{}, domain.ErrChannelInvalid
	}
	return s.channels.GetChannelDialogs(ctx, userID, channelIDs)
}

// SetViewForumAsMessages stores the current user's local forum presentation mode.
func (s *Service) SetViewForumAsMessages(ctx context.Context, userID, channelID int64, enabled bool) (bool, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return false, domain.ErrChannelInvalid
	}
	return s.channels.SetChannelViewForumAsMessages(ctx, userID, channelID, enabled)
}

// CommonChannels returns megagroups shared by userID and req.TargetUserID.
func (s *Service) CommonChannels(ctx context.Context, userID int64, req domain.CommonChannelsRequest) (domain.CommonChannelsResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.CommonChannelsResult{}, nil
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.TargetUserID == 0 || req.TargetUserID == userID || req.MaxID < 0 {
		return domain.CommonChannelsResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxCommonChannelsLimit)
	return s.channels.ListCommonChannels(ctx, req)
}

// LeftChannels returns a bounded export page of channels/supergroups the user has left.
func (s *Service) LeftChannels(ctx context.Context, userID int64, offset, limit int) (domain.LeftChannelsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || offset < 0 || offset > domain.MaxLeftChannelsOffset {
		return domain.LeftChannelsResult{}, domain.ErrChannelInvalid
	}
	return s.channels.ListLeftChannels(ctx, userID, offset, capLimit(limit, domain.MaxLeftChannelsLimit))
}

// InactiveChannels returns least recently active channels/supergroups for limits UI.
func (s *Service) InactiveChannels(ctx context.Context, userID int64, limit int) (domain.ChannelDialogList, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelDialogList{}, domain.ErrChannelInvalid
	}
	return s.channels.ListInactiveChannels(ctx, userID, capLimit(limit, domain.MaxInactiveChannelsLimit))
}

// ChannelRecommendations returns public broadcast channels for TDesktop similar/recommended UI.
func (s *Service) ChannelRecommendations(ctx context.Context, userID int64, req domain.ChannelRecommendationsRequest) (domain.ChannelRecommendationsResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelRecommendationsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.SourceChannelID < 0 {
		return domain.ChannelRecommendationsResult{}, domain.ErrChannelInvalid
	}
	if req.Limit <= 0 {
		req.Limit = domain.DefaultChannelRecommendationsLimit
	} else {
		req.Limit = capLimit(req.Limit, domain.MaxChannelRecommendationsLimit)
	}
	return s.channels.ListChannelRecommendations(ctx, req)
}

// DiscussionGroups returns manageable megagroups that can be linked to a broadcast channel.
func (s *Service) DiscussionGroups(ctx context.Context, userID int64, limit int) ([]domain.Channel, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	return s.channels.ListDiscussionGroups(ctx, userID, capLimit(limit, domain.MaxDiscussionGroupsLimit))
}

// SetDiscussionGroup links or unlinks a broadcast channel and a discussion megagroup.
func (s *Service) SetDiscussionGroup(ctx context.Context, userID, broadcastID, groupID int64) (domain.DiscussionGroupUpdateResult, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrChannelInvalid
	}
	if broadcastID == 0 && groupID == 0 {
		return domain.DiscussionGroupUpdateResult{}, domain.ErrLinkNotModified
	}
	return s.channels.SetDiscussionGroup(ctx, userID, broadcastID, groupID)
}

// GetHistory returns a channel/supergroup history page.
func (s *Service) GetHistory(ctx context.Context, userID int64, filter domain.ChannelHistoryFilter) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 || filter.ChannelID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(filter.Query) > domain.MaxChannelHistoryQueryLength {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	filter.Limit = capLimit(filter.Limit, 100)
	return s.channels.ListChannelHistory(ctx, userID, filter)
}

// SearchPosts returns a bounded page of public channel/supergroup posts.
func (s *Service) SearchPosts(ctx context.Context, userID int64, req domain.ChannelSearchPostsRequest) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if req.OffsetRate < 0 || req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	if req.OffsetChannelID < 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if (strings.TrimSpace(req.Query) == "") == (strings.TrimSpace(req.Hashtag) == "") {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if strings.Contains(req.Hashtag, "#") ||
		utf8.RuneCountInString(req.Query) > domain.MaxChannelHistoryQueryLength ||
		utf8.RuneCountInString(req.Hashtag) > domain.MaxChannelHistoryQueryLength {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	req.Query = strings.TrimSpace(req.Query)
	req.Hashtag = strings.TrimSpace(req.Hashtag)
	req.Limit = capLimit(req.Limit, domain.MaxChannelSearchPostsLimit)
	return s.channels.SearchPublicPosts(ctx, userID, req)
}

// SearchJoinedMessages returns current-account-visible channel/supergroup hits for messages.searchGlobal.
func (s *Service) SearchJoinedMessages(ctx context.Context, userID int64, req domain.ChannelGlobalSearchRequest) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if req.OffsetRate < 0 || req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	if req.OffsetChannelID < 0 || req.MinDate < 0 || req.MaxDate < 0 || (req.HasFolderID && req.FolderID < 0) {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if req.BroadcastsOnly && req.GroupsOnly {
		return domain.ChannelHistory{}, nil
	}
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if utf8.RuneCountInString(req.Query) > domain.MaxChannelHistoryQueryLength {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelGlobalSearchLimit)
	return s.channels.SearchJoinedMessages(ctx, userID, req)
}

// GetReplies returns a bounded message thread page.
func (s *Service) GetReplies(ctx context.Context, userID int64, filter domain.ChannelRepliesFilter) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 || filter.ChannelID == 0 || filter.RootMessageID <= 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if filter.RootMessageID > domain.MaxMessageBoxID || filter.OffsetID < 0 || filter.OffsetID > domain.MaxMessageBoxID || filter.MaxID < 0 || filter.MaxID > domain.MaxMessageBoxID || filter.MinID < 0 || filter.MinID > domain.MaxMessageBoxID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	filter.Limit = capLimit(filter.Limit, domain.MaxChannelRepliesLimit)
	return s.channels.ListChannelReplies(ctx, userID, filter)
}

// GetUnreadMentions returns a bounded unread mention page for a channel/supergroup.
func (s *Service) GetUnreadMentions(ctx context.Context, userID int64, filter domain.ChannelUnreadMentionsFilter) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 || filter.ChannelID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if filter.TopMsgID < 0 || filter.TopMsgID > domain.MaxMessageBoxID ||
		filter.OffsetID < 0 || filter.OffsetID > domain.MaxMessageBoxID ||
		filter.MaxID < 0 || filter.MaxID > domain.MaxMessageBoxID ||
		filter.MinID < 0 || filter.MinID > domain.MaxMessageBoxID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	filter.Limit = capLimit(filter.Limit, domain.MaxChannelUnreadMentionsLimit)
	return s.channels.ListChannelUnreadMentions(ctx, userID, filter)
}

// ReadMentions clears unread mentions for a channel/supergroup in a bounded batch.
func (s *Service) ReadMentions(ctx context.Context, userID int64, req domain.ReadChannelMentionsRequest) (domain.ReadChannelMentionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.TopMsgID < 0 || req.TopMsgID > domain.MaxMessageBoxID {
		return domain.ReadChannelMentionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.ReadChannelMentionsResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelReadMentionsBatch)
	return s.channels.ReadChannelMentions(ctx, req)
}

// GetUnreadReactions returns a bounded unread reaction page for a channel/supergroup.
func (s *Service) GetUnreadReactions(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 || filter.ChannelID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if filter.TopMsgID < 0 || filter.TopMsgID > domain.MaxMessageBoxID ||
		filter.OffsetID < 0 || filter.OffsetID > domain.MaxMessageBoxID ||
		filter.MaxID < 0 || filter.MaxID > domain.MaxMessageBoxID ||
		filter.MinID < 0 || filter.MinID > domain.MaxMessageBoxID {
		return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	filter.Limit = capLimit(filter.Limit, domain.MaxChannelUnreadReactionsLimit)
	return s.channels.ListChannelUnreadReactions(ctx, userID, filter)
}

// ReadReactions clears unread reactions for a channel/supergroup in a bounded batch.
func (s *Service) ReadReactions(ctx context.Context, userID int64, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.TopMsgID < 0 || req.TopMsgID > domain.MaxMessageBoxID {
		return domain.ReadChannelReactionsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.ReadChannelReactionsResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelReadReactionsBatch)
	return s.channels.ReadChannelReactions(ctx, req)
}

// GetMessages returns a bounded exact-id channel/supergroup message batch.
func (s *Service) GetMessages(ctx context.Context, userID, channelID int64, ids []int) (domain.ChannelHistory, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	if len(ids) > domain.MaxGetMessageIDs {
		return domain.ChannelHistory{}, domain.ErrChannelInvalid
	}
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.ChannelHistory{}, domain.ErrMessageIDInvalid
		}
	}
	return s.channels.GetChannelMessages(ctx, userID, channelID, ids)
}

// GetDiscussionMessage returns the root message used to open a discussion thread.
func (s *Service) GetDiscussionMessage(ctx context.Context, userID, channelID int64, msgID int) (domain.ChannelDiscussionMessage, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return domain.ChannelDiscussionMessage{}, domain.ErrChannelInvalid
	}
	if msgID <= 0 || msgID > domain.MaxMessageBoxID {
		return domain.ChannelDiscussionMessage{}, domain.ErrMessageIDInvalid
	}
	return s.channels.GetDiscussionMessage(ctx, userID, channelID, msgID)
}

// ReadHistory advances current user's channel read watermark.
func (s *Service) ReadHistory(ctx context.Context, userID int64, req domain.ReadChannelHistoryRequest) (domain.ReadChannelHistoryResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.ReadChannelHistoryResult{}, domain.ErrChannelInvalid
	}
	return s.channels.ReadChannelHistory(ctx, req)
}

// GetMessageReadParticipants returns a bounded read receipt list for a small megagroup message.
func (s *Service) GetMessageReadParticipants(ctx context.Context, userID int64, req domain.ChannelReadParticipantsRequest) (domain.ChannelReadParticipantsResult, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 || req.MessageID <= 0 {
		return domain.ChannelReadParticipantsResult{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.MessageID > domain.MaxMessageBoxID {
		return domain.ChannelReadParticipantsResult{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelReadParticipants)
	return s.channels.ListMessageReadParticipants(ctx, req)
}

// ActiveChannelIDsForUser pages active joined channels for one user.
func (s *Service) ActiveChannelIDsForUser(ctx context.Context, userID, afterChannelID int64, limit int) ([]int64, error) {
	if s == nil || s.channels == nil || userID == 0 || afterChannelID < 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxSynchronousChannelDialogFanout {
		limit = domain.MaxSynchronousChannelDialogFanout
	}
	return s.channels.ListActiveChannelIDsForUser(ctx, userID, afterChannelID, limit)
}

// ActiveMemberIDs returns a bounded list for transient online fanout such as typing.
func (s *Service) ActiveMemberIDs(ctx context.Context, userID, channelID int64, limit int) ([]int64, error) {
	if s == nil || s.channels == nil || userID == 0 || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelTypingFanout {
		limit = domain.MaxChannelTypingFanout
	}
	return s.channels.ListActiveChannelMemberIDs(ctx, userID, channelID, limit)
}

// InviteAdminMemberIDs returns active admins that can manage join requests.
func (s *Service) InviteAdminMemberIDs(ctx context.Context, channelID int64, limit int) ([]int64, error) {
	if s == nil || s.channels == nil || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	return s.channels.ListChannelInviteAdminMemberIDs(ctx, channelID, limit)
}

// FilterActiveMemberIDs keeps only candidates that are active members of channelID.
// It is used by realtime fanout to intersect the online-user set with membership.
func (s *Service) FilterActiveMemberIDs(ctx context.Context, channelID int64, userIDs []int64) ([]int64, error) {
	if s == nil || s.channels == nil || channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	candidates := uniqueNonZero(userIDs)
	if len(candidates) == 0 {
		return nil, nil
	}
	return s.channels.FilterActiveChannelMemberIDs(ctx, channelID, candidates)
}

// GetDifference returns channel-scoped pts difference.
func (s *Service) GetDifference(ctx context.Context, userID int64, req domain.ChannelDifferenceRequest) (domain.ChannelDifference, error) {
	if s == nil || s.channels == nil || userID == 0 || req.ChannelID == 0 {
		return domain.ChannelDifference{}, domain.ErrChannelInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID {
		return domain.ChannelDifference{}, domain.ErrChannelInvalid
	}
	req.Limit = capLimit(req.Limit, domain.MaxChannelDifferenceLimit)
	return s.channels.ListChannelDifference(ctx, req)
}

func capLimit(limit, max int) int {
	if max <= 0 {
		return limit
	}
	if limit <= 0 {
		return max
	}
	if limit > max {
		return max
	}
	return limit
}

func uniqueNonZeroLimit(ids []int64, limit int) []int64 {
	if limit <= 0 {
		return nil
	}
	out := make([]int64, 0, minInt(len(ids), limit))
	seen := make(map[int64]struct{}, minInt(len(ids), limit))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func uniqueNonZero(ids []int64) []int64 {
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func normalizeChannelUsername(username string) string {
	username = strings.TrimSpace(username)
	username = strings.TrimPrefix(username, "@")
	return strings.TrimSpace(username)
}

func validChannelUsername(username string) bool {
	if len(username) < 5 || len(username) > 32 {
		return false
	}
	for i := 0; i < len(username); i++ {
		c := username[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		case c == '_':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
