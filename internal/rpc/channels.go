package rpc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"

	"telesrv/internal/domain"
)

const (
	maxChannelTitleLength                 = 128
	maxChannelAboutLength                 = 255
	maxChannelUsernameOrder               = 32
	maxChannelReportMessageIDs            = 100
	maxChannelSearchPostsLimit            = 50
	maxChannelSearchPostsQuery            = 256
	maxChannelPaidMessageStars            = 10000
	maxChannelBoostsToUnblockRestrictions = 8
	maxChatAvailableReactions             = 64
	maxChatInviteListLimit                = 100
	maxChatInviteLinkLength               = 256
	maxChatInviteSearchLength             = 256
)

// registerChannels 注册超级群/频道相关 RPC。messages.createChat 在这里注册，
// 因为 telesrv 将普通群创建直接实现为 megagroup。
func (r *Router) registerChannels(d *tg.ServerDispatcher) {
	d.OnMessagesCreateChat(r.onMessagesCreateChat)
	d.OnMessagesMigrateChat(r.onMessagesMigrateChat)
	d.OnMessagesGetChats(r.onMessagesGetChats)
	d.OnMessagesGetFullChat(r.onMessagesGetFullChat)
	d.OnMessagesAddChatUser(r.onMessagesAddChatUser)
	d.OnMessagesDeleteChatUser(r.onMessagesDeleteChatUser)
	d.OnMessagesEditChatTitle(r.onMessagesEditChatTitle)
	d.OnMessagesEditChatPhoto(r.onMessagesEditChatPhoto)
	d.OnMessagesEditChatAdmin(r.onMessagesEditChatAdmin)
	d.OnMessagesEditChatAbout(r.onMessagesEditChatAbout)
	d.OnMessagesEditChatDefaultBannedRights(r.onMessagesEditChatDefaultBannedRights)
	d.OnMessagesEditChatCreator(r.onMessagesEditChatCreator)
	d.OnMessagesEditChatParticipantRank(r.onMessagesEditChatParticipantRank)
	d.OnMessagesSetChatTheme(r.onMessagesSetChatTheme)
	d.OnMessagesToggleNoForwards(r.onMessagesToggleNoForwards)
	d.OnMessagesSetChatAvailableReactions(r.onMessagesSetChatAvailableReactions)
	d.OnChannelsCreateChannel(r.onChannelsCreateChannel)
	d.OnChannelsGetChannels(r.onChannelsGetChannels)
	d.OnChannelsGetFullChannel(r.onChannelsGetFullChannel)
	d.OnChannelsGetParticipants(r.onChannelsGetParticipants)
	d.OnChannelsGetParticipant(r.onChannelsGetParticipant)
	d.OnChannelsGetSendAs(r.onChannelsGetSendAs)
	d.OnChannelsCheckUsername(r.onChannelsCheckUsername)
	d.OnChannelsUpdateUsername(r.onChannelsUpdateUsername)
	d.OnChannelsGetAdminedPublicChannels(r.onChannelsGetAdminedPublicChannels)
	d.OnChannelsExportMessageLink(r.onChannelsExportMessageLink)
	d.OnChannelsToggleSignatures(r.onChannelsToggleSignatures)
	d.OnChannelsTogglePreHistoryHidden(r.onChannelsTogglePreHistoryHidden)
	d.OnChannelsToggleSlowMode(r.onChannelsToggleSlowMode)
	d.OnChannelsSetStickers(r.onChannelsSetStickers)
	d.OnChannelsSetEmojiStickers(r.onChannelsSetEmojiStickers)
	d.OnChannelsReorderUsernames(r.onChannelsReorderUsernames)
	d.OnChannelsToggleUsername(r.onChannelsToggleUsername)
	d.OnChannelsDeactivateAllUsernames(r.onChannelsDeactivateAllUsernames)
	d.OnChannelsUpdateColor(r.onChannelsUpdateColor)
	d.OnChannelsUpdateEmojiStatus(r.onChannelsUpdateEmojiStatus)
	d.OnChannelsReadMessageContents(r.onChannelsReadMessageContents)
	d.OnChannelsReportSpam(r.onChannelsReportSpam)
	d.OnChannelsGetLeftChannels(r.onChannelsGetLeftChannels)
	d.OnChannelsGetInactiveChannels(r.onChannelsGetInactiveChannels)
	d.OnChannelsGetGroupsForDiscussion(r.onChannelsGetGroupsForDiscussion)
	d.OnChannelsSetDiscussionGroup(r.onChannelsSetDiscussionGroup)
	d.OnChannelsEditLocation(r.onChannelsEditLocation)
	d.OnChannelsConvertToGigagroup(r.onChannelsConvertToGigagroup)
	d.OnChannelsDeleteParticipantHistory(r.onChannelsDeleteParticipantHistory)
	d.OnChannelsToggleJoinToSend(r.onChannelsToggleJoinToSend)
	d.OnChannelsToggleJoinRequest(r.onChannelsToggleJoinRequest)
	d.OnChannelsToggleForum(r.onChannelsToggleForum)
	d.OnChannelsToggleAntiSpam(r.onChannelsToggleAntiSpam)
	d.OnChannelsReportAntiSpamFalsePositive(r.onChannelsReportAntiSpamFalsePositive)
	d.OnChannelsToggleParticipantsHidden(r.onChannelsToggleParticipantsHidden)
	d.OnChannelsToggleViewForumAsMessages(r.onChannelsToggleViewForumAsMessages)
	d.OnChannelsGetChannelRecommendations(r.onChannelsGetChannelRecommendations)
	d.OnChannelsSetBoostsToUnblockRestrictions(r.onChannelsSetBoostsToUnblockRestrictions)
	d.OnChannelsRestrictSponsoredMessages(r.onChannelsRestrictSponsoredMessages)
	d.OnChannelsSearchPosts(r.onChannelsSearchPosts)
	d.OnChannelsUpdatePaidMessagesPrice(r.onChannelsUpdatePaidMessagesPrice)
	d.OnChannelsToggleAutotranslation(r.onChannelsToggleAutotranslation)
	d.OnChannelsGetMessageAuthor(r.onChannelsGetMessageAuthor)
	d.OnChannelsCheckSearchPostsFlood(r.onChannelsCheckSearchPostsFlood)
	d.OnChannelsSetMainProfileTab(r.onChannelsSetMainProfileTab)
	d.OnChannelsInviteToChannel(r.onChannelsInviteToChannel)
	d.OnChannelsJoinChannel(r.onChannelsJoinChannel)
	d.OnChannelsLeaveChannel(r.onChannelsLeaveChannel)
	d.OnChannelsEditAdmin(r.onChannelsEditAdmin)
	d.OnChannelsEditBanned(r.onChannelsEditBanned)
	d.OnChannelsEditTitle(r.onChannelsEditTitle)
	d.OnChannelsEditPhoto(r.onChannelsEditPhoto)
	d.OnChannelsDeleteChannel(r.onChannelsDeleteChannel)
	d.OnChannelsGetAdminLog(r.onChannelsGetAdminLog)
	d.OnChannelsReadHistory(r.onChannelsReadHistory)
	d.OnChannelsGetMessages(r.onChannelsGetMessages)
	d.OnChannelsDeleteMessages(r.onChannelsDeleteMessages)
	d.OnChannelsDeleteHistory(r.onChannelsDeleteHistory)
	d.OnMessagesUpdatePinnedMessage(r.onMessagesUpdatePinnedMessage)
	d.OnMessagesUnpinAllMessages(r.onMessagesUnpinAllMessages)
	d.OnMessagesExportChatInvite(r.onMessagesExportChatInvite)
	d.OnMessagesCheckChatInvite(r.onMessagesCheckChatInvite)
	d.OnMessagesImportChatInvite(r.onMessagesImportChatInvite)
	d.OnMessagesGetExportedChatInvites(r.onMessagesGetExportedChatInvites)
	d.OnMessagesGetExportedChatInvite(r.onMessagesGetExportedChatInvite)
	d.OnMessagesEditExportedChatInvite(r.onMessagesEditExportedChatInvite)
	d.OnMessagesDeleteRevokedExportedChatInvites(r.onMessagesDeleteRevokedExportedChatInvites)
	d.OnMessagesDeleteExportedChatInvite(r.onMessagesDeleteExportedChatInvite)
	d.OnMessagesGetAdminsWithInvites(r.onMessagesGetAdminsWithInvites)
	d.OnMessagesGetChatInviteImporters(r.onMessagesGetChatInviteImporters)
	d.OnMessagesHideChatJoinRequest(r.onMessagesHideChatJoinRequest)
	d.OnMessagesHideAllChatJoinRequests(r.onMessagesHideAllChatJoinRequests)
	d.OnUpdatesGetChannelDifference(r.onUpdatesGetChannelDifference)
}

func (r *Router) onMessagesCreateChat(ctx context.Context, req *tg.MessagesCreateChatRequest) (*tg.MessagesInvitedUsers, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if !validChannelTitle(req.Title) || len(req.Users) > domain.MaxChannelInviteUsers {
		return nil, channelInvalidErr(domain.ErrChannelTitleInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	memberIDs, err := r.userIDsFromInputUsers(ctx, userID, req.Users)
	if err != nil {
		return nil, err
	}
	memberIDs = createChatInviteMemberIDs(memberIDs, userID)
	date := int(r.clock.Now().Unix())
	r.log.Debug("messages.createChat resolved users",
		zap.Int("input_users", len(req.Users)),
		zap.Int("member_ids", len(memberIDs)),
		zap.Int64s("member_user_ids", memberIDs),
	)
	if len(memberIDs) == 0 {
		return nil, usersTooFewErr()
	}
	createRes, err := r.deps.Channels.CreateMegagroupFromCreateChat(ctx, userID, domain.CreateChannelRequest{
		CreatorUserID: userID,
		Title:         req.Title,
		TTLPeriod:     req.TTLPeriod,
		Date:          date,
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	r.addOnlineChannelMemberships(createRes.Channel.ID, channelMemberUserIDs(createRes.Members)...)

	responseRes := createRes
	var inviteRes domain.CreateChannelResult
	if len(memberIDs) > 0 {
		inviteRes, err = r.deps.Channels.InviteToChannel(ctx, userID, createRes.Channel.ID, memberIDs, date)
		if err != nil {
			return nil, channelInviteErr(err)
		}
		r.addOnlineChannelMemberships(inviteRes.Channel.ID, channelMemberUserIDs(inviteRes.Members)...)
		responseRes.Channel = inviteRes.Channel
		responseRes.Members = mergeChannelMembers(createRes.Members, inviteRes.Members)
		responseRes.Recipients = uniqueRecipientIDs(append(append([]int64{}, createRes.Recipients...), inviteRes.Recipients...))
	}

	updates := r.channelOperationUpdates(ctx, userID, responseRes)
	if tdesktopCreateChatNeedsLegacyChat(ctx) {
		updates = r.tdesktopCreateChatUpdates(ctx, userID, responseRes)
	}
	if inviteRes.Event.Pts != 0 {
		inviteUpdates := r.channelOperationUpdates(ctx, userID, inviteRes)
		if inviteUpdates != nil {
			updates.Updates = append(updates.Updates, inviteUpdates.Updates...)
		}
	}
	if inviteRes.Event.Pts != 0 {
		r.pushChannelExplicitUpdates(ctx, userID, inviteRes.Channel.ID, memberIDs, func(viewerUserID int64) *tg.Updates {
			return r.channelOperationUpdates(ctx, viewerUserID, inviteRes)
		})
	}
	return &tg.MessagesInvitedUsers{Updates: updates, MissingInvitees: []tg.MissingInvitee{}}, nil
}

func (r *Router) onMessagesMigrateChat(ctx context.Context, chatID int64) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if chatID <= 0 {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	userID, view, err := r.channelChangeInfoView(ctx, &tg.InputChannel{ChannelID: chatID})
	if err != nil {
		return nil, err
	}
	if !view.Channel.Megagroup {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	return r.channelStateUpdates(userID, view.Channel), nil
}

func (r *Router) onMessagesGetChats(ctx context.Context, ids []int64) (tg.MessagesChatsClass, error) {
	if len(ids) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	chats := make([]tg.ChatClass, 0, len(ids))
	if r.deps.Channels != nil {
		for _, id := range ids {
			if id <= 0 {
				continue
			}
			view, err := r.deps.Channels.GetChannel(ctx, userID, id)
			if err != nil {
				if isChannelNotFound(err) {
					continue
				}
				return nil, channelInvalidErr(err)
			}
			chats = append(chats, tgChannelChat(userID, view.Channel, &view.Self))
		}
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (r *Router) onMessagesGetFullChat(ctx context.Context, chatID int64) (*tg.MessagesChatFull, error) {
	if chatID <= 0 {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	return r.onChannelsGetFullChannel(ctx, &tg.InputChannel{ChannelID: chatID})
}

func (r *Router) onMessagesAddChatUser(ctx context.Context, req *tg.MessagesAddChatUserRequest) (*tg.MessagesInvitedUsers, error) {
	if req.ChatID <= 0 || req.UserID == nil {
		return nil, peerIDInvalidErr()
	}
	return r.onChannelsInviteToChannel(ctx, &tg.ChannelsInviteToChannelRequest{
		Channel: &tg.InputChannel{ChannelID: req.ChatID},
		Users:   []tg.InputUserClass{req.UserID},
	})
}

func (r *Router) onMessagesDeleteChatUser(ctx context.Context, req *tg.MessagesDeleteChatUserRequest) (tg.UpdatesClass, error) {
	if req.ChatID <= 0 || req.UserID == nil {
		return nil, peerIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	target, found, err := r.userFromInput(ctx, userID, req.UserID)
	if err != nil {
		return nil, internalErr()
	}
	if !found || target.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	if target.ID == userID {
		return r.onChannelsLeaveChannel(ctx, &tg.InputChannel{ChannelID: req.ChatID})
	}
	return r.onChannelsEditBanned(ctx, &tg.ChannelsEditBannedRequest{
		Channel:     &tg.InputChannel{ChannelID: req.ChatID},
		Participant: &tg.InputPeerUser{UserID: target.ID, AccessHash: target.AccessHash},
		BannedRights: tg.ChatBannedRights{
			ViewMessages: true,
			UntilDate:    0,
		},
	})
}

func (r *Router) onMessagesEditChatTitle(ctx context.Context, req *tg.MessagesEditChatTitleRequest) (tg.UpdatesClass, error) {
	if req.ChatID <= 0 {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	return r.onChannelsEditTitle(ctx, &tg.ChannelsEditTitleRequest{
		Channel: &tg.InputChannel{ChannelID: req.ChatID},
		Title:   req.Title,
	})
}

func (r *Router) onMessagesEditChatPhoto(ctx context.Context, req *tg.MessagesEditChatPhotoRequest) (tg.UpdatesClass, error) {
	if req.ChatID <= 0 || req.Photo == nil {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	return r.onChannelsEditPhoto(ctx, &tg.ChannelsEditPhotoRequest{
		Channel: &tg.InputChannel{ChannelID: req.ChatID},
		Photo:   req.Photo,
	})
}

func (r *Router) onMessagesEditChatAdmin(ctx context.Context, req *tg.MessagesEditChatAdminRequest) (bool, error) {
	if req.ChatID <= 0 || req.UserID == nil {
		return false, peerIDInvalidErr()
	}
	rights := tg.ChatAdminRights{}
	if req.IsAdmin {
		rights = legacyBasicGroupAdminRights()
	}
	_, err := r.onChannelsEditAdmin(ctx, &tg.ChannelsEditAdminRequest{
		Channel:     &tg.InputChannel{ChannelID: req.ChatID},
		UserID:      req.UserID,
		AdminRights: rights,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesEditChatAbout(ctx context.Context, req *tg.MessagesEditChatAboutRequest) (bool, error) {
	if r.deps.Channels == nil {
		return false, notImplementedErr()
	}
	if utf8.RuneCountInString(req.About) > maxChannelAboutLength {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	channelID, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer)
	if err != nil {
		return false, err
	}
	channel, err := r.deps.Channels.EditAbout(ctx, userID, domain.EditChannelAboutRequest{
		UserID:    userID,
		ChannelID: channelID,
		About:     req.About,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return false, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return true, nil
}

func (r *Router) onMessagesEditChatDefaultBannedRights(ctx context.Context, req *tg.MessagesEditChatDefaultBannedRightsRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	channel, err := r.deps.Channels.EditDefaultBannedRights(ctx, userID, domain.EditChannelDefaultBannedRightsRequest{
		UserID:       userID,
		ChannelID:    peer.ID,
		BannedRights: domainChannelBannedRights(req.BannedRights),
		Date:         int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, channel)
	r.pushChannelStateToMembers(ctx, userID, channel)
	return updates, nil
}

func (r *Router) onMessagesEditChatCreator(ctx context.Context, req *tg.MessagesEditChatCreatorRequest) (tg.UpdatesClass, error) {
	if req.UserID == nil {
		return nil, peerIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if _, found, err := r.userFromInput(ctx, userID, req.UserID); err != nil {
		return nil, internalErr()
	} else if !found {
		return nil, peerIDInvalidErr()
	}
	return nil, tgerr.New(400, "PASSWORD_HASH_INVALID")
}

func (r *Router) onMessagesEditChatParticipantRank(ctx context.Context, req *tg.MessagesEditChatParticipantRankRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if len(req.Rank) > domain.MaxChannelAdminRankLength {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	participant, ok := r.domainPeerFromInputPeer(userID, req.Participant)
	if !ok || participant.Type != domain.PeerTypeUser || participant.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	member, err := r.deps.Channels.GetParticipant(ctx, userID, channelID, participant.ID)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	return r.onChannelsEditAdmin(ctx, &tg.ChannelsEditAdminRequest{
		Channel:     &tg.InputChannel{ChannelID: channelID},
		UserID:      &tg.InputUser{UserID: participant.ID},
		AdminRights: tgChatAdminRights(member.AdminRights),
		Rank:        req.Rank,
	})
}

func (r *Router) onMessagesSetChatTheme(ctx context.Context, req *tg.MessagesSetChatThemeRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if channelID, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer); err == nil {
		if r.deps.Channels == nil {
			return nil, notImplementedErr()
		}
		view, err := r.deps.Channels.GetChannel(ctx, userID, channelID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return r.channelStateUpdates(userID, view.Channel), nil
	} else if _, ok := channelIDFromLegacyInputPeer(userID, req.Peer); ok {
		return nil, err
	}
	peer, ok := r.domainPeerFromInputPeer(userID, req.Peer)
	if !ok || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}

func (r *Router) onMessagesToggleNoForwards(ctx context.Context, req *tg.MessagesToggleNoForwardsRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetNoForwards(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, channel)
	r.pushChannelStateToMembers(ctx, userID, channel)
	return updates, nil
}

func (r *Router) onMessagesSetChatAvailableReactions(ctx context.Context, req *tg.MessagesSetChatAvailableReactionsRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromLegacyInputPeerChecked(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	policy, err := domainChannelReactionPolicy(req)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetAvailableReactions(ctx, userID, channelID, policy)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, channel)
	r.pushChannelStateToMembers(ctx, userID, channel)
	return updates, nil
}

func (r *Router) onChannelsCreateChannel(ctx context.Context, req *tg.ChannelsCreateChannelRequest) (tg.UpdatesClass, error) {
	if err := validateChannelsCreateChannelOptions(req); err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if !validChannelTitle(req.Title) || utf8.RuneCountInString(req.About) > maxChannelAboutLength {
		return nil, channelInvalidErr(domain.ErrChannelTitleInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.CreateChannel(ctx, userID, domain.CreateChannelRequest{
		CreatorUserID: userID,
		Title:         req.Title,
		About:         req.About,
		Broadcast:     req.Broadcast,
		Megagroup:     req.Megagroup,
		Forum:         req.Forum,
		ForumTabs:     req.Forum,
		TTLPeriod:     req.TTLPeriod,
		Date:          int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func validateChannelsCreateChannelOptions(req *tg.ChannelsCreateChannelRequest) error {
	if req == nil {
		return inputRequestInvalidErr()
	}
	if req.ForImport {
		return chatInvalidErr()
	}
	if req.GeoPoint != nil || req.Address != "" {
		return addressInvalidErr()
	}
	if req.TTLPeriod < 0 {
		return ttlPeriodInvalidErr()
	}
	return nil
}

func (r *Router) onChannelsGetChannels(ctx context.Context, ids []tg.InputChannelClass) (tg.MessagesChatsClass, error) {
	if len(ids) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	chats := make([]tg.ChatClass, 0, len(ids))
	for _, input := range ids {
		ref, ok := inputChannelRef(input)
		if !ok || ref.ID == 0 || r.deps.Channels == nil {
			continue
		}
		view, err := r.deps.Channels.GetChannel(ctx, userID, ref.ID)
		if err != nil {
			if isChannelNotFound(err) {
				continue
			}
			return nil, internalErr()
		}
		if !inputChannelAccessHashMatches(ref, view.Channel) {
			continue
		}
		chats = append(chats, tgChannelChat(userID, view.Channel, &view.Self))
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (r *Router) onChannelsGetFullChannel(ctx context.Context, input tg.InputChannelClass) (*tg.MessagesChatFull, error) {
	if r.deps.Channels == nil {
		return &tg.MessagesChatFull{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	_, view, err := r.channelView(ctx, input)
	if err != nil {
		return nil, err
	}
	full := tgChannelFull(view)
	userIDs := []int64{view.Channel.CreatorUserID, view.Self.UserID}
	if canViewChannelJoinRequests(view.Self) {
		userIDs = r.applyPendingJoinRequestsToFullChannel(ctx, full, view.Channel.ID, userIDs)
	}
	r.trackChannelInterest(ctx, userID, view.Channel.ID)
	return &tg.MessagesChatFull{
		FullChat: full,
		Chats:    []tg.ChatClass{tgChannelChat(userID, view.Channel, &view.Self)},
		Users:    r.tgUsersForIDs(ctx, userID, userIDs),
	}, nil
}

func (r *Router) onChannelsGetSendAs(ctx context.Context, req *tg.ChannelsGetSendAsRequest) (*tg.ChannelsSendAsPeers, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, peerIDInvalidErr()
	}
	peer, ok := r.domainPeerFromInputPeer(userID, req.Peer)
	if !ok || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	chats := []tg.ChatClass(nil)
	peers := []tg.SendAsPeer{{Peer: &tg.PeerUser{UserID: userID}}}
	if peer.Type == domain.PeerTypeChannel {
		if r.deps.Channels == nil {
			return &tg.ChannelsSendAsPeers{}, nil
		}
		view, err := r.deps.Channels.GetChannel(ctx, userID, peer.ID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if ref, ok := inputPeerChannelRef(req.Peer); ok {
			if ref.ID != view.Channel.ID || (ref.CheckAccessHash && !inputChannelAccessHashMatches(ref, view.Channel)) {
				return nil, channelInvalidErr(domain.ErrChannelPrivate)
			}
		}
		chats = []tg.ChatClass{tgChannelChat(userID, view.Channel, &view.Self)}
		if canCurrentChannelSendAs(view) {
			peers = append(peers, tg.SendAsPeer{Peer: &tg.PeerChannel{ChannelID: view.Channel.ID}})
		}
	}
	return &tg.ChannelsSendAsPeers{
		Peers: peers,
		Chats: chats,
		Users: r.tgUsersForIDs(ctx, userID, []int64{userID}),
	}, nil
}

func (r *Router) onChannelsCheckUsername(ctx context.Context, req *tg.ChannelsCheckUsernameRequest) (bool, error) {
	if r.deps.Channels == nil {
		return false, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return false, err
	}
	okUsername, err := r.deps.Channels.CheckUsername(ctx, userID, channelID, req.Username)
	if err != nil {
		return false, channelUsernameErr(err)
	}
	return okUsername, nil
}

func (r *Router) onChannelsUpdateUsername(ctx context.Context, req *tg.ChannelsUpdateUsernameRequest) (bool, error) {
	if r.deps.Channels == nil {
		return false, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return false, err
	}
	channel, err := r.deps.Channels.UpdateUsername(ctx, userID, domain.UpdateChannelUsernameRequest{
		UserID:    userID,
		ChannelID: channelID,
		Username:  req.Username,
	})
	if err != nil {
		return false, channelUsernameErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return true, nil
}

func (r *Router) onChannelsGetAdminedPublicChannels(ctx context.Context, req *tg.ChannelsGetAdminedPublicChannelsRequest) (tg.MessagesChatsClass, error) {
	if r.deps.Channels == nil {
		return &tg.MessagesChats{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.ByLocation {
		return &tg.MessagesChats{}, nil
	}
	channels, err := r.deps.Channels.ListAdminedPublicChannels(ctx, userID)
	if err != nil {
		return nil, internalErr()
	}
	return &tg.MessagesChats{Chats: tgChannels(userID, channels)}, nil
}

func (r *Router) onChannelsExportMessageLink(ctx context.Context, req *tg.ChannelsExportMessageLinkRequest) (*tg.ExportedMessageLink, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	userID, view, err := r.channelView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	history, err := r.deps.Channels.GetMessages(ctx, userID, view.Channel.ID, []int{req.ID})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	if len(history.Messages) != 1 || history.Messages[0].ID != req.ID {
		return nil, messageIDInvalidErr()
	}
	link := ""
	if view.Channel.Username != "" {
		link = "https://t.me/" + view.Channel.Username + "/" + strconv.Itoa(req.ID)
	} else {
		link = "https://t.me/c/" + strconv.FormatInt(view.Channel.ID, 10) + "/" + strconv.Itoa(req.ID)
	}
	if req.Thread {
		if rootID := channelMessageThreadRootID(history.Messages[0]); rootID > 0 && rootID != req.ID {
			link += "?thread=" + strconv.Itoa(rootID)
		}
	}
	return &tg.ExportedMessageLink{Link: link, HTML: ""}, nil
}

func channelMessageThreadRootID(msg domain.ChannelMessage) int {
	if msg.ReplyTo == nil {
		return 0
	}
	if msg.ReplyTo.TopMessageID > 0 {
		return msg.ReplyTo.TopMessageID
	}
	return msg.ReplyTo.MessageID
}

func (r *Router) onChannelsToggleSignatures(ctx context.Context, req *tg.ChannelsToggleSignaturesRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetSignatures(ctx, userID, channelID, req.SignaturesEnabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, channel)
	r.pushChannelStateToMembers(ctx, userID, channel)
	return updates, nil
}

func (r *Router) onChannelsTogglePreHistoryHidden(ctx context.Context, req *tg.ChannelsTogglePreHistoryHiddenRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	viewerUserID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, viewerUserID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetPreHistoryHidden(ctx, viewerUserID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(viewerUserID, channel)
	r.pushChannelStateToMembers(ctx, viewerUserID, channel)
	return updates, nil
}

func (r *Router) onChannelsToggleSlowMode(ctx context.Context, req *tg.ChannelsToggleSlowModeRequest) (tg.UpdatesClass, error) {
	if !domain.ValidChannelSlowModeSeconds(req.Seconds) {
		return nil, secondsInvalidErr()
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	viewerUserID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, viewerUserID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetSlowMode(ctx, viewerUserID, channelID, req.Seconds)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(viewerUserID, channel)
	r.pushChannelStateToMembers(ctx, viewerUserID, channel)
	return updates, nil
}

func (r *Router) onChannelsSetStickers(ctx context.Context, req *tg.ChannelsSetStickersRequest) (bool, error) {
	_, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return false, err
	}
	if !view.Channel.Megagroup || view.Channel.Broadcast {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	if err := validateEmptyChannelStickerSet(req.Stickerset); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsSetEmojiStickers(ctx context.Context, req *tg.ChannelsSetEmojiStickersRequest) (bool, error) {
	_, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return false, err
	}
	if !view.Channel.Megagroup || view.Channel.Broadcast {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	if err := validateEmptyChannelStickerSet(req.Stickerset); err != nil {
		return false, err
	}
	return true, nil
}

func validateEmptyChannelStickerSet(stickerset tg.InputStickerSetClass) error {
	if _, ok := stickerset.(*tg.InputStickerSetEmpty); ok {
		return nil
	}
	return stickersetInvalidErr()
}

func (r *Router) onChannelsReorderUsernames(ctx context.Context, req *tg.ChannelsReorderUsernamesRequest) (bool, error) {
	if len(req.Order) > maxChannelUsernameOrder {
		return false, limitInvalidErr()
	}
	if _, _, err := r.channelChangeInfoView(ctx, req.Channel); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsToggleUsername(ctx context.Context, req *tg.ChannelsToggleUsernameRequest) (bool, error) {
	if req.Username != "" && !validChannelManagementUsername(req.Username) {
		return false, usernameInvalidErr()
	}
	if _, _, err := r.channelChangeInfoView(ctx, req.Channel); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsDeactivateAllUsernames(ctx context.Context, input tg.InputChannelClass) (bool, error) {
	if _, _, err := r.channelChangeInfoView(ctx, input); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsUpdateColor(ctx context.Context, req *tg.ChannelsUpdateColorRequest) (tg.UpdatesClass, error) {
	viewerUserID, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetColor(ctx, viewerUserID, view.Channel.ID, req.ForProfile, domainPeerColorFromChannelUpdate(req))
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(viewerUserID, channel)
	r.pushChannelStateToMembers(ctx, viewerUserID, channel)
	return updates, nil
}

func (r *Router) onChannelsUpdateEmojiStatus(ctx context.Context, req *tg.ChannelsUpdateEmojiStatusRequest) (tg.UpdatesClass, error) {
	viewerUserID, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	status, err := domainChannelEmojiStatus(req.EmojiStatus)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetEmojiStatus(ctx, viewerUserID, view.Channel.ID, status)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(viewerUserID, channel)
	r.pushChannelStateToMembers(ctx, viewerUserID, channel)
	return updates, nil
}

func (r *Router) onChannelsReadMessageContents(ctx context.Context, req *tg.ChannelsReadMessageContentsRequest) (bool, error) {
	if r.deps.Channels == nil {
		return false, notImplementedErr()
	}
	if req == nil {
		return false, inputRequestInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return false, err
	}
	read, err := r.deps.Channels.ReadMessageContents(ctx, userID, domain.ReadChannelMessageContentsRequest{
		UserID:    userID,
		ChannelID: channelID,
		IDs:       req.ID,
	})
	if err != nil {
		if errors.Is(err, domain.ErrMessageIDInvalid) {
			return false, messageIDInvalidErr()
		}
		return false, channelInvalidErr(err)
	}
	if ids := readChannelMessageContentIDs(read.Messages); len(ids) > 0 {
		r.pushUserUpdates(ctx, userID, &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateChannelReadMessagesContents{
				ChannelID: read.Channel.ID,
				Messages:  ids,
			}},
			Users: []tg.UserClass{},
			Chats: []tg.ChatClass{tgChannelChat(userID, read.Channel, nil)},
			Date:  int(r.clock.Now().Unix()),
			Seq:   0,
		})
	}
	if len(read.ClearedUnreadReactionMessageIDs) > 0 {
		r.pushUserUpdates(ctx, userID, r.channelMessagesReactionsUpdates(ctx, userID, domain.ChannelMessageReactionsResult{
			Channel:  read.Channel,
			Messages: read.Messages,
		}, read.ClearedUnreadReactionMessageIDs))
	}
	return true, nil
}

func readChannelMessageContentIDs(messages []domain.ChannelMessage) []int {
	if len(messages) == 0 {
		return nil
	}
	ids := make([]int, 0, len(messages))
	seen := make(map[int]struct{}, len(messages))
	for _, msg := range messages {
		if msg.ID <= 0 {
			continue
		}
		if _, ok := seen[msg.ID]; ok {
			continue
		}
		seen[msg.ID] = struct{}{}
		ids = append(ids, msg.ID)
	}
	sort.Ints(ids)
	return ids
}

func (r *Router) onChannelsReportSpam(ctx context.Context, req *tg.ChannelsReportSpamRequest) (bool, error) {
	if len(req.ID) > maxChannelReportMessageIDs {
		return false, limitInvalidErr()
	}
	for _, id := range req.ID {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return false, messageIDInvalidErr()
		}
	}
	if _, _, err := r.channelView(ctx, req.Channel); err != nil {
		return false, err
	}
	if peer, ok := r.domainPeerFromInputPeer(0, req.Participant); !ok || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return false, peerIDInvalidErr()
	}
	return true, nil
}

func (r *Router) onChannelsGetLeftChannels(ctx context.Context, offset int) (tg.MessagesChatsClass, error) {
	if offset < 0 || offset > domain.MaxLeftChannelsOffset {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return &tg.MessagesChats{Chats: []tg.ChatClass{}}, nil
	}
	list, err := r.deps.Channels.LeftChannels(ctx, userID, offset, domain.MaxLeftChannelsLimit)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	chats := make([]tg.ChatClass, 0, len(list.Channels))
	for _, item := range list.Channels {
		chats = append(chats, tgChannelChat(userID, item.Channel, &item.Self))
	}
	if len(chats) == 0 && list.Count > 0 {
		return &tg.MessagesChatsSlice{Count: list.Count, Chats: chats}, nil
	}
	if offset+len(chats) < list.Count {
		return &tg.MessagesChatsSlice{Count: list.Count, Chats: chats}, nil
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (r *Router) onChannelsGetInactiveChannels(ctx context.Context) (*tg.MessagesInactiveChats, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return &tg.MessagesInactiveChats{Dates: []int{}, Chats: []tg.ChatClass{}, Users: []tg.UserClass{}}, nil
	}
	list, err := r.deps.Channels.InactiveChannels(ctx, userID, domain.MaxInactiveChannelsLimit)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	dates := make([]int, 0, len(list.Channels))
	chats := make([]tg.ChatClass, 0, len(list.Channels))
	for i, channel := range list.Channels {
		date := channel.Date
		if i < len(list.Dialogs) && list.Dialogs[i].TopMessageDate > 0 {
			date = list.Dialogs[i].TopMessageDate
		}
		dates = append(dates, date)
		chats = append(chats, tgChannelChat(userID, channel, nil))
	}
	return &tg.MessagesInactiveChats{Dates: dates, Chats: chats, Users: []tg.UserClass{}}, nil
}

func (r *Router) onChannelsGetGroupsForDiscussion(ctx context.Context) (tg.MessagesChatsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if r.deps.Channels == nil {
		return &tg.MessagesChats{Chats: []tg.ChatClass{}}, nil
	}
	channels, err := r.deps.Channels.DiscussionGroups(ctx, userID, domain.MaxDiscussionGroupsLimit)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	return &tg.MessagesChats{Chats: tgChannels(userID, channels)}, nil
}

func (r *Router) onChannelsSetDiscussionGroup(ctx context.Context, req *tg.ChannelsSetDiscussionGroupRequest) (bool, error) {
	if r.deps.Channels == nil {
		return false, notImplementedErr()
	}
	if req == nil {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	broadcastID, err := r.optionalChannelIDFromInput(ctx, userID, req.Broadcast)
	if err != nil {
		return false, channelDiscussionErr(err)
	}
	groupID, err := r.optionalChannelIDFromInput(ctx, userID, req.Group)
	if err != nil {
		return false, channelDiscussionErr(err)
	}
	res, err := r.deps.Channels.SetDiscussionGroup(ctx, userID, broadcastID, groupID)
	if err != nil {
		return false, channelDiscussionErr(err)
	}
	for _, channel := range res.Channels {
		r.pushChannelStateToMembers(ctx, userID, channel)
	}
	return true, nil
}

func (r *Router) onChannelsEditLocation(ctx context.Context, req *tg.ChannelsEditLocationRequest) (bool, error) {
	if _, _, err := r.channelChangeInfoView(ctx, req.Channel); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsConvertToGigagroup(ctx context.Context, input tg.InputChannelClass) (tg.UpdatesClass, error) {
	userID, view, err := r.channelChangeInfoView(ctx, input)
	if err != nil {
		return nil, err
	}
	return r.channelStateUpdates(userID, view.Channel), nil
}

func (r *Router) onChannelsDeleteParticipantHistory(ctx context.Context, req *tg.ChannelsDeleteParticipantHistoryRequest) (*tg.MessagesAffectedHistory, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	peer, ok := r.domainPeerFromInputPeer(0, req.Participant)
	if !ok || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	res, err := r.deps.Channels.DeleteParticipantHistory(ctx, userID, domain.DeleteChannelParticipantHistoryRequest{
		UserID:            userID,
		ChannelID:         channelID,
		ParticipantUserID: peer.ID,
		Date:              int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelDeleteErr(err)
	}
	if res.Event.Pts != 0 {
		r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelDeleteMessagesUpdates(viewerUserID, res.Channel, res.Event)
		})
	}
	return &tg.MessagesAffectedHistory{Pts: res.Channel.Pts, PtsCount: res.Event.PtsCount, Offset: res.Offset}, nil
}

func (r *Router) onChannelsToggleJoinToSend(ctx context.Context, req *tg.ChannelsToggleJoinToSendRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetJoinToSend(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsToggleJoinRequest(ctx context.Context, req *tg.ChannelsToggleJoinRequestRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetJoinRequest(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsToggleForum(ctx context.Context, req *tg.ChannelsToggleForumRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetForum(ctx, userID, channelID, req.Enabled, req.Tabs)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsToggleAntiSpam(ctx context.Context, req *tg.ChannelsToggleAntiSpamRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetAntiSpam(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsReportAntiSpamFalsePositive(ctx context.Context, req *tg.ChannelsReportAntiSpamFalsePositiveRequest) (bool, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return false, messageIDInvalidErr()
	}
	if _, _, err := r.channelChangeInfoView(ctx, req.Channel); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsToggleParticipantsHidden(ctx context.Context, req *tg.ChannelsToggleParticipantsHiddenRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetParticipantsHidden(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsToggleViewForumAsMessages(ctx context.Context, req *tg.ChannelsToggleViewForumAsMessagesRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	changed, err := r.deps.Channels.SetViewForumAsMessages(ctx, userID, channelID, req.Enabled)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	if !changed {
		return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
	}
	event := domain.UpdateEvent{
		Type:     domain.UpdateEventChannelViewForum,
		Peer:     domain.Peer{Type: domain.PeerTypeChannel, ID: channelID},
		Bool:     req.Enabled,
		PtsCount: 1,
		Date:     int(r.clock.Now().Unix()),
	}
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		event, _, err = r.deps.Updates.RecordChannelViewForumAsMessages(ctx, authKeyID, userID, channelID, req.Enabled, sessionID)
		if err != nil {
			return nil, internalErr()
		}
	}
	out := tgUpdateForOutboxEvent(event)
	if out == nil {
		out = tgEmptyUpdates(event.Date)
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, out)
	return out, nil
}

func (r *Router) onChannelsGetChannelRecommendations(ctx context.Context, req *tg.ChannelsGetChannelRecommendationsRequest) (tg.MessagesChatsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if r.deps.Channels == nil {
		return &tg.MessagesChats{Chats: []tg.ChatClass{}}, nil
	}
	sourceChannelID := int64(0)
	if req != nil {
		if input, ok := req.GetChannel(); ok {
			source, err := r.publicRecommendationSourceChannel(ctx, userID, input)
			if err != nil {
				return nil, err
			}
			sourceChannelID = source
		}
	}
	res, err := r.deps.Channels.ChannelRecommendations(ctx, userID, domain.ChannelRecommendationsRequest{
		UserID:          userID,
		SourceChannelID: sourceChannelID,
		Limit:           domain.DefaultChannelRecommendationsLimit,
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	chats := tgChannels(userID, res.Channels)
	if res.Count > len(chats) {
		return &tg.MessagesChatsSlice{Count: res.Count, Chats: chats}, nil
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (r *Router) publicRecommendationSourceChannel(ctx context.Context, userID int64, input tg.InputChannelClass) (int64, error) {
	ref, ok := inputChannelRef(input)
	if !ok {
		return 0, channelInvalidErr(domain.ErrChannelInvalid)
	}
	channel, err := r.deps.Channels.GetJoinableChannel(ctx, userID, ref.ID)
	if err != nil {
		return 0, channelInvalidErr(err)
	}
	if !inputChannelAccessHashMatches(ref, channel) {
		return 0, channelInvalidErr(domain.ErrChannelPrivate)
	}
	if channel.Deleted || !channel.Broadcast || channel.Megagroup || channel.Username == "" {
		return 0, channelInvalidErr(domain.ErrChannelInvalid)
	}
	return channel.ID, nil
}

func (r *Router) onChannelsSetBoostsToUnblockRestrictions(ctx context.Context, req *tg.ChannelsSetBoostsToUnblockRestrictionsRequest) (tg.UpdatesClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if req.Boosts < 0 || req.Boosts > maxChannelBoostsToUnblockRestrictions {
		return nil, limitInvalidErr()
	}
	return r.channelStateCompatUpdate(ctx, req.Channel)
}

func (r *Router) onChannelsRestrictSponsoredMessages(ctx context.Context, req *tg.ChannelsRestrictSponsoredMessagesRequest) (tg.UpdatesClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetRestrictedSponsored(ctx, userID, view.Channel.ID, req.Restricted)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsSearchPosts(ctx context.Context, req *tg.ChannelsSearchPostsRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateChannelSearchPostsRequest(req); err != nil {
		return nil, err
	}
	offsetChannelID, err := r.searchPostsOffsetChannelID(ctx, userID, req.OffsetPeer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return &tg.MessagesMessages{Messages: []tg.MessageClass{}, Chats: []tg.ChatClass{}, Users: []tg.UserClass{}}, nil
	}
	hashtag, query := channelSearchPostsTerms(req)
	history, err := r.deps.Channels.SearchPosts(ctx, userID, domain.ChannelSearchPostsRequest{
		Hashtag:         hashtag,
		Query:           query,
		OffsetRate:      req.OffsetRate,
		OffsetChannelID: offsetChannelID,
		OffsetID:        req.OffsetID,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	history = r.enrichChannelHistory(ctx, userID, history)
	return tgChannelSearchPostsMessages(userID, history), nil
}

func validateChannelSearchPostsRequest(req *tg.ChannelsSearchPostsRequest) error {
	if req == nil {
		return inputRequestInvalidErr()
	}
	if req.Limit < 0 || req.Limit > maxChannelSearchPostsLimit {
		return limitInvalidErr()
	}
	if req.OffsetRate < 0 {
		return limitInvalidErr()
	}
	if req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID {
		return messageIDInvalidErr()
	}
	if req.AllowPaidStars < 0 {
		return limitInvalidErr()
	}
	hashtag, hasHashtag, query, hasQuery := channelSearchPostsTermsWithFlags(req)
	if hasHashtag == hasQuery {
		return searchQueryEmptyErr()
	}
	if hasHashtag {
		if strings.TrimSpace(hashtag) == "" {
			return searchQueryEmptyErr()
		}
		if strings.Contains(hashtag, "#") || utf8.RuneCountInString(hashtag) > maxChannelSearchPostsQuery {
			return limitInvalidErr()
		}
	}
	if hasQuery {
		if strings.TrimSpace(query) == "" {
			return searchQueryEmptyErr()
		}
		if utf8.RuneCountInString(query) > maxChannelSearchPostsQuery {
			return limitInvalidErr()
		}
	}
	return nil
}

func channelSearchPostsTerms(req *tg.ChannelsSearchPostsRequest) (hashtag, query string) {
	hashtag, _, query, _ = channelSearchPostsTermsWithFlags(req)
	return strings.TrimSpace(hashtag), strings.TrimSpace(query)
}

func channelSearchPostsTermsWithFlags(req *tg.ChannelsSearchPostsRequest) (hashtag string, hasHashtag bool, query string, hasQuery bool) {
	hashtag, hasHashtag = req.GetHashtag()
	if !hasHashtag && req.Hashtag != "" {
		hashtag, hasHashtag = req.Hashtag, true
	}
	query, hasQuery = req.GetQuery()
	if !hasQuery && req.Query != "" {
		query, hasQuery = req.Query, true
	}
	return hashtag, hasHashtag, query, hasQuery
}

func (r *Router) validateSearchPostsOffsetPeer(ctx context.Context, userID int64, peer tg.InputPeerClass) error {
	_, err := r.searchPostsOffsetChannelID(ctx, userID, peer)
	return err
}

func (r *Router) searchPostsOffsetChannelID(ctx context.Context, userID int64, peer tg.InputPeerClass) (int64, error) {
	if peer == nil {
		return 0, nil
	}
	if _, ok := peer.(*tg.InputPeerEmpty); ok {
		return 0, nil
	}
	out, ok := r.domainPeerFromInputPeer(userID, peer)
	if !ok || out.ID == 0 {
		return 0, peerIDInvalidErr()
	}
	if out.Type != domain.PeerTypeChannel {
		return 0, peerIDInvalidErr()
	}
	ref, ok := inputPeerChannelRef(peer)
	if !ok || !ref.CheckAccessHash || r.deps.Channels == nil {
		return out.ID, nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, out.ID)
	if err == nil {
		if !inputChannelAccessHashMatches(ref, view.Channel) {
			return 0, channelInvalidErr(domain.ErrChannelPrivate)
		}
		return out.ID, nil
	}
	if !errors.Is(err, domain.ErrChannelPrivate) {
		return 0, channelInvalidErr(err)
	}
	channel, joinErr := r.deps.Channels.GetJoinableChannel(ctx, userID, ref.ID)
	if joinErr != nil || channel.Username == "" || !inputChannelAccessHashMatches(ref, channel) {
		return 0, channelInvalidErr(err)
	}
	return out.ID, nil
}

func tgChannelSearchPostsMessages(viewerUserID int64, history domain.ChannelHistory) tg.MessagesMessagesClass {
	messages := make([]tg.MessageClass, 0, len(history.Messages))
	for _, msg := range history.Messages {
		if item := tgChannelMessage(viewerUserID, msg); item != nil {
			messages = append(messages, item)
		}
	}
	chats := tgChannels(viewerUserID, history.Channels)
	users := tgUsers(history.Users)
	if history.Count > len(messages) {
		out := &tg.MessagesMessagesSlice{
			Count:    history.Count,
			Messages: messages,
			Topics:   []tg.ForumTopicClass{},
			Chats:    chats,
			Users:    users,
		}
		if len(history.Messages) > 0 {
			out.SetNextRate(history.Messages[len(history.Messages)-1].Date)
		}
		out.SetSearchFlood(tg.SearchPostsFlood{QueryIsFree: true, TotalDaily: 100, Remains: 100, StarsAmount: 0})
		return out
	}
	return &tg.MessagesMessages{Messages: messages, Topics: []tg.ForumTopicClass{}, Chats: chats, Users: users}
}

func (r *Router) onChannelsUpdatePaidMessagesPrice(ctx context.Context, req *tg.ChannelsUpdatePaidMessagesPriceRequest) (tg.UpdatesClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	if err := validateChannelPaidMessagesPriceRequest(req, view.Channel); err != nil {
		return nil, err
	}
	stars := req.SendPaidMessagesStars
	if stars < 0 {
		stars = 0
	}
	channel, err := r.deps.Channels.SetPaidMessagesPrice(ctx, userID, view.Channel.ID, stars, req.BroadcastMessagesAllowed)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func validateChannelPaidMessagesPriceRequest(req *tg.ChannelsUpdatePaidMessagesPriceRequest, channel domain.Channel) error {
	stars := req.SendPaidMessagesStars
	if stars == -1 && channel.Broadcast && !req.BroadcastMessagesAllowed {
		return nil
	}
	if stars < 0 || stars > maxChannelPaidMessageStars {
		return starsAmountInvalidErr()
	}
	return nil
}

func (r *Router) onChannelsToggleAutotranslation(ctx context.Context, req *tg.ChannelsToggleAutotranslationRequest) (tg.UpdatesClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, view, err := r.channelChangeInfoView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetAutotranslation(ctx, userID, view.Channel.ID, req.Enabled)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	r.pushChannelStateToMembers(ctx, userID, channel)
	return r.channelStateUpdates(userID, channel), nil
}

func (r *Router) onChannelsGetMessageAuthor(ctx context.Context, req *tg.ChannelsGetMessageAuthorRequest) (tg.UserClass, error) {
	userID, view, err := r.channelView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	author, err := r.deps.Channels.GetMessageAuthor(ctx, userID, domain.GetChannelMessageAuthorRequest{
		UserID:    userID,
		ChannelID: view.Channel.ID,
		ID:        req.ID,
	})
	if err != nil {
		if errors.Is(err, domain.ErrMessageIDInvalid) {
			return nil, messageIDInvalidErr()
		}
		return nil, channelInvalidErr(err)
	}
	users := r.tgUsersForIDs(ctx, userID, []int64{author.SenderUserID})
	if len(users) == 0 {
		return nil, peerIDInvalidErr()
	}
	return users[0], nil
}

func (r *Router) onChannelsCheckSearchPostsFlood(ctx context.Context, req *tg.ChannelsCheckSearchPostsFloodRequest) (*tg.SearchPostsFlood, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if err := validateChannelCheckSearchPostsFloodRequest(req); err != nil {
		return nil, err
	}
	return &tg.SearchPostsFlood{QueryIsFree: true, TotalDaily: 100, Remains: 100, StarsAmount: 0}, nil
}

func validateChannelCheckSearchPostsFloodRequest(req *tg.ChannelsCheckSearchPostsFloodRequest) error {
	if req == nil {
		return inputRequestInvalidErr()
	}
	query, hasQuery := req.GetQuery()
	if !hasQuery && req.Query != "" {
		query, hasQuery = req.Query, true
	}
	if !hasQuery || strings.TrimSpace(query) == "" {
		return searchQueryEmptyErr()
	}
	if utf8.RuneCountInString(query) > maxChannelSearchPostsQuery {
		return limitInvalidErr()
	}
	return nil
}

func (r *Router) onChannelsSetMainProfileTab(ctx context.Context, req *tg.ChannelsSetMainProfileTabRequest) (bool, error) {
	if _, _, err := r.channelChangeInfoView(ctx, req.Channel); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onChannelsGetParticipants(ctx context.Context, req *tg.ChannelsGetParticipantsRequest) (tg.ChannelsChannelParticipantsClass, error) {
	if r.deps.Channels == nil {
		return &tg.ChannelsChannelParticipants{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	filter := domainChannelParticipantsFilter(req.Filter)
	if utf8.RuneCountInString(filter.Query) > domain.MaxChannelParticipantsQueryLength {
		return nil, limitInvalidErr()
	}
	list, err := r.deps.Channels.GetParticipants(ctx, userID, channelID, filter, req.Offset, req.Limit)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	if req.Hash != 0 && list.Hash == req.Hash {
		return &tg.ChannelsChannelParticipantsNotModified{}, nil
	}
	participants := make([]tg.ChannelParticipantClass, 0, len(list.Participants))
	userIDs := make([]int64, 0, len(list.Participants))
	for _, member := range list.Participants {
		participants = append(participants, tgChannelParticipant(userID, member))
		userIDs = append(userIDs, member.UserID)
	}
	users := r.tgUsers(list.Users)
	if len(users) == 0 {
		users = r.tgUsersForIDs(ctx, userID, userIDs)
	}
	r.log.Debug("channels.getParticipants result",
		zap.Int64("channel_id", channelID),
		zap.String("filter", string(filter.Kind)),
		zap.Int("count", list.Count),
		zap.Int("participants", len(participants)),
		zap.Int("users", len(users)),
	)
	return &tg.ChannelsChannelParticipants{
		Count:        list.Count,
		Participants: participants,
		Chats:        []tg.ChatClass{},
		Users:        users,
	}, nil
}

func (r *Router) onChannelsGetParticipant(ctx context.Context, req *tg.ChannelsGetParticipantRequest) (*tg.ChannelsChannelParticipant, error) {
	if r.deps.Channels == nil {
		return &tg.ChannelsChannelParticipant{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	peer, ok := r.domainPeerFromInputPeer(userID, req.Participant)
	if !ok || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	member, err := r.deps.Channels.GetParticipant(ctx, userID, channelID, peer.ID)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	return &tg.ChannelsChannelParticipant{
		Participant: tgChannelParticipant(userID, member),
		Users:       r.tgUsersForIDs(ctx, userID, []int64{member.UserID}),
	}, nil
}

func (r *Router) onChannelsInviteToChannel(ctx context.Context, req *tg.ChannelsInviteToChannelRequest) (*tg.MessagesInvitedUsers, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if len(req.Users) == 0 || len(req.Users) > domain.MaxChannelInviteUsers {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	userIDs, err := r.userIDsFromInputUsers(ctx, userID, req.Users)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Channels.InviteToChannel(ctx, userID, channelID, userIDs, int(r.clock.Now().Unix()))
	if err != nil {
		return nil, channelInviteErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	return &tg.MessagesInvitedUsers{Updates: updates, MissingInvitees: []tg.MissingInvitee{}}, nil
}

func (r *Router) onChannelsJoinChannel(ctx context.Context, input tg.InputChannelClass) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	ref, ok := inputChannelRef(input)
	if !ok {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	if ref.CheckAccessHash {
		channel, err := r.deps.Channels.GetJoinableChannel(ctx, userID, ref.ID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if !inputChannelAccessHashMatches(ref, channel) {
			return nil, channelInvalidErr(domain.ErrChannelPrivate)
		}
	}
	res, err := r.deps.Channels.JoinChannel(ctx, userID, ref.ID, int(r.clock.Now().Unix()))
	if err != nil {
		if errors.Is(err, domain.ErrInviteRequestSent) && res.Channel.ID != 0 {
			r.pushPendingJoinRequestsToAdmins(ctx, res.Channel)
		}
		return nil, channelInviteErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onChannelsLeaveChannel(ctx context.Context, input tg.InputChannelClass) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, input)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Channels.LeaveChannel(ctx, userID, channelID, int(r.clock.Now().Unix()))
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	r.removeOnlineChannelMemberships(res.Channel.ID, userID)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onChannelsEditTitle(ctx context.Context, req *tg.ChannelsEditTitleRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if !validChannelTitle(req.Title) {
		return nil, channelInvalidErr(domain.ErrChannelTitleInvalid)
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Channels.EditTitle(ctx, userID, domain.EditChannelTitleRequest{
		UserID:    userID,
		ChannelID: channelID,
		Title:     req.Title,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelTitleUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelTitleUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onChannelsEditAdmin(ctx context.Context, req *tg.ChannelsEditAdminRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	target, found, err := r.userFromInput(ctx, userID, req.UserID)
	if err != nil {
		return nil, internalErr()
	}
	if !found || target.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	res, err := r.deps.Channels.EditAdmin(ctx, userID, domain.EditChannelAdminRequest{
		UserID:      userID,
		ChannelID:   channelID,
		MemberID:    target.ID,
		AdminRights: domainChannelAdminRights(req.AdminRights),
		Rank:        req.Rank,
		Date:        int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	if res.Participant.Status == domain.ChannelMemberActive {
		r.addOnlineChannelMemberships(res.Channel.ID, res.Participant.UserID)
	} else {
		r.removeOnlineChannelMemberships(res.Channel.ID, res.Participant.UserID)
	}
	updates := r.channelParticipantUpdates(ctx, userID, userID, res.Channel, res.Previous, res.Participant, res.Date)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelParticipantUpdates(ctx, viewerUserID, userID, res.Channel, res.Previous, res.Participant, res.Date)
	})
	return updates, nil
}

func (r *Router) onChannelsEditBanned(ctx context.Context, req *tg.ChannelsEditBannedRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	participant, ok := r.domainPeerFromInputPeer(userID, req.Participant)
	if !ok || participant.Type != domain.PeerTypeUser || participant.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	res, err := r.deps.Channels.EditBanned(ctx, userID, domain.EditChannelBannedRequest{
		UserID:       userID,
		ChannelID:    channelID,
		Participant:  participant,
		BannedRights: domainChannelBannedRights(req.BannedRights),
		Date:         int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelParticipantUpdates(ctx, userID, userID, res.Channel, res.Previous, res.Participant, res.Date)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelParticipantUpdates(ctx, viewerUserID, userID, res.Channel, res.Previous, res.Participant, res.Date)
	})
	return updates, nil
}

func (r *Router) onChannelsEditPhoto(ctx context.Context, req *tg.ChannelsEditPhotoRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	if req.Photo == nil {
		return nil, photoInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	photo, err := r.resolveInputChatPhoto(ctx, userID, req.Photo)
	if err != nil {
		return nil, err
	}
	channel, err := r.deps.Channels.SetPhoto(ctx, userID, channelID, photo)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, channel)
	r.pushChannelStateToMembers(ctx, userID, channel)
	return updates, nil
}

func (r *Router) onChannelsDeleteChannel(ctx context.Context, input tg.InputChannelClass) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, input)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Channels.DeleteChannel(ctx, userID, domain.DeleteChannelRequest{
		UserID:    userID,
		ChannelID: channelID,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelStateUpdates(userID, res.Channel)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelStateUpdates(viewerUserID, res.Channel)
	})
	r.removeOnlineChannelMembershipsForOnlineMembers(res.Channel.ID)
	return updates, nil
}

func (r *Router) onChannelsGetAdminLog(ctx context.Context, req *tg.ChannelsGetAdminLogRequest) (*tg.ChannelsAdminLogResults, error) {
	if r.deps.Channels == nil {
		return &tg.ChannelsAdminLogResults{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	adminIDs := []int64(nil)
	if admins, ok := req.GetAdmins(); ok && len(admins) > 0 {
		if len(admins) > domain.MaxChannelAdminLogAdmins {
			return nil, limitInvalidErr()
		}
		adminIDs, err = r.userIDsFromInputUsers(ctx, userID, admins)
		if err != nil {
			return nil, err
		}
	}
	res, err := r.deps.Channels.ListAdminLog(ctx, userID, domain.ChannelAdminLogRequest{
		UserID:       userID,
		ChannelID:    channelID,
		Query:        req.Q,
		AdminUserIDs: adminIDs,
		MaxID:        req.MaxID,
		MinID:        req.MinID,
		Limit:        req.Limit,
		Filter:       domainChannelAdminLogFilter(req),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	return &tg.ChannelsAdminLogResults{
		Events: tgChannelAdminLogEvents(userID, res.Events),
		Chats:  []tg.ChatClass{tgChannelChat(userID, res.Channel, nil)},
		Users:  r.channelAdminLogUsers(ctx, userID, res.Events),
	}, nil
}

func (r *Router) onChannelsReadHistory(ctx context.Context, req *tg.ChannelsReadHistoryRequest) (bool, error) {
	if r.deps.Channels == nil {
		return true, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return false, err
	}
	read, err := r.deps.Channels.ReadHistory(ctx, userID, domain.ReadChannelHistoryRequest{
		UserID:    userID,
		ChannelID: channelID,
		MaxID:     req.MaxID,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return false, channelInvalidErr(err)
	}
	if _, err := r.recordChannelReadInbox(ctx, userID, read); err != nil {
		return false, err
	}
	r.pushChannelReadOutboxUpdates(ctx, read.ChannelID, read.OutboxUpdates)
	return true, nil
}

func (r *Router) onChannelsGetMessages(ctx context.Context, req *tg.ChannelsGetMessagesRequest) (tg.MessagesMessagesClass, error) {
	if len(req.ID) > domain.MaxGetMessageIDs {
		return nil, limitInvalidErr()
	}
	if r.deps.Channels == nil || len(req.ID) == 0 {
		return &tg.MessagesMessages{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(req.ID))
	for _, input := range req.ID {
		id, ok := inputMessageBoxID(input)
		if !ok || id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return &tg.MessagesMessages{}, nil
	}
	history, err := r.deps.Channels.GetMessages(ctx, userID, channelID, ids)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	history = r.enrichChannelHistory(ctx, userID, history)
	byID := make(map[int]domain.ChannelMessage, len(history.Messages))
	for _, msg := range history.Messages {
		byID[msg.ID] = msg
	}
	messages := make([]tg.MessageClass, 0, len(ids))
	for _, id := range ids {
		if msg, ok := byID[id]; ok {
			messages = append(messages, tgChannelMessage(userID, msg))
		} else {
			messages = append(messages, &tg.MessageEmpty{ID: id})
		}
	}
	return &tg.MessagesMessages{
		Messages: messages,
		Chats:    tgChannels(userID, []domain.Channel{history.Channel}),
		Users:    r.tgUsers(history.Users),
	}, nil
}

func (r *Router) onChannelsDeleteMessages(ctx context.Context, req *tg.ChannelsDeleteMessagesRequest) (*tg.MessagesAffectedMessages, error) {
	if len(req.ID) == 0 {
		return &tg.MessagesAffectedMessages{PtsCount: 0}, nil
	}
	if len(req.ID) > domain.MaxDeleteMessageIDs {
		return nil, limitInvalidErr()
	}
	for _, id := range req.ID {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
	}
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Channels.DeleteMessages(ctx, userID, domain.DeleteChannelMessagesRequest{
		UserID:    userID,
		ChannelID: channelID,
		IDs:       append([]int(nil), req.ID...),
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelDeleteErr(err)
	}
	if res.Event.Pts != 0 {
		r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelDeleteMessagesUpdates(viewerUserID, res.Channel, res.Event)
		})
		return &tg.MessagesAffectedMessages{Pts: res.Event.Pts, PtsCount: res.Event.PtsCount}, nil
	}
	return &tg.MessagesAffectedMessages{Pts: res.Channel.Pts, PtsCount: 0}, nil
}

func (r *Router) onChannelsDeleteHistory(ctx context.Context, req *tg.ChannelsDeleteHistoryRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return &tg.Updates{Date: int(r.clock.Now().Unix())}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	if req.MaxID < 0 || req.MaxID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	res, err := r.deps.Channels.DeleteHistory(ctx, userID, domain.DeleteChannelHistoryRequest{
		UserID:      userID,
		ChannelID:   channelID,
		MaxID:       req.MaxID,
		ForEveryone: req.GetForEveryone(),
		Date:        int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelDeleteErr(err)
	}
	if res.Event.Pts == 0 {
		event := r.recordChannelAvailableMessages(ctx, userID, res.Channel.ID, res.AvailableMinID)
		updates := r.channelAvailableMessagesUpdates(userID, res.Channel, event.MaxID)
		r.pushUserUpdates(ctx, userID, updates)
		return updates, nil
	}
	updates := r.channelDeleteMessagesUpdates(userID, res.Channel, res.Event)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelDeleteMessagesUpdates(viewerUserID, res.Channel, res.Event)
	})
	return updates, nil
}

func (r *Router) onMessagesUpdatePinnedMessage(ctx context.Context, req *tg.MessagesUpdatePinnedMessageRequest) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	res, err := r.deps.Channels.UpdatePinnedMessage(ctx, userID, domain.UpdateChannelPinnedMessageRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		MessageID: req.ID,
		Pinned:    !req.Unpin,
		Silent:    req.Silent,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelAdminErr(err)
	}
	updates := r.channelPinnedUpdates(userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelPinnedUpdates(viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onMessagesUnpinAllMessages(ctx context.Context, req *tg.MessagesUnpinAllMessagesRequest) (*tg.MessagesAffectedHistory, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && (topMsgID <= 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, messageIDInvalidErr()
	}
	if savedPeer, ok := req.GetSavedPeerID(); ok && savedPeer != nil {
		if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, savedPeer); err != nil {
			return nil, err
		}
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return &tg.MessagesAffectedHistory{Offset: 0}, nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, peer.ID)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && topMsgID > 0 {
		return &tg.MessagesAffectedHistory{Pts: view.Channel.Pts, Offset: 0}, nil
	}
	if _, ok := req.GetSavedPeerID(); ok {
		return &tg.MessagesAffectedHistory{Pts: view.Channel.Pts, Offset: 0}, nil
	}
	if view.Channel.PinnedMessageID == 0 {
		return &tg.MessagesAffectedHistory{Pts: view.Channel.Pts, Offset: 0}, nil
	}
	res, err := r.deps.Channels.UpdatePinnedMessage(ctx, userID, domain.UpdateChannelPinnedMessageRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		MessageID: view.Channel.PinnedMessageID,
		Pinned:    false,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		if errors.Is(err, domain.ErrChannelNotModified) {
			return &tg.MessagesAffectedHistory{Pts: view.Channel.Pts, Offset: 0}, nil
		}
		return nil, channelAdminErr(err)
	}
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelPinnedUpdates(viewerUserID, res)
	})
	return &tg.MessagesAffectedHistory{
		Pts:      res.Event.Pts,
		PtsCount: res.Event.PtsCount,
		Offset:   0,
	}, nil
}

func (r *Router) onMessagesExportChatInvite(ctx context.Context, req *tg.MessagesExportChatInviteRequest) (tg.ExportedChatInviteClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	if req.UsageLimit < 0 || req.ExpireDate < 0 || len(req.Title) > domain.MaxChannelInviteTitleLength {
		return nil, limitInvalidErr()
	}
	res, err := r.deps.Channels.ExportInvite(ctx, userID, domain.ExportChannelInviteRequest{
		UserID:                userID,
		ChannelID:             peer.ID,
		Title:                 req.Title,
		RequestNeeded:         req.RequestNeeded,
		ExpireDate:            req.ExpireDate,
		UsageLimit:            req.UsageLimit,
		LegacyRevokePermanent: req.LegacyRevokePermanent,
		Date:                  int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	return tgExportedChannelInvite(res.Invite), nil
}

func (r *Router) onMessagesCheckChatInvite(ctx context.Context, hash string) (tg.ChatInviteClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.CheckInvite(ctx, userID, hash, int(r.clock.Now().Unix()))
	if err != nil {
		return nil, channelInviteErr(err)
	}
	if res.Already {
		return &tg.ChatInviteAlready{Chat: tgChannelChat(userID, res.Channel, &res.Self)}, nil
	}
	return &tg.ChatInvite{
		Channel:           true,
		Broadcast:         res.Channel.Broadcast,
		Megagroup:         res.Channel.Megagroup,
		Public:            res.Channel.Username != "",
		RequestNeeded:     res.Invite.RequestNeeded,
		Title:             res.Channel.Title,
		About:             res.Channel.About,
		Photo:             &tg.PhotoEmpty{},
		ParticipantsCount: res.Channel.ParticipantsCount,
	}, nil
}

func (r *Router) onMessagesImportChatInvite(ctx context.Context, hash string) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.ImportInvite(ctx, userID, domain.ImportChannelInviteRequest{
		UserID: userID,
		Hash:   hash,
		Date:   int(r.clock.Now().Unix()),
	})
	if err != nil {
		if errors.Is(err, domain.ErrInviteRequestSent) && res.Channel.ID != 0 {
			r.pushPendingJoinRequestsToAdmins(ctx, res.Channel)
		}
		return nil, channelInviteErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onMessagesGetExportedChatInvites(ctx context.Context, req *tg.MessagesGetExportedChatInvitesRequest) (*tg.MessagesExportedChatInvites, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if req.Limit < 0 || req.Limit > maxChatInviteListLimit || len(req.OffsetLink) > maxChatInviteLinkLength {
		return nil, limitInvalidErr()
	}
	adminID := userID
	if !inputUserIsEmpty(req.AdminID) {
		admins, err := r.userIDsFromInputUsers(ctx, userID, []tg.InputUserClass{req.AdminID})
		if err != nil {
			return nil, err
		}
		if len(admins) > 0 {
			adminID = admins[0]
		}
	}
	offsetHash := ""
	if req.OffsetLink != "" {
		offsetHash, err = channelInviteHashFromLink(req.OffsetLink)
		if err != nil {
			return nil, err
		}
	}
	list, err := r.deps.Channels.ListExportedInvites(ctx, userID, domain.ChannelInviteListRequest{
		UserID:      userID,
		ChannelID:   view.Channel.ID,
		AdminUserID: adminID,
		Revoked:     req.Revoked,
		OffsetDate:  req.OffsetDate,
		OffsetHash:  offsetHash,
		Limit:       req.Limit,
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	userIDs := []int64{adminID}
	invites := make([]tg.ExportedChatInviteClass, 0, len(list.Invites))
	for _, invite := range list.Invites {
		invites = append(invites, tgExportedChannelInvite(invite))
		userIDs = append(userIDs, invite.AdminUserID)
	}
	return &tg.MessagesExportedChatInvites{
		Count:   list.Count,
		Invites: invites,
		Users:   r.tgUsersForIDs(ctx, userID, userIDs),
	}, nil
}

func (r *Router) onMessagesGetExportedChatInvite(ctx context.Context, req *tg.MessagesGetExportedChatInviteRequest) (tg.MessagesExportedChatInviteClass, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	hash, err := channelInviteHashFromLink(req.Link)
	if err != nil {
		return nil, err
	}
	invite, err := r.deps.Channels.GetExportedInvite(ctx, userID, domain.GetChannelInviteRequest{
		UserID:    userID,
		ChannelID: view.Channel.ID,
		Hash:      hash,
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	return &tg.MessagesExportedChatInvite{
		Invite: tgExportedChannelInvite(invite),
		Users:  r.tgUsersForIDs(ctx, userID, []int64{invite.AdminUserID}),
	}, nil
}

func (r *Router) onMessagesEditExportedChatInvite(ctx context.Context, req *tg.MessagesEditExportedChatInviteRequest) (tg.MessagesExportedChatInviteClass, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	hash, err := channelInviteHashFromLink(req.Link)
	if err != nil {
		return nil, err
	}
	if req.ExpireDate < 0 || req.UsageLimit < 0 || len(req.Title) > domain.MaxChannelInviteTitleLength {
		return nil, limitInvalidErr()
	}
	expireDate, hasExpireDate := req.GetExpireDate()
	usageLimit, hasUsageLimit := req.GetUsageLimit()
	requestNeeded, hasRequestNeeded := req.GetRequestNeeded()
	title, hasTitle := req.GetTitle()
	edited, err := r.deps.Channels.EditExportedInvite(ctx, userID, domain.EditChannelInviteRequest{
		UserID:           userID,
		ChannelID:        view.Channel.ID,
		Hash:             hash,
		Revoked:          req.Revoked,
		HasExpireDate:    hasExpireDate,
		ExpireDate:       expireDate,
		HasUsageLimit:    hasUsageLimit,
		UsageLimit:       usageLimit,
		HasRequestNeeded: hasRequestNeeded,
		RequestNeeded:    requestNeeded,
		HasTitle:         hasTitle,
		Title:            title,
		Date:             int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	users := r.tgUsersForIDs(ctx, userID, []int64{edited.Invite.AdminUserID})
	if edited.NewInvite != nil {
		return &tg.MessagesExportedChatInviteReplaced{
			Invite:    tgExportedChannelInvite(edited.Invite),
			NewInvite: tgExportedChannelInvite(*edited.NewInvite),
			Users:     users,
		}, nil
	}
	return &tg.MessagesExportedChatInvite{Invite: tgExportedChannelInvite(edited.Invite), Users: users}, nil
}

func (r *Router) onMessagesDeleteRevokedExportedChatInvites(ctx context.Context, req *tg.MessagesDeleteRevokedExportedChatInvitesRequest) (bool, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return false, err
	}
	adminID := userID
	if !inputUserIsEmpty(req.AdminID) {
		admins, err := r.userIDsFromInputUsers(ctx, userID, []tg.InputUserClass{req.AdminID})
		if err != nil {
			return false, err
		}
		if len(admins) > 0 {
			adminID = admins[0]
		}
	}
	if err := r.deps.Channels.DeleteRevokedExportedInvites(ctx, userID, domain.DeleteRevokedChannelInvitesRequest{
		UserID:      userID,
		ChannelID:   view.Channel.ID,
		AdminUserID: adminID,
		Limit:       domain.MaxChannelHideJoinRequests,
	}); err != nil {
		return false, channelInviteErr(err)
	}
	return true, nil
}

func (r *Router) onMessagesDeleteExportedChatInvite(ctx context.Context, req *tg.MessagesDeleteExportedChatInviteRequest) (bool, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return false, err
	}
	hash, err := channelInviteHashFromLink(req.Link)
	if err != nil {
		return false, err
	}
	if err := r.deps.Channels.DeleteExportedInvite(ctx, userID, domain.DeleteChannelInviteRequest{
		UserID:    userID,
		ChannelID: view.Channel.ID,
		Hash:      hash,
	}); err != nil {
		return false, channelInviteErr(err)
	}
	return true, nil
}

func (r *Router) onMessagesGetAdminsWithInvites(ctx context.Context, peer tg.InputPeerClass) (*tg.MessagesChatAdminsWithInvites, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, peer)
	if err != nil {
		return nil, err
	}
	counts, err := r.deps.Channels.ListAdminsWithInvites(ctx, userID, view.Channel.ID)
	if err != nil {
		return nil, channelInviteErr(err)
	}
	admins := make([]tg.ChatAdminWithInvites, 0, len(counts))
	userIDs := make([]int64, 0, len(counts))
	for _, count := range counts {
		admins = append(admins, tg.ChatAdminWithInvites{
			AdminID:             count.AdminUserID,
			InvitesCount:        count.InvitesCount,
			RevokedInvitesCount: count.RevokedInvitesCount,
		})
		userIDs = append(userIDs, count.AdminUserID)
	}
	return &tg.MessagesChatAdminsWithInvites{
		Admins: admins,
		Users:  r.tgUsersForIDs(ctx, userID, userIDs),
	}, nil
}

func (r *Router) onMessagesGetChatInviteImporters(ctx context.Context, req *tg.MessagesGetChatInviteImportersRequest) (*tg.MessagesChatInviteImporters, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	link, hasLink := req.GetLink()
	query, hasQuery := req.GetQ()
	if hasLink && hasQuery && strings.TrimSpace(link) != "" && strings.TrimSpace(query) != "" {
		return nil, tgerr400("SEARCH_WITH_LINK_NOT_SUPPORTED")
	}
	if req.Limit < 0 || req.Limit > maxChatInviteListLimit || len(link) > maxChatInviteLinkLength || len(query) > maxChatInviteSearchLength {
		return nil, limitInvalidErr()
	}
	hash := ""
	if strings.TrimSpace(link) != "" {
		hash, err = channelInviteHashFromLink(link)
		if err != nil {
			return nil, err
		}
	}
	offsetUserID := int64(0)
	if !inputUserIsEmpty(req.OffsetUser) {
		ids, err := r.userIDsFromInputUsers(ctx, userID, []tg.InputUserClass{req.OffsetUser})
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			offsetUserID = ids[0]
		}
	}
	list, err := r.deps.Channels.ListInviteImporters(ctx, userID, domain.ChannelInviteImportersRequest{
		UserID:       userID,
		ChannelID:    view.Channel.ID,
		Hash:         hash,
		Requested:    req.Requested,
		Query:        query,
		OffsetDate:   req.OffsetDate,
		OffsetUserID: offsetUserID,
		Limit:        req.Limit,
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	importers := make([]tg.ChatInviteImporter, 0, len(list.Importers))
	userIDs := make([]int64, 0, len(list.Importers))
	for _, importer := range list.Importers {
		tgImporter := tg.ChatInviteImporter{
			UserID: importer.UserID,
			Date:   importer.Date,
		}
		if importer.Requested {
			tgImporter.SetRequested(true)
		}
		if importer.ApprovedBy != 0 {
			tgImporter.SetApprovedBy(importer.ApprovedBy)
			userIDs = append(userIDs, importer.ApprovedBy)
		}
		importers = append(importers, tgImporter)
		userIDs = append(userIDs, importer.UserID)
	}
	return &tg.MessagesChatInviteImporters{
		Count:     list.Count,
		Importers: importers,
		Users:     r.tgUsersForIDs(ctx, userID, userIDs),
	}, nil
}

func (r *Router) onMessagesHideChatJoinRequest(ctx context.Context, req *tg.MessagesHideChatJoinRequestRequest) (tg.UpdatesClass, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	targets, err := r.userIDsFromInputUsers(ctx, userID, []tg.InputUserClass{req.UserID})
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, userIDInvalidErr()
	}
	res, err := r.deps.Channels.HideChatJoinRequest(ctx, userID, domain.HideChannelJoinRequestRequest{
		UserID:       userID,
		ChannelID:    view.Channel.ID,
		TargetUserID: targets[0],
		Approved:     req.Approved,
		Date:         int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.appendPendingJoinRequestsUpdate(ctx, userID, updates, res.Channel)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	r.pushPendingJoinRequestsToAdmins(ctx, res.Channel)
	return updates, nil
}

func (r *Router) onMessagesHideAllChatJoinRequests(ctx context.Context, req *tg.MessagesHideAllChatJoinRequestsRequest) (tg.UpdatesClass, error) {
	userID, view, err := r.inviteManagementChannelView(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	link, hasLink := req.GetLink()
	if hasLink && len(link) > maxChatInviteLinkLength {
		return nil, limitInvalidErr()
	}
	hash := ""
	if hasLink && strings.TrimSpace(link) != "" {
		hash, err = channelInviteHashFromLink(link)
		if err != nil {
			return nil, err
		}
	}
	res, err := r.deps.Channels.HideAllChatJoinRequests(ctx, userID, domain.HideChannelJoinRequestsRequest{
		UserID:    userID,
		ChannelID: view.Channel.ID,
		Hash:      hash,
		Approved:  req.Approved,
		Limit:     domain.MaxChannelHideJoinRequests,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInviteErr(err)
	}
	r.addOnlineChannelMemberships(res.Channel.ID, channelMemberUserIDs(res.Members)...)
	updates := r.channelOperationUpdates(ctx, userID, res)
	r.appendPendingJoinRequestsUpdate(ctx, userID, updates, res.Channel)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelOperationUpdates(ctx, viewerUserID, res)
	})
	r.pushPendingJoinRequestsToAdmins(ctx, res.Channel)
	return updates, nil
}

func (r *Router) onUpdatesGetChannelDifference(ctx context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
	if r.deps.Channels == nil {
		return &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: req.Pts, Timeout: 30}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, req.Channel)
	if err != nil {
		return nil, err
	}
	r.trackChannelInterest(ctx, userID, channelID)
	diff, err := r.deps.Channels.GetDifference(ctx, userID, domain.ChannelDifferenceRequest{
		UserID:    userID,
		ChannelID: channelID,
		Pts:       req.Pts,
		Limit:     req.Limit,
		Force:     req.Force,
	})
	if err != nil {
		if errors.Is(err, domain.ErrPersistentTimestamp) {
			return nil, persistentTimestampInvalidErr()
		}
		return nil, channelInvalidErr(err)
	}
	diff = r.enrichChannelDifference(ctx, userID, diff)
	return tgChannelDifference(userID, diff), nil
}

func (r *Router) channelOperationUpdates(ctx context.Context, viewerUserID int64, res domain.CreateChannelResult) *tg.Updates {
	users := make([]int64, 0, len(res.Members)+1)
	users = append(users, res.Channel.CreatorUserID)
	for _, member := range res.Members {
		users = append(users, member.UserID)
	}
	updates := make([]tg.UpdateClass, 0, 2)
	if res.Event.Pts != 0 {
		if update := tgChannelUpdate(viewerUserID, res.Event); update != nil {
			updates = append(updates, update)
		}
	}
	if res.Channel.ID != 0 {
		updates = append(updates, &tg.UpdateChannel{ChannelID: res.Channel.ID})
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, users),
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, res.Channel, channelMemberForUser(res.Members, viewerUserID))},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) tdesktopCreateChatUpdates(ctx context.Context, viewerUserID int64, res domain.CreateChannelResult) *tg.Updates {
	updates := r.channelOperationUpdates(ctx, viewerUserID, res)
	if updates == nil {
		return updates
	}
	self := channelMemberForUser(res.Members, viewerUserID)
	legacy := tgMigratedLegacyChat(viewerUserID, res.Channel, self)
	if legacy == nil {
		return updates
	}
	chats := make([]tg.ChatClass, 0, len(updates.Chats)+1)
	chats = append(chats, legacy)
	chats = append(chats, updates.Chats...)
	updates.Chats = chats
	return updates
}

func tdesktopCreateChatNeedsLegacyChat(ctx context.Context) bool {
	ci, ok := ClientInfoFrom(ctx)
	if !ok {
		_, hasSession := SessionIDFrom(ctx)
		return hasSession
	}
	device := strings.ToLower(ci.DeviceModel)
	return ci.LangPack == "tdesktop" || strings.Contains(device, "desktop")
}

func channelMemberForUser(members []domain.ChannelMember, userID int64) *domain.ChannelMember {
	for i := range members {
		if members[i].UserID == userID {
			return &members[i]
		}
	}
	return nil
}

func createChatInviteMemberIDs(ids []int64, selfUserID int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 || id == selfUserID {
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

func mergeChannelMembers(a, b []domain.ChannelMember) []domain.ChannelMember {
	if len(a) == 0 {
		return append([]domain.ChannelMember(nil), b...)
	}
	if len(b) == 0 {
		return append([]domain.ChannelMember(nil), a...)
	}
	out := make([]domain.ChannelMember, 0, len(a)+len(b))
	seen := make(map[int64]struct{}, len(a)+len(b))
	appendOne := func(member domain.ChannelMember) {
		if member.UserID == 0 {
			return
		}
		if _, ok := seen[member.UserID]; ok {
			return
		}
		seen[member.UserID] = struct{}{}
		out = append(out, member)
	}
	for _, member := range a {
		appendOne(member)
	}
	for _, member := range b {
		appendOne(member)
	}
	return out
}

func (r *Router) channelSelfForViewer(ctx context.Context, userID, channelID int64, members []domain.ChannelMember) *domain.ChannelMember {
	if self := channelMemberForUser(members, userID); self != nil {
		return self
	}
	if r.deps.Channels == nil || userID == 0 || channelID == 0 {
		return nil
	}
	self, err := r.deps.Channels.GetParticipant(ctx, userID, channelID, userID)
	if err != nil {
		return nil
	}
	return &self
}

func canViewChannelJoinRequests(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo))
}

func (r *Router) applyPendingJoinRequestsToFullChannel(ctx context.Context, full *tg.ChannelFull, channelID int64, userIDs []int64) []int64 {
	if r.deps.Channels == nil || full == nil || channelID == 0 {
		return userIDs
	}
	pending, err := r.deps.Channels.PendingJoinRequests(ctx, channelID, domain.MaxChannelPendingJoinRecentRequesters)
	if err != nil || pending.Count <= 0 {
		return userIDs
	}
	full.SetRequestsPending(pending.Count)
	full.SetRecentRequesters(pending.RecentRequesters)
	return append(userIDs, pending.RecentRequesters...)
}

func (r *Router) pendingJoinRequestsUpdates(ctx context.Context, viewerUserID int64, channel domain.Channel) *tg.Updates {
	if r.deps.Channels == nil || channel.ID == 0 {
		return nil
	}
	pending, err := r.deps.Channels.PendingJoinRequests(ctx, channel.ID, domain.MaxChannelPendingJoinRecentRequesters)
	if err != nil {
		return nil
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePendingJoinRequests{
			Peer:             &tg.PeerChannel{ChannelID: channel.ID},
			RequestsPending:  pending.Count,
			RecentRequesters: pending.RecentRequesters,
		}},
		Users: r.tgUsersForIDs(ctx, viewerUserID, pending.RecentRequesters),
		Chats: []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:  int(r.clock.Now().Unix()),
		Seq:   0,
	}
}

func (r *Router) appendPendingJoinRequestsUpdate(ctx context.Context, viewerUserID int64, updates *tg.Updates, channel domain.Channel) {
	if updates == nil {
		return
	}
	pending := r.pendingJoinRequestsUpdates(ctx, viewerUserID, channel)
	if pending == nil {
		return
	}
	updates.Updates = append(updates.Updates, pending.Updates...)
	updates.Users = append(updates.Users, pending.Users...)
}

func (r *Router) pushPendingJoinRequestsToAdmins(ctx context.Context, channel domain.Channel) {
	if r.deps.Channels == nil || r.deps.Sessions == nil || channel.ID == 0 {
		return
	}
	adminIDs, err := r.deps.Channels.InviteAdminMemberIDs(ctx, channel.ID, domain.MaxChannelRealtimeFanout)
	if err != nil || len(adminIDs) == 0 {
		adminIDs = []int64{channel.CreatorUserID}
	}
	seen := make(map[int64]struct{}, len(adminIDs))
	for _, adminID := range adminIDs {
		if adminID == 0 {
			continue
		}
		if _, ok := seen[adminID]; ok {
			continue
		}
		seen[adminID] = struct{}{}
		updates := r.pendingJoinRequestsUpdates(ctx, adminID, channel)
		if updates == nil {
			continue
		}
		r.pushUserUpdates(ctx, adminID, updates)
	}
}

func (r *Router) channelTitleUpdates(ctx context.Context, viewerUserID int64, res domain.EditChannelTitleResult) *tg.Updates {
	updates := []tg.UpdateClass{&tg.UpdateChannel{ChannelID: res.Channel.ID}}
	if res.Event.Pts != 0 {
		if update := tgChannelUpdate(viewerUserID, res.Event); update != nil {
			updates = append(updates, update)
		}
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, []int64{res.Message.SenderUserID}),
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, res.Channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelParticipantUpdates(ctx context.Context, viewerUserID, actorUserID int64, channel domain.Channel, previous, participant domain.ChannelMember, date int) *tg.Updates {
	update := &tg.UpdateChannelParticipant{
		ChannelID: channel.ID,
		Date:      date,
		ActorID:   actorUserID,
		UserID:    participant.UserID,
	}
	if update.ActorID == 0 {
		update.ActorID = viewerUserID
	}
	if previous.UserID != 0 {
		update.SetPrevParticipant(tgChannelParticipantForUpdate(viewerUserID, previous))
	}
	if participant.UserID != 0 {
		update.SetNewParticipant(tgChannelParticipantForUpdate(viewerUserID, participant))
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{update, &tg.UpdateChannel{ChannelID: channel.ID}},
		Users:   r.tgUsersForIDs(ctx, viewerUserID, []int64{participant.UserID, participant.InviterUserID, previous.UserID, previous.InviterUserID, update.ActorID}),
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelStateUpdates(viewerUserID int64, channel domain.Channel) *tg.Updates {
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: channel.ID}},
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelPinnedUpdates(viewerUserID int64, res domain.UpdateChannelPinnedMessageResult) *tg.Updates {
	updates := []tg.UpdateClass(nil)
	if update := tgChannelUpdate(viewerUserID, res.Event); update != nil {
		updates = append(updates, update)
	}
	updates = append(updates, &tg.UpdateChannel{ChannelID: res.Channel.ID})
	return &tg.Updates{
		Updates: updates,
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, res.Channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelMessageUpdates(ctx context.Context, viewerUserID int64, res domain.SendChannelMessageResult, randomID int64) *tg.Updates {
	randomIDs := []int64(nil)
	includeMessageIDs := randomID != 0
	if includeMessageIDs {
		randomIDs = []int64{randomID}
	}
	return r.channelMessagesUpdates(ctx, viewerUserID, []domain.SendChannelMessageResult{res}, randomIDs, includeMessageIDs, nil)
}

func (r *Router) pushChannelDiscussionUpdate(ctx context.Context, originUserID int64, discussion *domain.SendChannelDiscussionResult) {
	if discussion == nil || discussion.Channel.ID == 0 || discussion.Event.Pts == 0 {
		return
	}
	res := domain.SendChannelMessageResult{
		Channel:    discussion.Channel,
		Message:    discussion.Message,
		Event:      discussion.Event,
		Recipients: discussion.Recipients,
	}
	r.pushChannelUpdates(ctx, originUserID, discussion.Channel.ID, discussion.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelMessageUpdates(ctx, viewerUserID, res, 0)
	})
}

func (r *Router) channelMessagesUpdates(ctx context.Context, viewerUserID int64, results []domain.SendChannelMessageResult, randomIDs []int64, includeMessageIDs bool, extraUserIDs []int64) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, len(results)*2)
	userIDs := make([]int64, 0, len(results)+len(extraUserIDs))
	userIDs = append(userIDs, extraUserIDs...)
	extraChannelIDs := make([]int64, 0, len(results))
	var channel domain.Channel
	date := 0
	for i, res := range results {
		if res.Channel.ID != 0 {
			channel = res.Channel
		}
		if includeMessageIDs && res.Message.ID != 0 && i < len(randomIDs) && randomIDs[i] != 0 {
			updates = append(updates, &tg.UpdateMessageID{ID: res.Message.ID, RandomID: randomIDs[i]})
		}
		if res.Event.Pts != 0 {
			if update := tgChannelUpdate(viewerUserID, res.Event); update != nil {
				updates = append(updates, update)
			}
		}
		if res.Message.SenderUserID != 0 {
			userIDs = append(userIDs, res.Message.SenderUserID)
		}
		if res.Message.SendAs != nil {
			switch res.Message.SendAs.Type {
			case domain.PeerTypeUser:
				userIDs = append(userIDs, res.Message.SendAs.ID)
			case domain.PeerTypeChannel:
				extraChannelIDs = append(extraChannelIDs, res.Message.SendAs.ID)
			}
		}
		if res.Message.Forward != nil && res.Message.Forward.From.Type == domain.PeerTypeChannel {
			extraChannelIDs = append(extraChannelIDs, res.Message.Forward.From.ID)
		}
		if res.Message.ReplyTo != nil && res.Message.ReplyTo.Peer.Type == domain.PeerTypeChannel {
			extraChannelIDs = append(extraChannelIDs, res.Message.ReplyTo.Peer.ID)
		}
		if date == 0 {
			date = res.Event.Date
		}
		if date == 0 {
			date = res.Message.Date
		}
	}
	chats := []tg.ChatClass(nil)
	if channel.ID != 0 {
		chats = []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)}
	}
	chats = append(chats, r.tgChannelsForIDs(ctx, viewerUserID, extraChannelIDs, channel.ID)...)
	if date == 0 {
		date = int(r.clock.Now().Unix())
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, userIDs),
		Chats:   chats,
		Date:    date,
		Seq:     0,
	}
}

func (r *Router) tgChannelsForIDs(ctx context.Context, viewerUserID int64, ids []int64, skipIDs ...int64) []tg.ChatClass {
	if r.deps.Channels == nil || len(ids) == 0 {
		return nil
	}
	skip := make(map[int64]struct{}, len(skipIDs))
	for _, id := range skipIDs {
		if id != 0 {
			skip[id] = struct{}{}
		}
	}
	seen := make(map[int64]struct{}, len(ids))
	chats := make([]tg.ChatClass, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := skip[id]; ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		view, err := r.deps.Channels.GetChannel(ctx, viewerUserID, id)
		if err != nil || view.Channel.ID == 0 {
			continue
		}
		chats = append(chats, tgChannelChat(viewerUserID, view.Channel, &view.Self))
	}
	return chats
}

func (r *Router) channelEditMessageUpdates(ctx context.Context, viewerUserID int64, res domain.EditChannelMessageResult) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, 1)
	if res.Event.Pts != 0 {
		if update := tgChannelUpdate(viewerUserID, res.Event); update != nil {
			updates = append(updates, update)
		}
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, []int64{res.Message.SenderUserID}),
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, res.Channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelDeleteMessagesUpdates(viewerUserID int64, channel domain.Channel, event domain.ChannelUpdateEvent) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, 1)
	if update := tgChannelUpdate(viewerUserID, event); update != nil {
		updates = append(updates, update)
	}
	return &tg.Updates{
		Updates: updates,
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelAvailableMessagesUpdates(viewerUserID int64, channel domain.Channel, availableMinID int) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, 1)
	if channel.ID != 0 && availableMinID > 0 {
		updates = append(updates, &tg.UpdateChannelAvailableMessages{
			ChannelID:      channel.ID,
			AvailableMinID: availableMinID,
		})
	}
	return &tg.Updates{
		Updates: updates,
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

type channelUpdatesBuilder func(viewerUserID int64) *tg.Updates

type channelFanoutScope int

const (
	channelFanoutMembers channelFanoutScope = iota
	channelFanoutViewers
	channelFanoutExplicit
)

func (r *Router) pushChannelReadOutboxUpdates(ctx context.Context, channelID int64, updates []domain.ChannelReadOutboxUpdate) {
	if r.deps.Sessions == nil || channelID == 0 || len(updates) == 0 {
		return
	}
	seen := make(map[int64]int, len(updates))
	for _, update := range updates {
		if update.UserID == 0 || update.MaxID <= 0 {
			continue
		}
		if seen[update.UserID] < update.MaxID {
			seen[update.UserID] = update.MaxID
		}
	}
	date := int(r.clock.Now().Unix())
	for userID, maxID := range seen {
		r.pushUserUpdates(ctx, userID, &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateReadChannelOutbox{ChannelID: channelID, MaxID: maxID}},
			Date:    date,
			Seq:     0,
		})
	}
}

func (r *Router) recordChannelAvailableMessages(ctx context.Context, userID, channelID int64, availableMinID int) domain.UpdateEvent {
	event := domain.UpdateEvent{
		UserID:   userID,
		Type:     domain.UpdateEventChannelAvailable,
		Date:     int(r.clock.Now().Unix()),
		Peer:     domain.Peer{Type: domain.PeerTypeChannel, ID: channelID},
		MaxID:    availableMinID,
		PtsCount: 1,
	}
	if r.deps.Updates == nil || userID == 0 || channelID == 0 || availableMinID <= 0 {
		return event
	}
	authKeyID, _ := AuthKeyIDFrom(ctx)
	sessionID, _ := SessionIDFrom(ctx)
	recorded, _, err := r.deps.Updates.RecordChannelAvailableMessages(ctx, authKeyID, userID, channelID, availableMinID, sessionID)
	if err != nil {
		return event
	}
	return recorded
}

func (r *Router) recordChannelReadInbox(ctx context.Context, userID int64, read domain.ReadChannelHistoryResult) (domain.UpdateEvent, error) {
	if !read.Changed || read.ChannelID == 0 {
		return domain.UpdateEvent{}, nil
	}
	date := int(r.clock.Now().Unix())
	event := domain.UpdateEvent{
		UserID:           userID,
		Type:             domain.UpdateEventReadHistoryInbox,
		Date:             date,
		Peer:             domain.Peer{Type: domain.PeerTypeChannel, ID: read.ChannelID},
		MaxID:            read.MaxID,
		StillUnreadCount: read.StillUnreadCount,
		Pts:              read.Pts,
		PtsCount:         1,
	}
	recordedEvent := event
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		recorded, _, err := r.deps.Updates.RecordReadHistory(ctx, authKeyID, userID, domain.ReadHistoryResult{
			OwnerUserID:      userID,
			Peer:             event.Peer,
			MaxID:            read.MaxID,
			StillUnreadCount: read.StillUnreadCount,
			Changed:          read.Changed,
		}, sessionID)
		if err != nil {
			return domain.UpdateEvent{}, internalErr()
		}
		recordedEvent = recorded
	}
	r.pushReadHistoryEvent(ctx, userID, event)
	return recordedEvent, nil
}

func (r *Router) pushChannelUpdates(ctx context.Context, originUserID, channelID int64, recipients []int64, build channelUpdatesBuilder) {
	r.pushChannelUpdatesWithScope(ctx, channelFanoutMembers, originUserID, channelID, recipients, build)
}

func (r *Router) pushChannelViewerUpdates(ctx context.Context, originUserID, channelID int64, recipients []int64, build channelUpdatesBuilder) {
	r.pushChannelUpdatesWithScope(ctx, channelFanoutViewers, originUserID, channelID, recipients, build)
}

func (r *Router) pushChannelExplicitUpdates(ctx context.Context, originUserID, channelID int64, recipients []int64, build channelUpdatesBuilder) {
	r.pushChannelUpdatesWithScope(ctx, channelFanoutExplicit, originUserID, channelID, recipients, build)
}

func (r *Router) pushChannelUpdatesWithScope(ctx context.Context, scope channelFanoutScope, originUserID, channelID int64, recipients []int64, build channelUpdatesBuilder) {
	if r.deps.Sessions == nil || build == nil {
		return
	}
	recipients = r.channelFanoutRecipients(ctx, scope, channelID, recipients)
	seen := make(map[int64]struct{}, len(recipients))
	pushed := false
	for _, userID := range recipients {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		updates := build(userID)
		if updates == nil {
			continue
		}
		r.pushUserUpdates(ctx, userID, updates)
		pushed = true
	}
	if !pushed && originUserID != 0 {
		updates := build(originUserID)
		if updates == nil {
			return
		}
		r.pushUserUpdates(ctx, originUserID, updates)
	}
}

func (r *Router) channelFanoutRecipients(ctx context.Context, scope channelFanoutScope, channelID int64, explicit []int64) []int64 {
	if channelID == 0 || r.deps.Channels == nil || r.deps.Sessions == nil {
		return uniqueRecipientIDs(explicit)
	}
	if scope == channelFanoutExplicit {
		return uniqueRecipientIDs(explicit)
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return uniqueRecipientIDs(explicit)
	}
	var online []int64
	switch scope {
	case channelFanoutMembers:
		online = provider.OnlineChannelMemberUserIDs(channelID, 0)
	case channelFanoutViewers:
		online = provider.OnlineChannelUserIDs(channelID, 0)
	}
	if len(online) == 0 {
		return uniqueRecipientIDs(explicit)
	}
	active, err := r.deps.Channels.FilterActiveMemberIDs(ctx, channelID, online)
	if err != nil {
		return uniqueRecipientIDs(explicit)
	}
	if len(active) == 0 && len(explicit) == 0 {
		return nil
	}
	out := uniqueRecipientIDs(active)
	seen := make(map[int64]struct{}, len(out)+len(explicit))
	for _, userID := range active {
		if userID == 0 {
			continue
		}
		seen[userID] = struct{}{}
	}
	// Keep operation-specific recipients as a fallback: leave/kick/delete flows
	// may need to notify a user who is no longer an active member after commit.
	for _, userID := range explicit {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		out = append(out, userID)
	}
	return out
}

func uniqueRecipientIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, userID := range ids {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		out = append(out, userID)
	}
	return out
}

func (r *Router) pushChannelStateToMembers(ctx context.Context, originUserID int64, channel domain.Channel) {
	if r.deps.Channels == nil || channel.ID == 0 {
		return
	}
	r.pushChannelUpdates(ctx, originUserID, channel.ID, []int64{originUserID}, func(viewerUserID int64) *tg.Updates {
		return r.channelStateUpdates(viewerUserID, channel)
	})
}

func (r *Router) tgUsersForIDs(ctx context.Context, currentUserID int64, ids []int64) []tg.UserClass {
	if r.deps.Users == nil || len(ids) == 0 {
		return nil
	}
	unique := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	users, err := r.deps.Users.ByIDs(ctx, currentUserID, unique)
	if err != nil {
		// 批量解析失败（通常是 DB 故障）不静默丢弃：批量语义下无部分结果，记日志便于排查。
		// update 仍以空 users 列表推送，客户端用本地缓存或后续 getUser 补齐，不致命。
		// 不降级逐个查询以免 DB 抖动时把一次失败放大成 N 次查询。
		r.log.Warn("batch resolve users for channel update failed",
			zap.Int("count", len(unique)), zap.Error(err))
		return nil
	}
	byID := make(map[int64]domain.User, len(users))
	for _, u := range users {
		if u.ID != 0 {
			byID[u.ID] = u
		}
	}
	out := make([]tg.UserClass, 0, len(byID))
	for _, id := range unique {
		u, ok := byID[id]
		if !ok {
			continue
		}
		if id == currentUserID {
			out = append(out, r.tgSelfUser(u))
			continue
		}
		out = append(out, r.tgUser(u))
	}
	return out
}

func tgExportedChannelInvite(invite domain.ChannelInvite) tg.ExportedChatInviteClass {
	out := &tg.ChatInviteExported{
		Revoked:       invite.Revoked,
		Permanent:     invite.Permanent,
		RequestNeeded: invite.RequestNeeded,
		Link:          "https://t.me/+" + invite.Hash,
		AdminID:       invite.AdminUserID,
		Date:          invite.Date,
	}
	if invite.Title != "" {
		out.SetTitle(invite.Title)
	}
	if invite.ExpireDate > 0 {
		out.SetExpireDate(invite.ExpireDate)
	}
	if invite.UsageLimit > 0 {
		out.SetUsageLimit(invite.UsageLimit)
	}
	if invite.UsageCount > 0 {
		out.SetUsage(invite.UsageCount)
	}
	if invite.RequestedCount > 0 {
		out.SetRequested(invite.RequestedCount)
	}
	return out
}

func chatInviteExportedStub(adminUserID int64, link, title string, revoked, requestNeeded bool, expireDate, usageLimit, usageCount, date int) tg.ExportedChatInviteClass {
	out := &tg.ChatInviteExported{
		Revoked:       revoked,
		RequestNeeded: requestNeeded,
		Link:          strings.TrimSpace(link),
		AdminID:       adminUserID,
		Date:          date,
	}
	if title != "" {
		out.SetTitle(title)
	}
	if expireDate > 0 {
		out.SetExpireDate(expireDate)
	}
	if usageLimit > 0 {
		out.SetUsageLimit(usageLimit)
	}
	if usageCount > 0 {
		out.SetUsage(usageCount)
	}
	return out
}

func (r *Router) inviteManagementChannelView(ctx context.Context, peer tg.InputPeerClass) (int64, domain.ChannelView, error) {
	ref, ok := inviteManagementChannelRef(peer)
	if !ok {
		return 0, domain.ChannelView{}, peerIDInvalidErr()
	}
	input := &tg.InputChannel{ChannelID: ref.ID}
	if ref.CheckAccessHash {
		input.AccessHash = ref.AccessHash
	}
	userID, view, err := r.channelView(ctx, input)
	if err != nil {
		return 0, domain.ChannelView{}, err
	}
	if view.Self.Role != domain.ChannelRoleCreator && view.Self.Role != domain.ChannelRoleAdmin {
		return 0, domain.ChannelView{}, tgerr400("CHAT_ADMIN_REQUIRED")
	}
	return userID, view, nil
}

func inviteManagementChannelRef(peer tg.InputPeerClass) (channelInputRef, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return channelInputRef{
			ID:              p.ChannelID,
			AccessHash:      p.AccessHash,
			CheckAccessHash: p.AccessHash != 0,
		}, p.ChannelID > 0
	case *tg.InputPeerChannelFromMessage:
		return channelInputRef{ID: p.ChannelID}, p.ChannelID > 0
	case *tg.InputPeerChat:
		return channelInputRef{ID: p.ChatID}, p.ChatID > 0
	default:
		return channelInputRef{}, false
	}
}

func validateChatInviteLink(link string) error {
	link = strings.TrimSpace(link)
	if link == "" {
		return tgerr400("INVITE_HASH_EMPTY")
	}
	if len(link) > maxChatInviteLinkLength {
		return limitInvalidErr()
	}
	return nil
}

func channelInviteHashFromLink(link string) (string, error) {
	if err := validateChatInviteLink(link); err != nil {
		return "", err
	}
	link = strings.TrimSpace(link)
	link = strings.TrimPrefix(link, "tg://join?invite=")
	if strings.Contains(link, "://") {
		if idx := strings.LastIndex(link, "/+"); idx >= 0 {
			link = link[idx+2:]
		} else if idx := strings.LastIndex(link, "/joinchat/"); idx >= 0 {
			link = link[idx+10:]
		} else if idx := strings.LastIndex(link, "/"); idx >= 0 {
			link = link[idx+1:]
		}
	}
	link = strings.TrimPrefix(link, "+")
	link = strings.TrimSpace(link)
	if link == "" {
		return "", tgerr400("INVITE_HASH_EMPTY")
	}
	if len(link) > maxChatInviteLinkLength {
		return "", limitInvalidErr()
	}
	return link, nil
}

func inputUserIsEmpty(input tg.InputUserClass) bool {
	switch input.(type) {
	case nil, *tg.InputUserEmpty:
		return true
	default:
		return false
	}
}

func (r *Router) userIDsFromInputUsers(ctx context.Context, currentUserID int64, inputs []tg.InputUserClass) ([]int64, error) {
	out := make([]int64, 0, len(inputs))
	seen := make(map[int64]struct{}, len(inputs))
	for _, input := range inputs {
		u, found, err := r.userFromInput(ctx, currentUserID, input)
		if err != nil {
			return nil, internalErr()
		}
		if !found || u.ID == 0 {
			return nil, peerIDInvalidErr()
		}
		if _, ok := seen[u.ID]; ok {
			continue
		}
		seen[u.ID] = struct{}{}
		out = append(out, u.ID)
	}
	return out, nil
}

func domainChannelAdminLogFilter(req *tg.ChannelsGetAdminLogRequest) domain.ChannelAdminLogFilter {
	filter, ok := req.GetEventsFilter()
	if !ok {
		return domain.ChannelAdminLogFilter{}
	}
	return domain.ChannelAdminLogFilter{
		Join:      filter.GetJoin(),
		Leave:     filter.GetLeave(),
		Invite:    filter.GetInvite(),
		Ban:       filter.GetBan(),
		Unban:     filter.GetUnban(),
		Kick:      filter.GetKick(),
		Unkick:    filter.GetUnkick(),
		Promote:   filter.GetPromote(),
		Demote:    filter.GetDemote(),
		Info:      filter.GetInfo(),
		Settings:  filter.GetSettings(),
		Pinned:    filter.GetPinned(),
		Edit:      filter.GetEdit(),
		Delete:    filter.GetDelete(),
		Send:      filter.GetSend(),
		Invites:   filter.GetInvites(),
		Forums:    filter.GetForums(),
		SubExtend: filter.GetSubExtend(),
		EditRank:  filter.GetEditRank(),
	}
}

func (r *Router) channelAdminLogUsers(ctx context.Context, currentUserID int64, events []domain.ChannelAdminLogEvent) []tg.UserClass {
	if r.deps.Users == nil || len(events) == 0 {
		return nil
	}
	ids := make(map[int64]struct{}, len(events))
	add := func(id int64) {
		if id != 0 {
			ids[id] = struct{}{}
		}
	}
	addMember := func(member *domain.ChannelMember) {
		if member != nil {
			add(member.UserID)
			add(member.InviterUserID)
		}
	}
	addMessage := func(msg *domain.ChannelMessage) {
		if msg != nil {
			add(msg.SenderUserID)
			if msg.From.Type == domain.PeerTypeUser {
				add(msg.From.ID)
			}
		}
	}
	for _, event := range events {
		add(event.UserID)
		addMember(event.PrevParticipant)
		addMember(event.NewParticipant)
		addMember(event.Participant)
		addMessage(event.Message)
		addMessage(event.PrevMessage)
		addMessage(event.NewMessage)
	}
	out := make([]tg.UserClass, 0, len(ids))
	userIDs := make([]int64, 0, len(ids))
	for id := range ids {
		userIDs = append(userIDs, id)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	for _, id := range userIDs {
		u, found, err := r.deps.Users.ByID(ctx, currentUserID, id)
		if err == nil && found {
			out = append(out, r.tgUser(u))
		}
	}
	return out
}

type channelInputRef struct {
	ID              int64
	AccessHash      int64
	CheckAccessHash bool
}

func inputChannelRef(input tg.InputChannelClass) (channelInputRef, bool) {
	switch channel := input.(type) {
	case *tg.InputChannel:
		return channelInputRef{
			ID:              channel.ChannelID,
			AccessHash:      channel.AccessHash,
			CheckAccessHash: channel.AccessHash != 0,
		}, channel.ChannelID > 0
	case *tg.InputChannelFromMessage:
		return channelInputRef{ID: channel.ChannelID}, channel.ChannelID > 0
	default:
		return channelInputRef{}, false
	}
}

func inputChannelID(input tg.InputChannelClass) (int64, bool) {
	ref, ok := inputChannelRef(input)
	return ref.ID, ok
}

func inputChannelAccessHashMatches(ref channelInputRef, channel domain.Channel) bool {
	return !ref.CheckAccessHash || ref.AccessHash == channel.AccessHash
}

func (r *Router) optionalChannelIDFromInput(ctx context.Context, userID int64, input tg.InputChannelClass) (int64, error) {
	switch input.(type) {
	case nil, *tg.InputChannelEmpty:
		return 0, nil
	default:
		return r.channelIDFromInput(ctx, userID, input)
	}
}

func (r *Router) channelIDFromInput(ctx context.Context, userID int64, input tg.InputChannelClass) (int64, error) {
	ref, ok := inputChannelRef(input)
	if !ok {
		return 0, channelInvalidErr(domain.ErrChannelInvalid)
	}
	if !ref.CheckAccessHash || r.deps.Channels == nil {
		return ref.ID, nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, ref.ID)
	if err != nil {
		return 0, channelInvalidErr(err)
	}
	if !inputChannelAccessHashMatches(ref, view.Channel) {
		return 0, channelInvalidErr(domain.ErrChannelPrivate)
	}
	return ref.ID, nil
}

func validChannelTitle(title string) bool {
	n := utf8.RuneCountInString(title)
	return n > 0 && n <= maxChannelTitleLength
}

func (r *Router) channelView(ctx context.Context, input tg.InputChannelClass) (int64, domain.ChannelView, error) {
	if r.deps.Channels == nil {
		return 0, domain.ChannelView{}, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return 0, domain.ChannelView{}, internalErr()
	}
	ref, ok := inputChannelRef(input)
	if !ok {
		return 0, domain.ChannelView{}, channelInvalidErr(domain.ErrChannelInvalid)
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, ref.ID)
	if err != nil {
		return 0, domain.ChannelView{}, channelInvalidErr(err)
	}
	if !inputChannelAccessHashMatches(ref, view.Channel) {
		return 0, domain.ChannelView{}, channelInvalidErr(domain.ErrChannelPrivate)
	}
	return userID, view, nil
}

func (r *Router) channelChangeInfoView(ctx context.Context, input tg.InputChannelClass) (int64, domain.ChannelView, error) {
	if r.deps.Channels == nil {
		return 0, domain.ChannelView{}, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return 0, domain.ChannelView{}, internalErr()
	}
	ref, ok := inputChannelRef(input)
	if !ok {
		return 0, domain.ChannelView{}, channelInvalidErr(domain.ErrChannelInvalid)
	}
	view, err := r.deps.Channels.GetChannelForChangeInfo(ctx, userID, ref.ID)
	if err != nil {
		return 0, domain.ChannelView{}, channelAdminErr(err)
	}
	if !inputChannelAccessHashMatches(ref, view.Channel) {
		return 0, domain.ChannelView{}, channelInvalidErr(domain.ErrChannelPrivate)
	}
	return userID, view, nil
}

func (r *Router) channelStateCompatUpdate(ctx context.Context, input tg.InputChannelClass) (tg.UpdatesClass, error) {
	userID, view, err := r.channelChangeInfoView(ctx, input)
	if err != nil {
		return nil, err
	}
	updates := r.channelStateUpdates(userID, view.Channel)
	r.pushChannelStateToMembers(ctx, userID, view.Channel)
	return updates, nil
}

func tgEmptyUpdates(date int) *tg.Updates {
	return &tg.Updates{
		Updates: []tg.UpdateClass{},
		Users:   []tg.UserClass{},
		Chats:   []tg.ChatClass{},
		Date:    date,
		Seq:     0,
	}
}

func validChannelSlowModeSeconds(seconds int) bool {
	return domain.ValidChannelSlowModeSeconds(seconds)
}

func validChannelManagementUsername(username string) bool {
	username = strings.TrimSpace(strings.TrimPrefix(username, "@"))
	if len(username) < 5 || len(username) > 32 {
		return false
	}
	for i := 0; i < len(username); i++ {
		c := username[i]
		switch {
		case i == 0 && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')):
		case i > 0 && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'):
		default:
			return false
		}
	}
	return true
}

func domainChannelReactionPolicy(req *tg.MessagesSetChatAvailableReactionsRequest) (domain.ChannelReactionPolicy, error) {
	if req == nil || req.AvailableReactions == nil {
		return domain.ChannelReactionPolicy{}, tgerr400("REACTION_INVALID")
	}
	if req.ReactionsLimit < 0 || req.ReactionsLimit > domain.MaxChannelReactionItems {
		return domain.ChannelReactionPolicy{}, limitInvalidErr()
	}
	policy := domain.ChannelReactionPolicy{
		Limit:       req.ReactionsLimit,
		PaidEnabled: req.PaidEnabled,
	}
	switch reactions := req.AvailableReactions.(type) {
	case *tg.ChatReactionsNone:
		policy.Type = domain.ChannelReactionPolicyNone
	case *tg.ChatReactionsAll:
		policy.Type = domain.ChannelReactionPolicyAll
		policy.AllowCustom = reactions.AllowCustom
	case *tg.ChatReactionsSome:
		if len(reactions.Reactions) > domain.MaxChannelReactionItems {
			return domain.ChannelReactionPolicy{}, limitInvalidErr()
		}
		policy.Type = domain.ChannelReactionPolicySome
		for _, reaction := range reactions.Reactions {
			switch value := reaction.(type) {
			case *tg.ReactionEmoji:
				if strings.TrimSpace(value.Emoticon) == "" || utf8.RuneCountInString(value.Emoticon) > domain.MaxChannelReactionEmoticonLength {
					return domain.ChannelReactionPolicy{}, tgerr400("REACTION_INVALID")
				}
				policy.Emoticons = append(policy.Emoticons, value.Emoticon)
			case *tg.ReactionCustomEmoji:
				if value.DocumentID <= 0 {
					return domain.ChannelReactionPolicy{}, tgerr400("REACTION_INVALID")
				}
				policy.CustomEmojiIDs = append(policy.CustomEmojiIDs, value.DocumentID)
			default:
				return domain.ChannelReactionPolicy{}, tgerr400("REACTION_INVALID")
			}
		}
	default:
		return domain.ChannelReactionPolicy{}, tgerr400("REACTION_INVALID")
	}
	return policy, nil
}

func domainPeerColorFromChannelUpdate(req *tg.ChannelsUpdateColorRequest) domain.ChannelPeerColor {
	if req == nil {
		return domain.ChannelPeerColor{}
	}
	color, hasColor := req.GetColor()
	backgroundEmojiID, hasBackground := req.GetBackgroundEmojiID()
	out := domain.ChannelPeerColor{HasColor: hasColor, Color: color}
	if hasBackground {
		out.BackgroundEmojiID = backgroundEmojiID
	}
	return out
}

func domainChannelEmojiStatus(status tg.EmojiStatusClass) (domain.ChannelEmojiStatus, error) {
	switch s := status.(type) {
	case *tg.EmojiStatusEmpty:
		return domain.ChannelEmojiStatus{}, nil
	case *tg.EmojiStatus:
		if s.DocumentID <= 0 {
			return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
		}
		until, _ := s.GetUntil()
		if until < 0 {
			return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
		}
		return domain.ChannelEmojiStatus{DocumentID: s.DocumentID, Until: until}, nil
	case *tg.EmojiStatusCollectible:
		if s.DocumentID <= 0 {
			return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
		}
		until, _ := s.GetUntil()
		if until < 0 {
			return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
		}
		return domain.ChannelEmojiStatus{DocumentID: s.DocumentID, Until: until}, nil
	case *tg.InputEmojiStatusCollectible:
		return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
	default:
		return domain.ChannelEmojiStatus{}, tgerr400("EMOJI_STATUS_INVALID")
	}
}

func domainChannelParticipantsFilter(filter tg.ChannelParticipantsFilterClass) domain.ChannelParticipantsFilter {
	switch f := filter.(type) {
	case *tg.ChannelParticipantsAdmins:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsAdmins}
	case *tg.ChannelParticipantsKicked:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsKicked, Query: f.Q}
	case *tg.ChannelParticipantsBanned:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsBanned, Query: f.Q}
	case *tg.ChannelParticipantsSearch:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsSearch, Query: f.Q}
	case *tg.ChannelParticipantsBots:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsBots}
	case *tg.ChannelParticipantsContacts:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsContacts, Query: f.Q}
	case *tg.ChannelParticipantsMentions:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsMentions, Query: f.Q}
	default:
		return domain.ChannelParticipantsFilter{Kind: domain.ChannelParticipantsRecent}
	}
}

func legacyBasicGroupAdminRights() tg.ChatAdminRights {
	return tg.ChatAdminRights{
		ChangeInfo:     true,
		DeleteMessages: true,
		BanUsers:       true,
		InviteUsers:    true,
		PinMessages:    true,
		Other:          true,
	}
}

func channelIDFromLegacyInputPeer(userID int64, peer tg.InputPeerClass) (int64, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return p.ChannelID, p.ChannelID > 0
	case *tg.InputPeerChat:
		return p.ChatID, p.ChatID > 0
	case *tg.InputPeerChannelFromMessage:
		return p.ChannelID, p.ChannelID > 0
	default:
		return 0, false
	}
}

func (r *Router) channelIDFromLegacyInputPeerChecked(ctx context.Context, userID int64, peer tg.InputPeerClass) (int64, error) {
	channelID, ok := channelIDFromLegacyInputPeer(userID, peer)
	if !ok {
		return 0, peerIDInvalidErr()
	}
	if err := r.validateInputPeerChannelAccess(ctx, userID, peer, channelID); err != nil {
		return 0, err
	}
	return channelID, nil
}

func isChannelNotFound(err error) bool {
	return errors.Is(err, domain.ErrChannelInvalid) ||
		errors.Is(err, domain.ErrChannelPrivate) ||
		errors.Is(err, domain.ErrChannelUserBanned)
}

func channelInvalidErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrChannelTitleInvalid):
		return tgerr400("CHAT_TITLE_EMPTY")
	case errors.Is(err, domain.ErrChannelInvalid):
		return tgerr400("CHANNEL_INVALID")
	case errors.Is(err, domain.ErrChannelPrivate):
		return tgerr400("CHANNEL_PRIVATE")
	case errors.Is(err, domain.ErrChannelUserBanned):
		return tgerr400("USER_BANNED_IN_CHANNEL")
	case errors.Is(err, domain.ErrChannelWriteForbidden):
		return tgerr400("CHAT_WRITE_FORBIDDEN")
	case errors.Is(err, domain.ErrChannelAdminRequired):
		return tgerr400("CHAT_ADMIN_REQUIRED")
	case errors.Is(err, domain.ErrUserAlreadyParticipant):
		return tgerr400("USER_ALREADY_PARTICIPANT")
	case errors.Is(err, domain.ErrReplyMessageIDInvalid):
		return replyMessageIDInvalidErr()
	default:
		if seconds, ok := domain.SlowModeWaitSeconds(err); ok {
			return tgerr.New(420, fmt.Sprintf("SLOWMODE_WAIT_%d", seconds))
		}
		return internalErr()
	}
}

func channelDeleteErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	case errors.Is(err, domain.ErrMessageAuthorRequired):
		return messageAuthorRequiredErr()
	case errors.Is(err, domain.ErrChannelAdminRequired):
		return tgerr400("CHAT_ADMIN_REQUIRED")
	default:
		return channelInvalidErr(err)
	}
}

func channelAdminErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrChannelNotModified):
		return tgerr400("CHAT_NOT_MODIFIED")
	case errors.Is(err, domain.ErrChatPublicRequired):
		return tgerr400("CHAT_PUBLIC_REQUIRED")
	case errors.Is(err, domain.ErrChatDiscussionUnallowed):
		return tgerr400("CHAT_DISCUSSION_UNALLOWED")
	case errors.Is(err, domain.ErrChannelRightForbidden):
		return tgerr.New(403, "RIGHT_FORBIDDEN")
	case errors.Is(err, domain.ErrChannelUserCreator):
		return tgerr400("USER_CREATOR")
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	default:
		return channelInvalidErr(err)
	}
}

func channelDiscussionErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrLinkNotModified):
		return tgerr400("LINK_NOT_MODIFIED")
	case errors.Is(err, domain.ErrBroadcastIDInvalid):
		return tgerr400("BROADCAST_ID_INVALID")
	case errors.Is(err, domain.ErrMegagroupIDInvalid):
		return tgerr400("MEGAGROUP_ID_INVALID")
	case errors.Is(err, domain.ErrMegagroupPrehistoryHidden):
		return tgerr400("MEGAGROUP_PREHISTORY_HIDDEN")
	default:
		return channelAdminErr(err)
	}
}

func channelInviteErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrInviteHashEmpty):
		return tgerr400("INVITE_HASH_EMPTY")
	case errors.Is(err, domain.ErrInviteHashInvalid):
		return tgerr400("INVITE_HASH_INVALID")
	case errors.Is(err, domain.ErrInviteHashExpired):
		return tgerr.New(406, "INVITE_HASH_EXPIRED")
	case errors.Is(err, domain.ErrInvitePermanent):
		return tgerr400("CHAT_INVITE_PERMANENT")
	case errors.Is(err, domain.ErrInviteRevokedMissing):
		return tgerr400("INVITE_REVOKED_MISSING")
	case errors.Is(err, domain.ErrInviteRequestSent):
		return tgerr400("INVITE_REQUEST_SENT")
	case errors.Is(err, domain.ErrHideRequesterMissing):
		return tgerr400("HIDE_REQUESTER_MISSING")
	case errors.Is(err, domain.ErrUsersTooMuch):
		return tgerr400("USERS_TOO_MUCH")
	case errors.Is(err, domain.ErrUserAlreadyParticipant):
		return tgerr400("USER_ALREADY_PARTICIPANT")
	case errors.Is(err, domain.ErrUserKicked):
		return tgerr400("USER_KICKED")
	default:
		return channelInvalidErr(err)
	}
}

func channelUsernameErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrUsernameInvalid):
		return usernameInvalidErr()
	case errors.Is(err, domain.ErrUsernameOccupied):
		return usernameOccupiedErr()
	case errors.Is(err, domain.ErrChannelNotModified):
		return usernameNotModifiedErr()
	default:
		return channelAdminErr(err)
	}
}

func tgerr400(message string) error {
	return tgerr.New(400, message)
}
