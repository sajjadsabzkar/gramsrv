package rpc

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"

	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

const (
	maxSendMessageTextLength = domain.MaxMessageTextLength
	maxReplyQuoteLength      = domain.MaxMessageReplyQuoteLength
	maxMessageSearchQLength  = 256
	maxMessageEntityCount    = domain.MaxMessageEntityCount
	maxGetMessagesIDs        = 100
	maxMessageSearchFilters  = 32
	maxForumTopicIDs         = 100
	maxDialogInputPeers      = 100
	maxSearchResultsLimit    = 100
	maxReactionVector        = 16
	maxReactionListOffset    = 128
	maxReportOptionLength    = 32
	maxReportCommentLength   = 1024
	maxReportRandomIDLength  = 128
	maxReadMetrics           = 100
	maxBusinessConnIDLength  = 128
	maxSendMultiMediaItems   = 10
	maxForumTopicTitleLength = 128
	maxPollVoteOptions       = 10
	maxPollOptionBytes       = 256
	maxPollVotesOffsetLength = 128
	maxTodoItems             = 30
	maxTodoTitleLength       = 200
	maxCommonChatsLimit      = domain.MaxCommonChannelsLimit
	maxStickerSearchQLength  = 128
	maxStickerSearchLangs    = 16
	maxEmojiLangCodeLength   = 32
	maxEmojiDocuments        = 100
	maxSavedReactionTagTitle = 12
	defaultTopReactionsLimit = 14
	sendMessageRateLimit     = 30
	sendMessageRateWindow    = time.Minute
	forumGeneralTopicID      = 1
	forumGeneralIconColor    = 0x6FB9F0
)

type accountDefaultReactionService interface {
	SetDefaultReaction(ctx context.Context, userID int64, reaction domain.MessageReaction) (domain.AccountReactionSettings, error)
}

type accountPaidReactionPrivacyService interface {
	GetReactionSettings(ctx context.Context, userID int64) (domain.AccountReactionSettings, error)
	SetPaidReactionPrivacy(ctx context.Context, userID int64, privacy domain.PaidReactionPrivacy) (domain.AccountReactionSettings, error)
}

type messageReactionUpdateRecorder interface {
	RecordMessageReactions(ctx context.Context, authKeyID [8]byte, userID int64, msg domain.Message) (domain.UpdateEvent, domain.UpdateState, error)
}

type messageReactionUsageRecorder interface {
	RecordMessageReactionUse(ctx context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error
}

type channelParticipantReactionModerator interface {
	DeleteParticipantReaction(ctx context.Context, userID int64, req domain.DeleteChannelParticipantReactionRequest) (domain.ChannelMessageReactionsResult, error)
	DeleteParticipantReactions(ctx context.Context, userID int64, req domain.DeleteChannelParticipantReactionsRequest) (domain.DeleteChannelParticipantReactionsResult, error)
}

// registerMessages 注册 messages.* RPC handler。
func (r *Router) registerMessages(d *tg.ServerDispatcher) {
	d.OnMessagesSetTyping(r.onMessagesSetTyping)
	d.OnMessagesSaveDraft(r.onMessagesSaveDraft)
	d.OnMessagesSaveDefaultSendAs(r.onMessagesSaveDefaultSendAs)
	d.OnMessagesGetAllDrafts(r.onMessagesGetAllDrafts)
	d.OnMessagesClearAllDrafts(r.onMessagesClearAllDrafts)
	d.OnMessagesGetAllStickers(r.onMessagesGetAllStickers)
	d.OnMessagesGetEmojiStickers(r.onMessagesGetEmojiStickers)
	d.OnMessagesGetFeaturedStickers(func(ctx context.Context, hash int64) (tg.MessagesFeaturedStickersClass, error) {
		return messagesFeaturedStickersEmpty(hash), nil
	})
	d.OnMessagesGetFeaturedEmojiStickers(func(ctx context.Context, hash int64) (tg.MessagesFeaturedStickersClass, error) {
		return messagesFeaturedStickersEmpty(hash), nil
	})
	d.OnMessagesGetRecentStickers(func(ctx context.Context, req *tg.MessagesGetRecentStickersRequest) (tg.MessagesRecentStickersClass, error) {
		if req.Hash != 0 {
			return &tg.MessagesRecentStickersNotModified{}, nil
		}
		return &tg.MessagesRecentStickers{
			Packs:    []tg.StickerPack{},
			Stickers: []tg.DocumentClass{},
			Dates:    []int{},
		}, nil
	})
	d.OnMessagesGetFavedStickers(func(ctx context.Context, hash int64) (tg.MessagesFavedStickersClass, error) {
		if hash != 0 {
			return &tg.MessagesFavedStickersNotModified{}, nil
		}
		return &tg.MessagesFavedStickers{
			Packs:    []tg.StickerPack{},
			Stickers: []tg.DocumentClass{},
		}, nil
	})
	d.OnMessagesGetSavedGifs(func(ctx context.Context, hash int64) (tg.MessagesSavedGifsClass, error) {
		if hash != 0 {
			return &tg.MessagesSavedGifsNotModified{}, nil
		}
		return &tg.MessagesSavedGifs{Gifs: []tg.DocumentClass{}}, nil
	})
	d.OnMessagesSendMessage(r.onMessagesSendMessage)
	d.OnMessagesForwardMessages(r.onMessagesForwardMessages)
	d.OnMessagesGetDialogFilters(r.onMessagesGetDialogFilters)
	d.OnMessagesGetSuggestedDialogFilters(func(ctx context.Context) ([]tg.DialogFilterSuggested, error) {
		return []tg.DialogFilterSuggested{}, nil
	})
	d.OnMessagesUpdateDialogFilter(r.onMessagesUpdateDialogFilter)
	d.OnMessagesUpdateDialogFiltersOrder(r.onMessagesUpdateDialogFiltersOrder)
	d.OnMessagesToggleDialogFilterTags(r.onMessagesToggleDialogFilterTags)
	d.OnMessagesGetSavedDialogs(func(ctx context.Context, req *tg.MessagesGetSavedDialogsRequest) (tg.MessagesSavedDialogsClass, error) {
		if req.Hash != 0 {
			return &tg.MessagesSavedDialogsNotModified{Count: 0}, nil
		}
		return &tg.MessagesSavedDialogs{}, nil
	})
	d.OnMessagesGetPinnedSavedDialogs(func(ctx context.Context) (tg.MessagesSavedDialogsClass, error) {
		return &tg.MessagesSavedDialogs{}, nil
	})
	d.OnMessagesToggleSavedDialogPin(func(ctx context.Context, req *tg.MessagesToggleSavedDialogPinRequest) (bool, error) {
		return true, nil
	})
	d.OnMessagesReorderPinnedSavedDialogs(func(ctx context.Context, req *tg.MessagesReorderPinnedSavedDialogsRequest) (bool, error) {
		return true, nil
	})
	d.OnMessagesGetSavedDialogsByID(func(ctx context.Context, req *tg.MessagesGetSavedDialogsByIDRequest) (tg.MessagesSavedDialogsClass, error) {
		return &tg.MessagesSavedDialogs{}, nil
	})
	d.OnMessagesGetSavedHistory(r.onMessagesGetSavedHistory)
	d.OnMessagesReadSavedHistory(r.onMessagesReadSavedHistory)
	d.OnMessagesDeleteSavedHistory(r.onMessagesDeleteSavedHistory)
	d.OnMessagesGetCommonChats(r.onMessagesGetCommonChats)
	d.OnMessagesGetDefaultHistoryTTL(r.onMessagesGetDefaultHistoryTTL)
	d.OnMessagesGetSponsoredMessages(r.onMessagesGetSponsoredMessages)
	d.OnMessagesGetWebPagePreview(r.onMessagesGetWebPagePreview)
	d.OnMessagesUploadMedia(r.onMessagesUploadMedia)
	d.OnMessagesSendMedia(r.onMessagesSendMedia)
	d.OnMessagesSendMultiMedia(r.onMessagesSendMultiMedia)
	d.OnMessagesReportSpam(r.onMessagesReportSpam)
	d.OnMessagesReport(r.onMessagesReport)
	d.OnMessagesReportReaction(r.onMessagesReportReaction)
	d.OnMessagesReportMessagesDelivery(r.onMessagesReportMessagesDelivery)
	d.OnMessagesReportReadMetrics(r.onMessagesReportReadMetrics)
	d.OnMessagesReportMusicListen(r.onMessagesReportMusicListen)
	d.OnMessagesReportSponsoredMessage(r.onMessagesReportSponsoredMessage)
	d.OnMessagesReadMessageContents(r.onMessagesReadMessageContents)
	d.OnMessagesGetMessagesViews(r.onMessagesGetMessagesViews)
	d.OnMessagesGetUnreadMentions(r.onMessagesGetUnreadMentions)
	d.OnMessagesReadMentions(r.onMessagesReadMentions)
	d.OnMessagesGetSearchCounters(r.onMessagesGetSearchCounters)
	d.OnMessagesGetReplies(r.onMessagesGetReplies)
	d.OnMessagesGetDiscussionMessage(r.onMessagesGetDiscussionMessage)
	d.OnMessagesReadDiscussion(r.onMessagesReadDiscussion)
	d.OnMessagesGetForumTopics(r.onMessagesGetForumTopics)
	d.OnMessagesGetForumTopicsByID(r.onMessagesGetForumTopicsByID)
	d.OnMessagesGetOnlines(r.onMessagesGetOnlines)
	d.OnMessagesGetAvailableReactions(r.onMessagesGetAvailableReactions)
	d.OnMessagesGetAvailableEffects(func(ctx context.Context, hash int) (tg.MessagesAvailableEffectsClass, error) {
		return &tg.MessagesAvailableEffects{
			Hash:      0,
			Effects:   []tg.AvailableEffect{},
			Documents: []tg.DocumentClass{},
		}, nil
	})
	d.OnMessagesGetStickers(func(ctx context.Context, req *tg.MessagesGetStickersRequest) (tg.MessagesStickersClass, error) {
		return tdesktop.Stickers(), nil
	})
	d.OnMessagesGetStickerSet(r.onMessagesGetStickerSet)
	d.OnMessagesGetEmojiGroups(func(ctx context.Context, hash int) (tg.MessagesEmojiGroupsClass, error) {
		return tdesktop.EmojiGroups(), nil
	})
	d.OnMessagesGetEmojiStickerGroups(func(ctx context.Context, hash int) (tg.MessagesEmojiGroupsClass, error) {
		return tdesktop.EmojiGroups(), nil
	})
	d.OnMessagesGetEmojiProfilePhotoGroups(func(ctx context.Context, hash int) (tg.MessagesEmojiGroupsClass, error) {
		return tdesktop.EmojiProfilePhotoGroups(), nil
	})
	d.OnMessagesGetEmojiKeywords(r.onMessagesGetEmojiKeywords)
	d.OnMessagesGetEmojiKeywordsDifference(r.onMessagesGetEmojiKeywordsDifference)
	d.OnMessagesGetEmojiKeywordsLanguages(func(ctx context.Context, langcodes []string) ([]tg.EmojiLanguage, error) {
		return []tg.EmojiLanguage{}, nil
	})
	d.OnMessagesGetCustomEmojiDocuments(r.onMessagesGetCustomEmojiDocuments)
	d.OnMessagesGetAttachedStickers(r.onMessagesGetAttachedStickers)
	d.OnMessagesSearchStickerSets(r.onMessagesSearchStickerSets)
	d.OnMessagesSearchStickers(r.onMessagesSearchStickers)
	d.OnMessagesGetAttachMenuBots(func(ctx context.Context, hash int64) (tg.AttachMenuBotsClass, error) {
		return tdesktop.AttachMenuBots(), nil
	})
	d.OnMessagesGetQuickReplies(func(ctx context.Context, hash int64) (tg.MessagesQuickRepliesClass, error) {
		return tdesktop.QuickReplies(), nil
	})
	d.OnMessagesGetWebPage(func(ctx context.Context, req *tg.MessagesGetWebPageRequest) (*tg.MessagesWebPage, error) {
		return tdesktop.WebPage(req.URL), nil
	})
	d.OnMessagesGetDialogs(func(ctx context.Context, req *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
		if r.deps.Dialogs == nil {
			return &tg.MessagesDialogs{}, nil
		}
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		filter, err := r.dialogFilterFromRequest(ctx, userID, req)
		if err != nil {
			return nil, err
		}
		list, err := r.deps.Dialogs.GetDialogs(ctx, userID, filter)
		if err != nil {
			return nil, internalErr()
		}
		if filter.Hash != 0 && list.Hash == filter.Hash {
			return &tg.MessagesDialogsNotModified{Count: list.Count}, nil
		}
		return tgMessagesDialogs(userID, r.withDialogListPresence(list)), nil
	})
	d.OnMessagesGetPinnedDialogs(func(ctx context.Context, folderID int) (*tg.MessagesPeerDialogs, error) {
		id, _ := AuthKeyIDFrom(ctx)
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		var list domain.DialogList
		if r.deps.Dialogs != nil {
			list, err = r.deps.Dialogs.GetDialogs(ctx, userID, domain.DialogFilter{
				PinnedOnly:  true,
				HasFolderID: true,
				FolderID:    folderID,
				Limit:       100,
			})
			if err != nil {
				return nil, internalErr()
			}
		}
		st := domain.UpdateState{Date: int(r.clock.Now().Unix())}
		if r.deps.Updates != nil {
			var err error
			st, err = r.deps.Updates.GetState(ctx, id, userID)
			if err != nil {
				return nil, internalErr()
			}
		}
		return tgPeerDialogs(userID, r.withDialogListPresence(list), st), nil
	})
	d.OnMessagesGetPeerDialogs(func(ctx context.Context, peers []tg.InputDialogPeerClass) (*tg.MessagesPeerDialogs, error) {
		id, _ := AuthKeyIDFrom(ctx)
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		domainPeers, err := r.dialogPeersFromInput(ctx, userID, peers)
		if err != nil {
			return nil, err
		}
		var list domain.DialogList
		if len(domainPeers) > 0 && r.deps.Dialogs != nil {
			var err error
			list, err = r.deps.Dialogs.GetPeerDialogs(ctx, userID, domainPeers)
			if err != nil {
				return nil, internalErr()
			}
		}
		st := domain.UpdateState{Date: int(r.clock.Now().Unix())}
		if r.deps.Updates != nil {
			var err error
			st, err = r.deps.Updates.GetState(ctx, id, userID)
			if err != nil {
				return nil, internalErr()
			}
		}
		r.trackChannelInterest(ctx, userID, channelIDsFromDialogs(list)...)
		return tgPeerDialogs(userID, r.withDialogListPresence(list), st), nil
	})
	d.OnMessagesGetPeerSettings(r.onMessagesGetPeerSettings)
	d.OnMessagesToggleDialogPin(r.onMessagesToggleDialogPin)
	d.OnMessagesReorderPinnedDialogs(r.onMessagesReorderPinnedDialogs)
	d.OnMessagesMarkDialogUnread(r.onMessagesMarkDialogUnread)
	d.OnMessagesGetDialogUnreadMarks(r.onMessagesGetDialogUnreadMarks)
	d.OnMessagesHidePeerSettingsBar(r.onMessagesHidePeerSettingsBar)
	d.OnMessagesGetMessageEditData(r.onMessagesGetMessageEditData)
	d.OnMessagesEditMessage(r.onMessagesEditMessage)
	d.OnMessagesGetOutboxReadDate(r.onMessagesGetOutboxReadDate)
	d.OnMessagesGetMessageReadParticipants(r.onMessagesGetMessageReadParticipants)
	d.OnMessagesDeleteMessages(r.onMessagesDeleteMessages)
	d.OnMessagesDeleteHistory(r.onMessagesDeleteHistory)
	d.OnMessagesGetMessages(r.onMessagesGetMessages)
	d.OnMessagesGetHistory(func(ctx context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		filter, ok := r.messageFilterFromHistoryRequest(userID, req)
		if !ok {
			return messagesNotModifiedOrEmpty(req.Hash), nil
		}
		if filter.Peer.Type == domain.PeerTypeChannel {
			if r.deps.Channels == nil {
				return messagesNotModifiedOrEmpty(req.Hash), nil
			}
			if err := r.validateInputPeerChannelAccess(ctx, userID, req.Peer, filter.Peer.ID); err != nil {
				return nil, err
			}
			if isLegacyInputPeerChat(req.Peer) {
				return &tg.MessagesMessages{}, nil
			}
			history, err := r.deps.Channels.GetHistory(ctx, userID, domain.ChannelHistoryFilter{
				ChannelID:  filter.Peer.ID,
				OffsetID:   filter.OffsetID,
				OffsetDate: filter.OffsetDate,
				AddOffset:  filter.AddOffset,
				Limit:      filter.Limit,
				MaxID:      filter.MaxID,
				MinID:      filter.MinID,
				Hash:       filter.Hash,
			})
			if err != nil {
				return nil, channelInvalidErr(err)
			}
			history = r.enrichChannelHistory(ctx, userID, history)
			r.trackChannelInterest(ctx, userID, filter.Peer.ID)
			if filter.Hash != 0 && history.Hash == filter.Hash {
				return &tg.MessagesMessagesNotModified{Count: history.Count}, nil
			}
			return tgChannelHistoryMessages(userID, history), nil
		}
		r.clearChannelInterest(ctx, userID)
		if r.deps.Messages == nil {
			return messagesNotModifiedOrEmpty(req.Hash), nil
		}
		list, err := r.deps.Messages.GetHistory(ctx, userID, filter)
		if err != nil {
			return nil, internalErr()
		}
		if filter.Hash != 0 && list.Hash == filter.Hash {
			return &tg.MessagesMessagesNotModified{Count: list.Count}, nil
		}
		return tgMessagesMessages(r.withMessageListPresence(list)), nil
	})
	d.OnMessagesReadHistory(func(ctx context.Context, req *tg.MessagesReadHistoryRequest) (*tg.MessagesAffectedMessages, error) {
		id, _ := AuthKeyIDFrom(ctx)
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		peer, peerErr := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
		if peerErr == nil && peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
			read, err := r.deps.Channels.ReadHistory(ctx, userID, domain.ReadChannelHistoryRequest{
				UserID:    userID,
				ChannelID: peer.ID,
				MaxID:     req.MaxID,
				Date:      int(r.clock.Now().Unix()),
			})
			if err != nil {
				return nil, channelInvalidErr(err)
			}
			event, err := r.recordChannelReadInbox(ctx, userID, read)
			if err != nil {
				return nil, err
			}
			r.pushChannelReadOutboxUpdates(ctx, read.ChannelID, read.OutboxUpdates)
			if event.Pts != 0 {
				return &tg.MessagesAffectedMessages{Pts: event.Pts, PtsCount: event.PtsCount}, nil
			}
			return r.affectedMessages(ctx, id, userID)
		}
		if peerErr != nil {
			return nil, peerErr
		}
		if r.deps.Messages != nil {
			sessionID, _ := SessionIDFrom(ctx)
			read, err := r.deps.Messages.ReadHistory(ctx, userID, domain.ReadHistoryRequest{
				OwnerUserID:     userID,
				Peer:            peer,
				MaxID:           req.MaxID,
				Date:            int(r.clock.Now().Unix()),
				OriginAuthKeyID: id,
				OriginSessionID: sessionID,
			})
			if err != nil {
				return nil, internalErr()
			}
			if read.Changed && read.InboxEvent.Pts != 0 {
				r.pushReadHistoryEvent(ctx, read.OwnerUserID, read.InboxEvent)
				if read.OutboxChanged && read.OutboxEvent.Pts != 0 {
					r.pushReadHistoryEvent(ctx, read.OutboxUserID, read.OutboxEvent)
				}
				return &tg.MessagesAffectedMessages{Pts: read.InboxEvent.Pts, PtsCount: read.InboxEvent.PtsCount}, nil
			}
		}
		return r.affectedMessages(ctx, id, userID)
	})
	d.OnMessagesSearch(func(ctx context.Context, req *tg.MessagesSearchRequest) (tg.MessagesMessagesClass, error) {
		if utf8.RuneCountInString(req.Q) > maxMessageSearchQLength {
			return nil, limitInvalidErr()
		}
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		filter := r.messageFilterFromSearchRequest(userID, req)
		if filter.HasPeer && filter.Peer.Type == domain.PeerTypeChannel {
			if r.deps.Channels == nil {
				return messagesNotModifiedOrEmpty(req.Hash), nil
			}
			if err := r.validateInputPeerChannelAccess(ctx, userID, req.Peer, filter.Peer.ID); err != nil {
				return nil, err
			}
			if isLegacyInputPeerChat(req.Peer) {
				return &tg.MessagesMessages{}, nil
			}
			if searchFilterNeedsMediaStore(req.Filter) {
				view, err := r.deps.Channels.GetChannel(ctx, userID, filter.Peer.ID)
				if err != nil {
					return nil, channelInvalidErr(err)
				}
				return tgChannelHistoryMessages(userID, domain.ChannelHistory{Channel: view.Channel}), nil
			}
			chFilter, ok := r.channelHistoryFilterFromSearchRequest(userID, req, filter.Peer.ID)
			if !ok {
				return nil, peerIDInvalidErr()
			}
			history, err := r.deps.Channels.GetHistory(ctx, userID, chFilter)
			if err != nil {
				return nil, channelInvalidErr(err)
			}
			history = r.enrichChannelHistory(ctx, userID, history)
			if chFilter.Hash != 0 && history.Hash == chFilter.Hash {
				return &tg.MessagesMessagesNotModified{Count: history.Count}, nil
			}
			return tgChannelHistoryMessages(userID, history), nil
		}
		if r.deps.Messages == nil {
			return messagesNotModifiedOrEmpty(req.Hash), nil
		}
		if searchFilterNeedsMediaStore(req.Filter) {
			if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
				return nil, err
			}
			return &tg.MessagesMessages{}, nil
		}
		list, err := r.deps.Messages.Search(ctx, userID, filter)
		if err != nil {
			return nil, internalErr()
		}
		if filter.Hash != 0 && list.Hash == filter.Hash {
			return &tg.MessagesMessagesNotModified{Count: list.Count}, nil
		}
		return tgMessagesMessages(r.withMessageListPresence(list)), nil
	})
	d.OnMessagesSearchGlobal(r.onMessagesSearchGlobal)
	d.OnMessagesGetSearchResultsCalendar(r.onMessagesGetSearchResultsCalendar)
	d.OnMessagesGetSearchResultsPositions(r.onMessagesGetSearchResultsPositions)
	d.OnMessagesSendReaction(r.onMessagesSendReaction)
	d.OnMessagesGetMessagesReactions(r.onMessagesGetMessagesReactions)
	d.OnMessagesGetMessageReactionsList(r.onMessagesGetMessageReactionsList)
	d.OnMessagesSetDefaultReaction(r.onMessagesSetDefaultReaction)
	d.OnMessagesGetPaidReactionPrivacy(r.onMessagesGetPaidReactionPrivacy)
	d.OnMessagesTogglePaidReactionPrivacy(r.onMessagesTogglePaidReactionPrivacy)
	d.OnMessagesSendPaidReaction(r.onMessagesSendPaidReaction)
	d.OnMessagesDeleteParticipantReactions(r.onMessagesDeleteParticipantReactions)
	d.OnMessagesDeleteParticipantReaction(r.onMessagesDeleteParticipantReaction)
	d.OnMessagesGetUnreadReactions(r.onMessagesGetUnreadReactions)
	d.OnMessagesReadReactions(r.onMessagesReadReactions)
	d.OnMessagesGetTopReactions(r.onMessagesGetTopReactions)
	d.OnMessagesGetRecentReactions(r.onMessagesGetRecentReactions)
	d.OnMessagesClearRecentReactions(r.onMessagesClearRecentReactions)
	d.OnMessagesGetSavedReactionTags(r.onMessagesGetSavedReactionTags)
	d.OnMessagesUpdateSavedReactionTag(r.onMessagesUpdateSavedReactionTag)
	d.OnMessagesGetDefaultTagReactions(r.onMessagesGetDefaultTagReactions)
	d.OnMessagesSendVote(r.onMessagesSendVote)
	d.OnMessagesGetPollResults(r.onMessagesGetPollResults)
	d.OnMessagesGetPollVotes(r.onMessagesGetPollVotes)
	d.OnMessagesAddPollAnswer(r.onMessagesAddPollAnswer)
	d.OnMessagesDeletePollAnswer(r.onMessagesDeletePollAnswer)
	d.OnMessagesGetUnreadPollVotes(r.onMessagesGetUnreadPollVotes)
	d.OnMessagesReadPollVotes(r.onMessagesReadPollVotes)
	d.OnMessagesAppendTodoList(r.onMessagesAppendTodoList)
	d.OnMessagesToggleTodoCompleted(r.onMessagesToggleTodoCompleted)
	d.OnMessagesGetScheduledHistory(r.onMessagesGetScheduledHistory)
	d.OnMessagesGetScheduledMessages(r.onMessagesGetScheduledMessages)
	d.OnMessagesSendScheduledMessages(r.onMessagesSendScheduledMessages)
	d.OnMessagesDeleteScheduledMessages(r.onMessagesDeleteScheduledMessages)
	d.OnMessagesCreateForumTopic(r.onMessagesCreateForumTopic)
	d.OnMessagesEditForumTopic(r.onMessagesEditForumTopic)
	d.OnMessagesUpdatePinnedForumTopic(r.onMessagesUpdatePinnedForumTopic)
	d.OnMessagesReorderPinnedForumTopics(r.onMessagesReorderPinnedForumTopics)
	d.OnMessagesDeleteTopicHistory(r.onMessagesDeleteTopicHistory)
}

func (r *Router) onMessagesGetSavedHistory(ctx context.Context, req *tg.MessagesGetSavedHistoryRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateSavedHistoryBounds(req.OffsetID, req.OffsetDate, req.AddOffset, req.Limit, req.MaxID, req.MinID); err != nil {
		return nil, err
	}
	parentPeer, hasParent, err := r.validateSavedHistoryParentPeer(ctx, userID, req.GetParentPeer)
	if err != nil {
		return nil, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if req.Hash != 0 {
		return &tg.MessagesMessagesNotModified{Count: 0}, nil
	}
	chats := r.savedHistoryChats(ctx, userID, hasParent, parentPeer, req.Peer)
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Chats:    chats,
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesReadSavedHistory(ctx context.Context, req *tg.MessagesReadSavedHistoryRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if req.MaxID < 0 || req.MaxID > domain.MaxMessageBoxID {
		return false, messageIDInvalidErr()
	}
	if err := r.validateRequiredSavedHistoryParentPeer(ctx, userID, req.ParentPeer); err != nil {
		return false, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesDeleteSavedHistory(ctx context.Context, req *tg.MessagesDeleteSavedHistoryRequest) (*tg.MessagesAffectedHistory, error) {
	authKeyID, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.MaxID < 0 || req.MaxID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if minDate, ok := req.GetMinDate(); ok && minDate < 0 {
		return nil, limitInvalidErr()
	}
	if maxDate, ok := req.GetMaxDate(); ok && maxDate < 0 {
		return nil, limitInvalidErr()
	}
	if _, _, err := r.validateSavedHistoryParentPeer(ctx, userID, req.GetParentPeer); err != nil {
		return nil, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	return r.affectedHistory(ctx, authKeyID, userID, 0)
}

func (r *Router) onMessagesGetCommonChats(ctx context.Context, req *tg.MessagesGetCommonChatsRequest) (tg.MessagesChatsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.Limit < 0 || req.Limit > maxCommonChatsLimit {
		return nil, limitInvalidErr()
	}
	if req.MaxID < 0 {
		return nil, messageIDInvalidErr()
	}
	target, found, err := r.userFromInput(ctx, userID, req.UserID)
	if err != nil {
		return nil, internalErr()
	}
	if !found || target.ID == 0 || target.ID == userID {
		return nil, userIDInvalidErr()
	}
	if req.Limit == 0 || r.deps.Channels == nil {
		return &tg.MessagesChats{Chats: []tg.ChatClass{}}, nil
	}
	common, err := r.deps.Channels.CommonChannels(ctx, userID, domain.CommonChannelsRequest{
		UserID:       userID,
		TargetUserID: target.ID,
		MaxID:        req.MaxID,
		Limit:        req.Limit,
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	chats := make([]tg.ChatClass, 0, len(common.Channels))
	for _, ch := range common.Channels {
		chats = append(chats, tgChannelChat(userID, ch, nil))
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func (r *Router) onMessagesGetAttachedStickers(ctx context.Context, media tg.InputStickeredMediaClass) ([]tg.StickerSetCoveredClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if media == nil {
		return nil, mediaEmptyErr()
	}
	return []tg.StickerSetCoveredClass{}, nil
}

func (r *Router) onMessagesGetCustomEmojiDocuments(ctx context.Context, documentIDs []int64) ([]tg.DocumentClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if len(documentIDs) > maxEmojiDocuments {
		return nil, limitInvalidErr()
	}
	for _, id := range documentIDs {
		if id <= 0 {
			return nil, messageIDInvalidErr()
		}
	}
	if r.deps.Files == nil || len(documentIDs) == 0 {
		return []tg.DocumentClass{}, nil
	}
	docs, err := r.deps.Files.GetDocuments(ctx, documentIDs)
	if err != nil {
		return nil, internalErr()
	}
	byID := documentsByID(docs)
	out := make([]tg.DocumentClass, 0, len(documentIDs))
	for _, id := range documentIDs {
		if d, ok := byID[id]; ok {
			out = append(out, tgDocument(d))
		} else {
			out = append(out, &tg.DocumentEmpty{ID: id})
		}
	}
	return out, nil
}

func (r *Router) onMessagesSearchStickerSets(ctx context.Context, req *tg.MessagesSearchStickerSetsRequest) (tg.MessagesFoundStickerSetsClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if utf8.RuneCountInString(req.Q) > maxStickerSearchQLength {
		return nil, limitInvalidErr()
	}
	if req.Hash != 0 {
		return &tg.MessagesFoundStickerSetsNotModified{}, nil
	}
	return &tg.MessagesFoundStickerSets{
		Hash: 0,
		Sets: []tg.StickerSetCoveredClass{},
	}, nil
}

func (r *Router) onMessagesSearchStickers(ctx context.Context, req *tg.MessagesSearchStickersRequest) (tg.MessagesFoundStickersClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if req.Offset < 0 || req.Offset > domain.MaxMessageBoxID || req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	if utf8.RuneCountInString(req.Q) > maxStickerSearchQLength || utf8.RuneCountInString(req.Emoticon) > maxStickerSearchQLength {
		return nil, limitInvalidErr()
	}
	if len(req.LangCode) > maxStickerSearchLangs {
		return nil, limitInvalidErr()
	}
	for _, lang := range req.LangCode {
		if err := validateEmojiLangCode(lang); err != nil {
			return nil, err
		}
	}
	if req.Hash != 0 {
		return &tg.MessagesFoundStickersNotModified{}, nil
	}
	return &tg.MessagesFoundStickers{
		Hash:     0,
		Stickers: []tg.DocumentClass{},
	}, nil
}

func (r *Router) onMessagesGetEmojiKeywords(ctx context.Context, langcode string) (*tg.EmojiKeywordsDifference, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if err := validateEmojiLangCode(langcode); err != nil {
		return nil, err
	}
	return &tg.EmojiKeywordsDifference{
		LangCode:    langcode,
		FromVersion: 0,
		Version:     0,
		Keywords:    []tg.EmojiKeywordClass{},
	}, nil
}

func (r *Router) onMessagesGetEmojiKeywordsDifference(ctx context.Context, req *tg.MessagesGetEmojiKeywordsDifferenceRequest) (*tg.EmojiKeywordsDifference, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if err := validateEmojiLangCode(req.LangCode); err != nil {
		return nil, err
	}
	if req.FromVersion < 0 {
		return nil, limitInvalidErr()
	}
	return &tg.EmojiKeywordsDifference{
		LangCode:    req.LangCode,
		FromVersion: req.FromVersion,
		Version:     req.FromVersion,
		Keywords:    []tg.EmojiKeywordClass{},
	}, nil
}

func (r *Router) onMessagesGetExtendedMedia(ctx context.Context, req *tg.MessagesGetExtendedMediaRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateMessageIDVector(req.ID); err != nil {
		return nil, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}

func (r *Router) onMessagesGetTopReactions(ctx context.Context, req *tg.MessagesGetTopReactionsRequest) (tg.MessagesReactionsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultTopReactionsLimit
	}
	reactions := []domain.MessageReaction{}
	if r.deps.Channels != nil {
		var err error
		reactions, err = r.deps.Channels.TopReactions(ctx, userID, limit)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
	}
	return messagesReactionsFromDomain(r.reactionsWithCatalogFallback(ctx, reactions, limit), req.Hash), nil
}

func (r *Router) onMessagesGetRecentReactions(ctx context.Context, req *tg.MessagesGetRecentReactionsRequest) (tg.MessagesReactionsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	if r.deps.Channels == nil {
		return messagesReactionsEmpty(req.Hash), nil
	}
	reactions, err := r.deps.Channels.RecentReactions(ctx, userID, req.Limit)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	return messagesReactionsFromDomain(reactions, req.Hash), nil
}

func (r *Router) onMessagesClearRecentReactions(ctx context.Context) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if r.deps.Channels != nil {
		if err := r.deps.Channels.ClearRecentReactions(ctx, userID); err != nil {
			return false, channelInvalidErr(err)
		}
	}
	return true, nil
}

func (r *Router) onMessagesGetSavedReactionTags(ctx context.Context, req *tg.MessagesGetSavedReactionTagsRequest) (tg.MessagesSavedReactionTagsClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if peer, ok := req.GetPeer(); ok && peer != nil {
		if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer); err != nil {
			return nil, err
		}
		return savedReactionTagsEmpty(req.Hash), nil
	}
	if r.deps.Channels == nil {
		return savedReactionTagsEmpty(req.Hash), nil
	}
	tags, err := r.deps.Channels.SavedReactionTags(ctx, userID, domain.MaxSavedReactionTags)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	return savedReactionTagsFromDomain(tags, req.Hash), nil
}

func (r *Router) onMessagesUpdateSavedReactionTag(ctx context.Context, req *tg.MessagesUpdateSavedReactionTagRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	reaction, err := domainMessageReactionFromTL(req.Reaction)
	if err != nil {
		return false, err
	}
	title, ok := req.GetTitle()
	if !ok {
		title = ""
	}
	if utf8.RuneCountInString(title) > maxSavedReactionTagTitle {
		return false, limitInvalidErr()
	}
	if r.deps.Channels != nil {
		if err := r.deps.Channels.UpdateSavedReactionTag(ctx, userID, domain.SavedReactionTag{
			UserID:   userID,
			Reaction: reaction,
			Title:    title,
		}); err != nil {
			return false, channelInvalidErr(err)
		}
	}
	r.pushUserUpdates(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateSavedReactionTags{}},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	})
	return true, nil
}

func (r *Router) onMessagesGetDefaultTagReactions(ctx context.Context, hash int64) (tg.MessagesReactionsClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	return messagesReactionsEmpty(hash), nil
}

func (r *Router) onMessagesGetWebPagePreview(ctx context.Context, req *tg.MessagesGetWebPagePreviewRequest) (*tg.MessagesWebPagePreview, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		return nil, messageEmptyErr()
	}
	if utf8.RuneCountInString(message) > maxSendMessageTextLength || len(req.Entities) > maxMessageEntityCount {
		return nil, limitInvalidErr()
	}
	return &tg.MessagesWebPagePreview{
		Media: &tg.MessageMediaEmpty{},
		Chats: []tg.ChatClass{},
		Users: []tg.UserClass{},
	}, nil
}

// onMessagesUploadMedia / onMessagesSendMedia / onMessagesSendMultiMedia 实现见 send_media.go。

func (r *Router) onMessagesGetScheduledMessages(ctx context.Context, req *tg.MessagesGetScheduledMessagesRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateMessageIDVector(req.ID); err != nil {
		return nil, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesGetScheduledHistory(ctx context.Context, req *tg.MessagesGetScheduledHistoryRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if req.Hash != 0 {
		return &tg.MessagesMessagesNotModified{Count: 0}, nil
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesSendScheduledMessages(ctx context.Context, req *tg.MessagesSendScheduledMessagesRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateMessageIDVector(req.ID); err != nil {
		return nil, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if len(req.ID) == 0 {
		return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) onMessagesDeleteScheduledMessages(ctx context.Context, req *tg.MessagesDeleteScheduledMessagesRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateMessageIDVector(req.ID); err != nil {
		return nil, err
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateDeleteScheduledMessages{
			Peer:     tgPeer(peer),
			Messages: append([]int(nil), req.ID...),
		}},
		Users: []tg.UserClass{},
		Chats: r.chatsForInputPeer(ctx, userID, req.Peer),
		Date:  int(r.clock.Now().Unix()),
		Seq:   0,
	}, nil
}

func (r *Router) onMessagesCreateForumTopic(ctx context.Context, req *tg.MessagesCreateForumTopicRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateForumTopicTitle(req.Title, req.TitleMissing); err != nil {
		return nil, err
	}
	if req.RandomID == 0 {
		return nil, randomIDEmptyErr()
	}
	peer, err := r.forumTopicPeer(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, internalErr()
	}
	var sendAs *domain.Peer
	if req.SendAs != nil {
		sendAs, err = r.forumSendAsPeer(ctx, req.Peer, req.SendAs)
		if err != nil {
			return nil, err
		}
	}
	res, err := r.deps.Channels.CreateForumTopic(ctx, userID, domain.CreateChannelForumTopicRequest{
		UserID:       userID,
		ChannelID:    peer.ID,
		Title:        strings.TrimSpace(req.Title),
		TitleMissing: req.TitleMissing,
		IconColor:    req.IconColor,
		IconEmojiID:  req.IconEmojiID,
		RandomID:     req.RandomID,
		SendAs:       sendAs,
		Date:         int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, forumTopicError(err)
	}
	sendRes := domain.SendChannelMessageResult{
		Channel:    res.Channel,
		Message:    res.Message,
		Event:      res.Event,
		Recipients: res.Recipients,
		Duplicate:  res.Duplicate,
	}
	updates := r.channelMessageUpdates(ctx, userID, sendRes, req.RandomID)
	if !res.Duplicate {
		r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelMessageUpdates(ctx, viewerUserID, sendRes, 0)
		})
	}
	return updates, nil
}

func (r *Router) onMessagesEditForumTopic(ctx context.Context, req *tg.MessagesEditForumTopicRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if title, ok := req.GetTitle(); ok {
		if err := validateForumTopicTitle(title, false); err != nil {
			return nil, err
		}
	}
	if req.TopicID <= 0 || req.TopicID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.forumTopicPeer(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, internalErr()
	}
	edit := domain.EditChannelForumTopicRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		TopicID:   req.TopicID,
		Date:      int(r.clock.Now().Unix()),
	}
	if title, ok := req.GetTitle(); ok {
		edit.Title = &title
	}
	if iconEmojiID, ok := req.GetIconEmojiID(); ok {
		edit.IconEmojiID = &iconEmojiID
	}
	if closed, ok := req.GetClosed(); ok {
		edit.Closed = &closed
	}
	if hidden, ok := req.GetHidden(); ok {
		edit.Hidden = &hidden
	}
	res, err := r.deps.Channels.EditForumTopic(ctx, userID, edit)
	if err != nil {
		return nil, forumTopicError(err)
	}
	sendRes := domain.SendChannelMessageResult{
		Channel:    res.Channel,
		Message:    res.Message,
		Event:      res.Event,
		Recipients: res.Recipients,
	}
	updates := r.channelMessageUpdates(ctx, userID, sendRes, 0)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelMessageUpdates(ctx, viewerUserID, sendRes, 0)
	})
	return updates, nil
}

func (r *Router) onMessagesUpdatePinnedForumTopic(ctx context.Context, req *tg.MessagesUpdatePinnedForumTopicRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.TopicID <= 0 || req.TopicID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.forumTopicPeer(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.UpdatePinnedForumTopic(ctx, userID, domain.UpdateChannelForumTopicPinnedRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		TopicID:   req.TopicID,
		Pinned:    req.Pinned,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, forumTopicError(err)
	}
	updates := r.pinnedForumTopicUpdates(userID, res.Channel, res.Topic.TopicID, res.Topic.Pinned)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.pinnedForumTopicUpdates(viewerUserID, res.Channel, res.Topic.TopicID, res.Topic.Pinned)
	})
	return updates, nil
}

func (r *Router) onMessagesReorderPinnedForumTopics(ctx context.Context, req *tg.MessagesReorderPinnedForumTopicsRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if err := validateMessageIDVector(req.Order); err != nil {
		return nil, err
	}
	peer, err := r.forumTopicPeer(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.ReorderPinnedForumTopics(ctx, userID, domain.ReorderChannelPinnedForumTopicsRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		Order:     append([]int(nil), req.Order...),
		Force:     req.Force,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, forumTopicError(err)
	}
	updates := r.pinnedForumTopicsOrderUpdates(userID, res.Channel, res.Order)
	r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.pinnedForumTopicsOrderUpdates(viewerUserID, res.Channel, res.Order)
	})
	return updates, nil
}

func (r *Router) onMessagesDeleteTopicHistory(ctx context.Context, req *tg.MessagesDeleteTopicHistoryRequest) (*tg.MessagesAffectedHistory, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.TopMsgID <= 0 || req.TopMsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.forumTopicPeer(ctx, req.Peer)
	if err != nil {
		return nil, err
	}
	if r.deps.Channels == nil {
		return nil, internalErr()
	}
	res, err := r.deps.Channels.DeleteForumTopicHistory(ctx, userID, domain.DeleteChannelForumTopicHistoryRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		TopicID:   req.TopMsgID,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, forumTopicError(err)
	}
	if res.Event.Pts != 0 {
		r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return &tg.Updates{
				Updates: []tg.UpdateClass{tgChannelUpdate(viewerUserID, res.Event)},
				Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, res.Channel, nil)},
				Date:    res.Event.Date,
				Seq:     0,
			}
		})
		return &tg.MessagesAffectedHistory{Pts: res.Event.Pts, PtsCount: res.Event.PtsCount, Offset: res.Offset}, nil
	}
	return &tg.MessagesAffectedHistory{Pts: res.Channel.Pts, PtsCount: 0, Offset: res.Offset}, nil
}

func validateMessageIDVector(ids []int) error {
	if len(ids) > maxGetMessagesIDs {
		return limitInvalidErr()
	}
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return messageIDInvalidErr()
		}
	}
	return nil
}

func validateForumTopicTitle(title string, titleMissing bool) error {
	title = strings.TrimSpace(title)
	if title == "" && !titleMissing {
		return topicTitleEmptyErr()
	}
	if utf8.RuneCountInString(title) > maxForumTopicTitleLength {
		return limitInvalidErr()
	}
	return nil
}

func forumTopicError(err error) error {
	switch {
	case errors.Is(err, domain.ErrChannelForumMissing):
		return channelForumMissingErr()
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return topicIDInvalidErr()
	case errors.Is(err, domain.ErrChannelNotModified):
		return tgerr400("CHAT_NOT_MODIFIED")
	default:
		return channelInvalidErr(err)
	}
}

func (r *Router) forumTopicPeer(ctx context.Context, input tg.InputPeerClass) (domain.Peer, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return domain.Peer{}, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input)
	if err != nil {
		return domain.Peer{}, err
	}
	if peer.Type != domain.PeerTypeChannel {
		return domain.Peer{}, peerIDInvalidErr()
	}
	return peer, nil
}

func (r *Router) validateForumSendAs(ctx context.Context, peerInput, sendAsInput tg.InputPeerClass) error {
	_, err := r.forumSendAsPeer(ctx, peerInput, sendAsInput)
	return err
}

func (r *Router) forumSendAsPeer(ctx context.Context, peerInput, sendAsInput tg.InputPeerClass) (*domain.Peer, error) {
	peer, err := r.forumTopicPeer(ctx, peerInput)
	if err != nil {
		return nil, err
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	sendAs, err := r.checkedDomainPeerFromInputPeer(ctx, userID, sendAsInput)
	if err != nil {
		return nil, sendAsPeerInvalidErr()
	}
	if sendAs.Type == domain.PeerTypeUser && sendAs.ID == userID {
		return &sendAs, nil
	}
	if sendAs.Type == domain.PeerTypeChannel && sendAs.ID == peer.ID {
		return &sendAs, nil
	}
	return nil, sendAsPeerInvalidErr()
}

func (r *Router) onMessagesReportSpam(ctx context.Context, peer tg.InputPeerClass) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesReport(ctx context.Context, req *tg.MessagesReportRequest) (tg.ReportResultClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if len(req.ID) == 0 {
		return nil, tgerr.New(400, "MESSAGE_REQUIRED")
	}
	if len(req.ID) > maxGetMessagesIDs || len(req.Option) > maxReportOptionLength || utf8.RuneCountInString(req.Message) > maxReportCommentLength {
		return nil, limitInvalidErr()
	}
	for _, msgID := range req.ID {
		if msgID <= 0 || msgID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
	}
	return reportResultForOption(string(req.Option))
}

func reportResultForOption(option string) (tg.ReportResultClass, error) {
	switch option {
	case "":
		return &tg.ReportResultChooseOption{
			Title: "Report",
			Options: []tg.MessageReportOption{
				{Text: "Spam", Option: []byte("spam")},
				{Text: "Violence", Option: []byte("violence")},
				{Text: "Illegal goods", Option: []byte("illegal_goods")},
				{Text: "Child abuse", Option: []byte("child_abuse")},
				{Text: "Personal data", Option: []byte("personal_data")},
				{Text: "Copyright", Option: []byte("copyright")},
				{Text: "Other", Option: []byte("other")},
			},
		}, nil
	case "other":
		return &tg.ReportResultAddComment{Optional: false, Option: []byte("other:comment")}, nil
	case "spam", "violence", "illegal_goods", "child_abuse", "personal_data", "copyright", "other:comment":
		return &tg.ReportResultReported{}, nil
	default:
		return nil, tgerr.New(400, "OPTION_INVALID")
	}
}

func (r *Router) onMessagesReportReaction(ctx context.Context, req *tg.MessagesReportReactionRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return false, messageIDInvalidErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return false, err
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.ReactionPeer); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesReportMessagesDelivery(ctx context.Context, req *tg.MessagesReportMessagesDeliveryRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if len(req.ID) > maxGetMessagesIDs {
		return false, limitInvalidErr()
	}
	for _, msgID := range req.ID {
		if msgID <= 0 || msgID > domain.MaxMessageBoxID {
			return false, messageIDInvalidErr()
		}
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesReportReadMetrics(ctx context.Context, req *tg.MessagesReportReadMetricsRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if len(req.Metrics) > maxReadMetrics {
		return false, limitInvalidErr()
	}
	for _, metric := range req.Metrics {
		if metric.MsgID <= 0 || metric.MsgID > domain.MaxMessageBoxID {
			return false, messageIDInvalidErr()
		}
		if metric.TimeInViewMs < 0 || metric.ActiveTimeInViewMs < 0 || metric.HeightToViewportRatioPermille < 0 || metric.SeenRangeRatioPermille < 0 {
			return false, limitInvalidErr()
		}
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Router) onMessagesReportMusicListen(ctx context.Context, req *tg.MessagesReportMusicListenRequest) (bool, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return false, internalErr()
	}
	if req.ID == nil {
		return false, tgerr.New(400, "DOCUMENT_INVALID")
	}
	if req.ListenedDuration < 0 {
		return false, limitInvalidErr()
	}
	return true, nil
}

func (r *Router) onMessagesReportSponsoredMessage(ctx context.Context, req *tg.MessagesReportSponsoredMessageRequest) (tg.ChannelsSponsoredMessageReportResultClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	if len(req.RandomID) == 0 || len(req.RandomID) > maxReportRandomIDLength || len(req.Option) > maxReportOptionLength {
		return nil, limitInvalidErr()
	}
	return &tg.ChannelsSponsoredMessageReportResultReported{}, nil
}

func (r *Router) onMessagesGetSearchResultsCalendar(ctx context.Context, req *tg.MessagesGetSearchResultsCalendarRequest) (*tg.MessagesSearchResultsCalendar, error) {
	if req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID || req.OffsetDate < 0 {
		return nil, limitInvalidErr()
	}
	if err := r.validateSearchResultsPeer(ctx, req.Peer, req.GetSavedPeerID); err != nil {
		return nil, err
	}
	minDate := req.OffsetDate
	if minDate == 0 {
		minDate = int(r.clock.Now().Unix())
	}
	return &tg.MessagesSearchResultsCalendar{
		Count:    0,
		MinDate:  minDate,
		MinMsgID: req.OffsetID,
		Periods:  []tg.SearchResultsCalendarPeriod{},
		Messages: []tg.MessageClass{},
		Chats:    []tg.ChatClass{},
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesGetSearchResultsPositions(ctx context.Context, req *tg.MessagesGetSearchResultsPositionsRequest) (*tg.MessagesSearchResultsPositions, error) {
	if req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID || req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	if err := r.validateSearchResultsPeer(ctx, req.Peer, req.GetSavedPeerID); err != nil {
		return nil, err
	}
	return &tg.MessagesSearchResultsPositions{
		Count:     0,
		Positions: []tg.SearchResultPosition{},
	}, nil
}

func (r *Router) validateSearchResultsPeer(ctx context.Context, peer tg.InputPeerClass, savedPeer func() (tg.InputPeerClass, bool)) error {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer); err != nil {
		return err
	}
	if savedPeer == nil {
		return nil
	}
	if input, ok := savedPeer(); ok && input != nil {
		if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) onMessagesSendReaction(ctx context.Context, req *tg.MessagesSendReactionRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if reactions, ok := req.GetReaction(); ok && len(reactions) > maxReactionVector {
		return nil, limitInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	reactions, err := domainMessageReactionsFromTL(req)
	if err != nil {
		return nil, err
	}
	date := int(r.clock.Now().Unix())
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		res, err := r.deps.Channels.SetMessageReactions(ctx, userID, domain.SetChannelMessageReactionsRequest{
			UserID:      userID,
			ChannelID:   peer.ID,
			MessageID:   req.MsgID,
			Reactions:   reactions,
			Big:         req.Big,
			AddToRecent: req.GetAddToRecent(),
			Date:        date,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		updates := r.channelMessageReactionsUpdates(ctx, userID, res)
		r.pushChannelViewerUpdates(ctx, userID, res.Channel.ID, []int64{userID}, func(viewerUserID int64) *tg.Updates {
			return r.channelMessageReactionsUpdates(ctx, viewerUserID, res)
		})
		return updates, nil
	}
	if peer.Type == domain.PeerTypeUser && r.deps.Messages != nil {
		res, err := r.deps.Messages.SetMessageReactions(ctx, userID, domain.SetPrivateMessageReactionsRequest{
			UserID:      userID,
			Peer:        peer,
			MessageID:   req.MsgID,
			Reactions:   reactions,
			Big:         req.Big,
			AddToRecent: req.GetAddToRecent(),
			Date:        date,
		})
		if err != nil {
			return nil, messageReactionErr(err)
		}
		if err := r.recordMessageReactionUse(ctx, userID, reactions, req.GetAddToRecent(), date); err != nil {
			return nil, internalErr()
		}
		if err := r.recordPrivateMessageReactionEvents(ctx, userID, res); err != nil {
			return nil, internalErr()
		}
		updates := r.privateMessageReactionsUpdates(ctx, userID, peer, res)
		r.pushUserUpdates(ctx, userID, updates)
		for _, msg := range res.Messages {
			if msg.OwnerUserID == 0 || msg.OwnerUserID == userID {
				continue
			}
			viewerPeer := msg.Peer
			viewerUpdates := r.privateMessageReactionsUpdates(ctx, msg.OwnerUserID, viewerPeer, res)
			r.pushUserUpdates(ctx, msg.OwnerUserID, viewerUpdates)
		}
		return updates, nil
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}

func (r *Router) recordMessageReactionUse(ctx context.Context, userID int64, reactions []domain.MessageReaction, addToRecent bool, date int) error {
	if len(reactions) == 0 || r.deps.Channels == nil {
		return nil
	}
	recorder, ok := r.deps.Channels.(messageReactionUsageRecorder)
	if !ok {
		return nil
	}
	return recorder.RecordMessageReactionUse(ctx, userID, reactions, addToRecent, date)
}

func (r *Router) recordPrivateMessageReactionEvents(ctx context.Context, requestUserID int64, res domain.PrivateMessageReactionsResult) error {
	if r.deps.Updates == nil {
		return nil
	}
	recorder, ok := r.deps.Updates.(messageReactionUpdateRecorder)
	if !ok {
		return nil
	}
	authKeyID, _ := AuthKeyIDFrom(ctx)
	for _, msg := range res.Messages {
		if msg.OwnerUserID == 0 || msg.ID == 0 {
			continue
		}
		eventAuthKeyID := [8]byte{}
		if msg.OwnerUserID == requestUserID {
			eventAuthKeyID = authKeyID
		}
		if _, _, err := recorder.RecordMessageReactions(ctx, eventAuthKeyID, msg.OwnerUserID, msg); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) onMessagesSetDefaultReaction(ctx context.Context, reaction tg.ReactionClass) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	parsed, err := domainMessageReactionFromTL(reaction)
	if err != nil {
		return false, err
	}
	if svc, ok := r.deps.Account.(accountDefaultReactionService); ok {
		if _, err := svc.SetDefaultReaction(ctx, userID, parsed); err != nil {
			return false, internalErr()
		}
	}
	return true, nil
}

func (r *Router) onMessagesGetPaidReactionPrivacy(ctx context.Context) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	settings := domain.DefaultAccountReactionSettings()
	if svc, ok := r.deps.Account.(accountPaidReactionPrivacyService); ok {
		next, err := svc.GetReactionSettings(ctx, userID)
		if err != nil {
			return nil, internalErr()
		}
		settings = next
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePaidReactionPrivacy{
			Private: r.tgPaidReactionPrivacy(ctx, userID, settings.PaidPrivacy),
		}},
		Users: []tg.UserClass{},
		Chats: []tg.ChatClass{},
		Date:  int(r.clock.Now().Unix()),
		Seq:   0,
	}, nil
}

func (r *Router) onMessagesTogglePaidReactionPrivacy(ctx context.Context, req *tg.MessagesTogglePaidReactionPrivacyRequest) (bool, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return false, messageIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return false, err
	}
	privacy, err := r.domainPaidReactionPrivacy(ctx, userID, req.Private)
	if err != nil {
		return false, err
	}
	if svc, ok := r.deps.Account.(accountPaidReactionPrivacyService); ok {
		next, err := svc.SetPaidReactionPrivacy(ctx, userID, privacy)
		if err != nil {
			return false, internalErr()
		}
		privacy = next.PaidPrivacy
	}
	r.pushUserUpdates(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePaidReactionPrivacy{Private: r.tgPaidReactionPrivacy(ctx, userID, privacy)}},
		Users:   []tg.UserClass{},
		Chats:   []tg.ChatClass{},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	})
	return true, nil
}

func (r *Router) onMessagesSendPaidReaction(ctx context.Context, req *tg.MessagesSendPaidReactionRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if req.Count <= 0 {
		return nil, starsAmountInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if private, ok := req.GetPrivate(); ok {
		if _, err := r.domainPaidReactionPrivacy(ctx, userID, private); err != nil {
			return nil, err
		}
	}
	return nil, balanceTooLowErr()
}

func (r *Router) onMessagesDeleteParticipantReaction(ctx context.Context, req *tg.MessagesDeleteParticipantReactionRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	participant, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Participant)
	if err != nil {
		return nil, err
	}
	if participant.Type != domain.PeerTypeUser || participant.ID == 0 {
		return nil, userIDInvalidErr()
	}
	if peer.Type != domain.PeerTypeChannel || r.deps.Channels == nil {
		return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
	}
	moderator, ok := r.deps.Channels.(channelParticipantReactionModerator)
	if !ok {
		return nil, channelInvalidErr(domain.ErrChannelInvalid)
	}
	res, err := moderator.DeleteParticipantReaction(ctx, userID, domain.DeleteChannelParticipantReactionRequest{
		UserID:            userID,
		ChannelID:         peer.ID,
		MessageID:         req.MsgID,
		ParticipantUserID: participant.ID,
		Date:              int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	updates := r.channelMessageReactionsUpdates(ctx, userID, res)
	r.pushChannelViewerUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
		return r.channelMessageReactionsUpdates(ctx, viewerUserID, res)
	})
	return updates, nil
}

func (r *Router) onMessagesDeleteParticipantReactions(ctx context.Context, req *tg.MessagesDeleteParticipantReactionsRequest) (bool, error) {
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return false, err
	}
	participant, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Participant)
	if err != nil {
		return false, err
	}
	if participant.Type != domain.PeerTypeUser || participant.ID == 0 {
		return false, userIDInvalidErr()
	}
	if peer.Type != domain.PeerTypeChannel || r.deps.Channels == nil {
		return true, nil
	}
	moderator, ok := r.deps.Channels.(channelParticipantReactionModerator)
	if !ok {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	res, err := moderator.DeleteParticipantReactions(ctx, userID, domain.DeleteChannelParticipantReactionsRequest{
		UserID:            userID,
		ChannelID:         peer.ID,
		ParticipantUserID: participant.ID,
		Limit:             domain.MaxDeleteParticipantReactionsBatch,
		Date:              int(r.clock.Now().Unix()),
	})
	if err != nil {
		return false, channelInvalidErr(err)
	}
	if len(res.Messages) > 0 {
		reactionRes := domain.ChannelMessageReactionsResult{
			Channel:    res.Channel,
			Messages:   res.Messages,
			Recipients: res.Recipients,
		}
		ids := make([]int, 0, len(res.Messages))
		for _, msg := range res.Messages {
			if msg.ID > 0 {
				ids = append(ids, msg.ID)
			}
		}
		r.pushChannelViewerUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelMessagesReactionsUpdates(ctx, viewerUserID, reactionRes, ids)
		})
	}
	return true, nil
}

func (r *Router) onMessagesGetMessagesReactions(ctx context.Context, req *tg.MessagesGetMessagesReactionsRequest) (tg.UpdatesClass, error) {
	if len(req.ID) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		res, err := r.deps.Channels.GetMessageReactions(ctx, userID, domain.ChannelMessageReactionsRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			IDs:       append([]int(nil), req.ID...),
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return r.channelMessagesReactionsUpdates(ctx, userID, res, req.ID), nil
	}
	if peer.Type == domain.PeerTypeUser && r.deps.Messages != nil {
		res, err := r.deps.Messages.GetMessageReactions(ctx, userID, domain.PrivateMessageReactionsRequest{
			OwnerUserID: userID,
			Peer:        peer,
			IDs:         append([]int(nil), req.ID...),
		})
		if err != nil {
			return nil, messageReactionErr(err)
		}
		return r.privateMessagesReactionsUpdates(ctx, userID, peer, res, req.ID), nil
	}
	updates := make([]tg.UpdateClass, 0, len(req.ID))
	tgPeer := tgPeer(peer)
	for _, msgID := range req.ID {
		if msgID <= 0 || msgID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
		updates = append(updates, &tg.UpdateMessageReactions{
			Peer:  tgPeer,
			MsgID: msgID,
			Reactions: tg.MessageReactions{
				Results: []tg.ReactionCount{},
			},
		})
	}
	return &tg.Updates{
		Updates: updates,
		Users:   []tg.UserClass{},
		Chats:   []tg.ChatClass{},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}, nil
}

func (r *Router) onMessagesGetMessageReactionsList(ctx context.Context, req *tg.MessagesGetMessageReactionsListRequest) (*tg.MessagesMessageReactionsList, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	if offset, ok := req.GetOffset(); ok && len(offset) > maxReactionListOffset {
		return nil, limitInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		filter, err := optionalDomainMessageReaction(req.Reaction)
		if err != nil {
			return nil, err
		}
		res, err := r.deps.Channels.ListMessageReactions(ctx, userID, domain.ChannelMessageReactionsListRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			MessageID: req.ID,
			Reaction:  filter,
			Offset:    optionalString(req.GetOffset),
			Limit:     req.Limit,
		})
		if errors.Is(err, domain.ErrChannelRightForbidden) {
			return nil, tgerr.New(403, "BROADCAST_FORBIDDEN")
		}
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		userIDs := make([]int64, 0, len(res.Reactions))
		reactions := make([]tg.MessagePeerReaction, 0, len(res.Reactions))
		for _, item := range res.Reactions {
			if item.UserID != 0 {
				userIDs = append(userIDs, item.UserID)
			}
			if converted := tgMessagePeerReaction(userID, item); converted != nil {
				reactions = append(reactions, *converted)
			}
		}
		out := &tg.MessagesMessageReactionsList{
			Count:     res.Count,
			Reactions: reactions,
			Chats:     tgChannels(userID, []domain.Channel{res.Channel}),
			Users:     r.tgUsersForIDs(ctx, userID, userIDs),
		}
		if res.NextOffset != "" {
			out.SetNextOffset(res.NextOffset)
		}
		return out, nil
	}
	if peer.Type == domain.PeerTypeUser && r.deps.Messages != nil {
		filter, err := optionalDomainMessageReaction(req.Reaction)
		if err != nil {
			return nil, err
		}
		res, err := r.deps.Messages.GetMessageReactions(ctx, userID, domain.PrivateMessageReactionsRequest{
			OwnerUserID: userID,
			Peer:        peer,
			IDs:         []int{req.ID},
		})
		if err != nil {
			return nil, messageReactionErr(err)
		}
		var source domain.ChannelMessageReactions
		if len(res.Messages) > 0 && res.Messages[0].Reactions != nil {
			source = *res.Messages[0].Reactions
		} else {
			source = res.Reactions
		}
		limit := req.Limit
		if limit <= 0 || limit > len(source.Recent) {
			limit = len(source.Recent)
		}
		userIDs := []int64{userID, peer.ID}
		reactions := make([]tg.MessagePeerReaction, 0, limit)
		count := 0
		for _, item := range source.Recent {
			if filter != nil && (item.Reaction.Type != filter.Type || item.Reaction.Emoticon != filter.Emoticon) {
				continue
			}
			count++
			if len(reactions) >= limit {
				continue
			}
			if item.UserID != 0 {
				userIDs = append(userIDs, item.UserID)
			}
			if converted := tgMessagePeerReaction(userID, item); converted != nil {
				reactions = append(reactions, *converted)
			}
		}
		return &tg.MessagesMessageReactionsList{
			Count:     count,
			Reactions: reactions,
			Chats:     r.chatsForInputPeer(ctx, userID, req.Peer),
			Users:     r.tgUsersForIDs(ctx, userID, userIDs),
		}, nil
	}
	return &tg.MessagesMessageReactionsList{
		Count:     0,
		Reactions: []tg.MessagePeerReaction{},
		Chats:     r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:     []tg.UserClass{},
	}, nil
}

func (r *Router) channelMessageReactionsUpdates(ctx context.Context, viewerUserID int64, res domain.ChannelMessageReactionsResult) *tg.Updates {
	ids := []int{res.Message.ID}
	if res.Message.ID <= 0 && len(res.Messages) > 0 {
		ids = []int{res.Messages[0].ID}
	}
	return r.channelMessagesReactionsUpdates(ctx, viewerUserID, res, ids)
}

func (r *Router) privateMessageReactionsUpdates(ctx context.Context, viewerUserID int64, peer domain.Peer, res domain.PrivateMessageReactionsResult) *tg.Updates {
	ids := make([]int, 0, 1)
	for _, msg := range res.Messages {
		if msg.OwnerUserID == viewerUserID && msg.ID > 0 {
			ids = append(ids, msg.ID)
			break
		}
	}
	return r.privateMessagesReactionsUpdates(ctx, viewerUserID, peer, res, ids)
}

func (r *Router) privateMessagesReactionsUpdates(ctx context.Context, viewerUserID int64, peer domain.Peer, res domain.PrivateMessageReactionsResult, ids []int) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, len(ids))
	messagesByID := make(map[int]domain.Message, len(res.Messages))
	userIDs := []int64{viewerUserID}
	if peer.Type == domain.PeerTypeUser && peer.ID != 0 {
		userIDs = append(userIDs, peer.ID)
	}
	for _, msg := range res.Messages {
		if msg.OwnerUserID != viewerUserID || msg.ID == 0 {
			continue
		}
		messagesByID[msg.ID] = msg
		if msg.Peer.Type == domain.PeerTypeUser && msg.Peer.ID != 0 {
			userIDs = append(userIDs, msg.Peer.ID)
		}
		if msg.From.Type == domain.PeerTypeUser && msg.From.ID != 0 {
			userIDs = append(userIDs, msg.From.ID)
		}
		if msg.Reactions != nil {
			userIDs = append(userIDs, channelMessageReactionUserIDs(*msg.Reactions)...)
		}
	}
	userIDs = append(userIDs, channelMessageReactionUserIDs(res.Reactions)...)
	fallbackPeer := tgPeer(peer)
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		msg, ok := messagesByID[id]
		outPeer := fallbackPeer
		reactions := domain.ChannelMessageReactions{
			CanSeeList: true,
			Results:    []domain.ChannelMessageReactionCount{},
			Recent:     []domain.ChannelMessagePeerReaction{},
		}
		if ok {
			outPeer = tgPeer(msg.Peer)
			if msg.Reactions != nil {
				reactions = *msg.Reactions
			}
		}
		if outPeer == nil {
			continue
		}
		converted := tgMessageReactions(viewerUserID, &reactions)
		if converted == nil {
			converted = &tg.MessageReactions{Results: []tg.ReactionCount{}}
		}
		updates = append(updates, &tg.UpdateMessageReactions{
			Peer:      outPeer,
			MsgID:     id,
			Reactions: *converted,
		})
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, userIDs),
		Chats:   []tg.ChatClass{},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) channelMessagesReactionsUpdates(ctx context.Context, viewerUserID int64, res domain.ChannelMessageReactionsResult, ids []int) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, len(ids))
	messagesByID := make(map[int]domain.ChannelMessage, len(res.Messages)+1)
	if res.Message.ID != 0 {
		messagesByID[res.Message.ID] = res.Message
	}
	for _, msg := range res.Messages {
		if msg.ID != 0 {
			messagesByID[msg.ID] = msg
		}
	}
	userIDs := make([]int64, 0)
	for _, msg := range messagesByID {
		if msg.Reactions != nil {
			userIDs = append(userIDs, channelMessageReactionUserIDs(*msg.Reactions)...)
		}
	}
	userIDs = append(userIDs, channelMessageReactionUserIDs(res.Reactions)...)
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		msg, ok := messagesByID[id]
		reactions := domain.ChannelMessageReactions{
			CanSeeList: !res.Channel.Broadcast || res.Channel.Megagroup,
			Results:    []domain.ChannelMessageReactionCount{},
			Recent:     []domain.ChannelMessagePeerReaction{},
		}
		if ok && msg.Reactions != nil {
			reactions = *msg.Reactions
		} else if ok && len(res.Reactions.Results) > 0 && res.Message.ID == id {
			reactions = res.Reactions
		}
		converted := tgMessageReactions(viewerUserID, &reactions)
		if converted == nil {
			converted = &tg.MessageReactions{Results: []tg.ReactionCount{}}
		}
		update := &tg.UpdateMessageReactions{
			Peer:      &tg.PeerChannel{ChannelID: res.Channel.ID},
			MsgID:     id,
			Reactions: *converted,
		}
		if ok {
			if topID := channelMessageThreadRootID(msg); topID > 0 && topID != id {
				update.SetTopMsgID(topID)
			}
		}
		updates = append(updates, update)
	}
	return &tg.Updates{
		Updates: updates,
		Users:   r.tgUsersForIDs(ctx, viewerUserID, userIDs),
		Chats:   tgChannels(viewerUserID, []domain.Channel{res.Channel}),
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func channelMessageReactionUserIDs(reactions domain.ChannelMessageReactions) []int64 {
	out := make([]int64, 0, len(reactions.Recent))
	for _, item := range reactions.Recent {
		if item.UserID != 0 {
			out = append(out, item.UserID)
		}
	}
	return out
}

func (r *Router) onMessagesGetUnreadReactions(ctx context.Context, req *tg.MessagesGetUnreadReactionsRequest) (tg.MessagesMessagesClass, error) {
	if err := validateHistoryBounds(req.OffsetID, req.AddOffset, req.Limit, req.MaxID, req.MinID); err != nil {
		return nil, err
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, req.GetSavedPeerID)
	if err != nil {
		return nil, err
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, messageIDInvalidErr()
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		history, err := r.deps.Channels.GetUnreadReactions(ctx, userID, domain.ChannelUnreadReactionsFilter{
			ChannelID: peer.ID,
			TopMsgID:  req.TopMsgID,
			OffsetID:  req.OffsetID,
			AddOffset: req.AddOffset,
			Limit:     req.Limit,
			MaxID:     req.MaxID,
			MinID:     req.MinID,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return tgChannelHistoryMessages(userID, r.enrichChannelHistory(ctx, userID, history)), nil
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Topics:   []tg.ForumTopicClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesReadReactions(ctx context.Context, req *tg.MessagesReadReactionsRequest) (*tg.MessagesAffectedHistory, error) {
	authKeyID, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if savedPeer, ok := req.GetSavedPeerID(); ok && savedPeer != nil {
		if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, savedPeer); err != nil {
			return nil, err
		}
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, messageIDInvalidErr()
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		res, err := r.deps.Channels.ReadReactions(ctx, userID, domain.ReadChannelReactionsRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			TopMsgID:  req.TopMsgID,
			Limit:     domain.MaxChannelReadReactionsBatch,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return &tg.MessagesAffectedHistory{Pts: res.ChannelPts, PtsCount: 0, Offset: res.Offset}, nil
	}
	return r.affectedHistory(ctx, authKeyID, userID, 0)
}

func (r *Router) onMessagesSendVote(ctx context.Context, req *tg.MessagesSendVoteRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := validatePollOptions(req.Options, true); err != nil {
		return nil, err
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) onMessagesGetPollResults(ctx context.Context, req *tg.MessagesGetPollResultsRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}

func (r *Router) onMessagesGetPollVotes(ctx context.Context, req *tg.MessagesGetPollVotesRequest) (*tg.MessagesVotesList, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if req.Limit < 0 || req.Limit > maxSearchResultsLimit {
		return nil, limitInvalidErr()
	}
	if option, ok := req.GetOption(); ok {
		if err := validatePollOption(option); err != nil {
			return nil, err
		}
	}
	if offset, ok := req.GetOffset(); ok && len(offset) > maxPollVotesOffsetLength {
		return nil, limitInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		view, err := r.deps.Channels.GetChannel(ctx, userID, peer.ID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if view.Channel.Broadcast && !view.Channel.Megagroup {
			return nil, tgerr.New(403, "BROADCAST_FORBIDDEN")
		}
	}
	return &tg.MessagesVotesList{
		Count: 0,
		Votes: []tg.MessagePeerVoteClass{},
		Chats: r.chatsForInputPeer(ctx, userID, req.Peer),
		Users: []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesAddPollAnswer(ctx context.Context, req *tg.MessagesAddPollAnswerRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := validatePollAnswer(req.Answer); err != nil {
		return nil, err
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) onMessagesDeletePollAnswer(ctx context.Context, req *tg.MessagesDeletePollAnswerRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := validatePollOption(req.Option); err != nil {
		return nil, err
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) onMessagesGetUnreadPollVotes(ctx context.Context, req *tg.MessagesGetUnreadPollVotesRequest) (tg.MessagesMessagesClass, error) {
	if err := validateHistoryBounds(req.OffsetID, req.AddOffset, req.Limit, req.MaxID, req.MinID); err != nil {
		return nil, err
	}
	userID, _, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, messageIDInvalidErr()
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Topics:   []tg.ForumTopicClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesReadPollVotes(ctx context.Context, req *tg.MessagesReadPollVotesRequest) (*tg.MessagesAffectedHistory, error) {
	authKeyID, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if topMsgID, ok := req.GetTopMsgID(); ok && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, messageIDInvalidErr()
	}
	return r.affectedHistory(ctx, authKeyID, userID, 0)
}

func (r *Router) onMessagesAppendTodoList(ctx context.Context, req *tg.MessagesAppendTodoListRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if len(req.List) == 0 {
		return nil, todoNotModifiedErr()
	}
	if err := validateTodoItems(req.List); err != nil {
		return nil, err
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) onMessagesToggleTodoCompleted(ctx context.Context, req *tg.MessagesToggleTodoCompletedRequest) (tg.UpdatesClass, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if len(req.Completed) == 0 && len(req.Incompleted) == 0 {
		return nil, todoNotModifiedErr()
	}
	if err := validateTodoIDVector(req.Completed, req.Incompleted); err != nil {
		return nil, err
	}
	if _, _, err := r.reactionPeer(ctx, req.Peer, nil); err != nil {
		return nil, err
	}
	return nil, messageIDInvalidErr()
}

func (r *Router) reactionPeer(ctx context.Context, peer tg.InputPeerClass, savedPeer func() (tg.InputPeerClass, bool)) (int64, domain.Peer, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return 0, domain.Peer{}, internalErr()
	}
	out, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer)
	if err != nil {
		return 0, domain.Peer{}, err
	}
	if savedPeer != nil {
		if input, ok := savedPeer(); ok && input != nil {
			if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input); err != nil {
				return 0, domain.Peer{}, err
			}
		}
	}
	return userID, out, nil
}

func (r *Router) domainPaidReactionPrivacy(ctx context.Context, userID int64, in tg.PaidReactionPrivacyClass) (domain.PaidReactionPrivacy, error) {
	switch typed := in.(type) {
	case nil, *tg.PaidReactionPrivacyDefault:
		return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyDefault}, nil
	case *tg.PaidReactionPrivacyAnonymous:
		return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyAnonymous}, nil
	case *tg.PaidReactionPrivacyPeer:
		peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, typed.Peer)
		if err != nil {
			return domain.PaidReactionPrivacy{}, err
		}
		return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyPeer, Peer: &peer}, nil
	default:
		return domain.PaidReactionPrivacy{}, inputConstructorInvalidErr()
	}
}

func (r *Router) tgPaidReactionPrivacy(ctx context.Context, userID int64, in domain.PaidReactionPrivacy) tg.PaidReactionPrivacyClass {
	switch in.Kind {
	case domain.PaidReactionPrivacyAnonymous:
		return &tg.PaidReactionPrivacyAnonymous{}
	case domain.PaidReactionPrivacyPeer:
		if in.Peer == nil {
			return &tg.PaidReactionPrivacyDefault{}
		}
		if peer := r.inputPeerForDomainPeer(ctx, userID, *in.Peer); peer != nil {
			return &tg.PaidReactionPrivacyPeer{Peer: peer}
		}
	}
	return &tg.PaidReactionPrivacyDefault{}
}

func (r *Router) inputPeerForDomainPeer(ctx context.Context, currentUserID int64, peer domain.Peer) tg.InputPeerClass {
	switch peer.Type {
	case domain.PeerTypeUser:
		switch {
		case peer.ID == domain.OfficialSystemUserID:
			u := domain.OfficialSystemUser()
			return &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
		case r.deps.Users == nil:
			return nil
		case peer.ID == currentUserID:
			u, err := r.deps.Users.Self(ctx, currentUserID)
			if err != nil || u.ID == 0 {
				return nil
			}
			return &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
		default:
			u, found, err := r.deps.Users.ByID(ctx, currentUserID, peer.ID)
			if err != nil || !found {
				return nil
			}
			return &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
		}
	case domain.PeerTypeChannel:
		if r.deps.Channels == nil || peer.ID == 0 {
			return nil
		}
		view, err := r.deps.Channels.GetChannel(ctx, currentUserID, peer.ID)
		if err != nil || view.Channel.ID == 0 {
			return nil
		}
		return &tg.InputPeerChannel{ChannelID: view.Channel.ID, AccessHash: view.Channel.AccessHash}
	default:
		return nil
	}
}

func domainMessageReactionsFromTL(req *tg.MessagesSendReactionRequest) ([]domain.MessageReaction, error) {
	if req == nil {
		return nil, nil
	}
	reactions, ok := req.GetReaction()
	if !ok || len(reactions) == 0 {
		return nil, nil
	}
	out := make([]domain.MessageReaction, 0, len(reactions))
	seen := make(map[string]struct{}, len(reactions))
	for _, reaction := range reactions {
		parsed, err := domainMessageReactionFromTL(reaction)
		if err != nil {
			return nil, err
		}
		key := string(parsed.Type) + "\x00" + parsed.Emoticon
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, parsed)
	}
	return out, nil
}

func optionalDomainMessageReaction(reaction tg.ReactionClass) (*domain.MessageReaction, error) {
	if reaction == nil {
		return nil, nil
	}
	out, err := domainMessageReactionFromTL(reaction)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func domainMessageReactionFromTL(reaction tg.ReactionClass) (domain.MessageReaction, error) {
	switch typed := reaction.(type) {
	case *tg.ReactionEmoji:
		emoticon := strings.TrimSpace(typed.Emoticon)
		if emoticon == "" || utf8.RuneCountInString(emoticon) > domain.MaxChannelReactionEmoticonLength {
			return domain.MessageReaction{}, reactionInvalidErr()
		}
		return domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: emoticon}, nil
	case nil, *tg.ReactionEmpty, *tg.ReactionCustomEmoji, *tg.ReactionPaid:
		return domain.MessageReaction{}, reactionInvalidErr()
	default:
		return domain.MessageReaction{}, inputConstructorInvalidErr()
	}
}

func optionalString(get func() (string, bool)) string {
	if get == nil {
		return ""
	}
	value, ok := get()
	if !ok {
		return ""
	}
	return value
}

func validateEmojiLangCode(langcode string) error {
	if langcode == "" || len(langcode) > maxEmojiLangCodeLength {
		return limitInvalidErr()
	}
	for _, c := range langcode {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_':
		default:
			return limitInvalidErr()
		}
	}
	return nil
}

func messagesReactionsEmpty(hash int64) tg.MessagesReactionsClass {
	if hash != 0 {
		return &tg.MessagesReactionsNotModified{}
	}
	return &tg.MessagesReactions{
		Hash:      0,
		Reactions: []tg.ReactionClass{},
	}
}

func messagesReactionsFromDomain(reactions []domain.MessageReaction, requestHash int64) tg.MessagesReactionsClass {
	hash := messageReactionListHash(reactions)
	if hash != 0 && requestHash == hash {
		return &tg.MessagesReactionsNotModified{}
	}
	out := make([]tg.ReactionClass, 0, len(reactions))
	for _, reaction := range reactions {
		tgReaction := tgMessageReaction(reaction)
		if tgReaction != nil {
			out = append(out, tgReaction)
		}
	}
	return &tg.MessagesReactions{
		Hash:      hash,
		Reactions: out,
	}
}

func savedReactionTagsEmpty(_ int64) tg.MessagesSavedReactionTagsClass {
	return &tg.MessagesSavedReactionTags{
		Tags: []tg.SavedReactionTag{},
		Hash: 0,
	}
}

func savedReactionTagsFromDomain(tags []domain.SavedReactionTag, requestHash int64) tg.MessagesSavedReactionTagsClass {
	hash := savedReactionTagListHash(tags)
	if hash != 0 && requestHash == hash {
		return &tg.MessagesSavedReactionTagsNotModified{}
	}
	out := make([]tg.SavedReactionTag, 0, len(tags))
	for _, tag := range tags {
		reaction := tgMessageReaction(tag.Reaction)
		if reaction == nil {
			continue
		}
		item := tg.SavedReactionTag{
			Reaction: reaction,
			Count:    tag.Count,
		}
		if tag.Title != "" {
			item.SetTitle(tag.Title)
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return savedReactionTagsEmpty(requestHash)
	}
	return &tg.MessagesSavedReactionTags{
		Tags: out,
		Hash: hash,
	}
}

func (r *Router) reactionsWithCatalogFallback(ctx context.Context, reactions []domain.MessageReaction, limit int) []domain.MessageReaction {
	return mergeReactionCatalogFallback(reactions, r.availableReactionCatalog(ctx, limit), limit)
}

func reactionsWithCatalogFallback(reactions []domain.MessageReaction, limit int) []domain.MessageReaction {
	return mergeReactionCatalogFallback(reactions, staticReactionCatalog(), limit)
}

func (r *Router) availableReactionCatalog(ctx context.Context, limit int) []domain.MessageReaction {
	if limit <= 0 {
		return nil
	}
	if r.deps.Files != nil {
		catalog, err := r.deps.Files.ListAvailableReactions(ctx)
		if err == nil {
			out := make([]domain.MessageReaction, 0, min(limit, len(catalog)))
			for _, item := range catalog {
				emoticon := strings.TrimSpace(item.Reaction)
				if item.Inactive || emoticon == "" {
					continue
				}
				out = append(out, domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: emoticon})
				if len(out) >= limit {
					return out
				}
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	return staticReactionCatalog()
}

func staticReactionCatalog() []domain.MessageReaction {
	emoticons := tdesktop.DefaultReactionEmoticons()
	out := make([]domain.MessageReaction, 0, len(emoticons))
	for _, emoticon := range emoticons {
		out = append(out, domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: emoticon})
	}
	return out
}

func mergeReactionCatalogFallback(reactions, fallback []domain.MessageReaction, limit int) []domain.MessageReaction {
	if limit <= 0 {
		return []domain.MessageReaction{}
	}
	if limit > domain.MaxTopMessageReactions {
		limit = domain.MaxTopMessageReactions
	}
	out := make([]domain.MessageReaction, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, reaction := range reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" {
			continue
		}
		key := string(reaction.Type) + "\x00" + reaction.Emoticon
		if _, ok := seen[key]; ok {
			continue
		}
		out = append(out, reaction)
		seen[key] = struct{}{}
		if len(out) >= limit {
			return out
		}
	}
	for _, reaction := range fallback {
		key := string(reaction.Type) + "\x00" + reaction.Emoticon
		if _, ok := seen[key]; ok {
			continue
		}
		out = append(out, reaction)
		seen[key] = struct{}{}
		if len(out) >= limit {
			return out
		}
	}
	return out
}

func messageReactionListHash(reactions []domain.MessageReaction) int64 {
	if len(reactions) == 0 {
		return 0
	}
	h := fnv.New64a()
	for _, reaction := range reactions {
		_, _ = h.Write([]byte(reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(reaction.Emoticon))
		_, _ = h.Write([]byte{0xff})
	}
	sum := int64(h.Sum64() & 0x7fffffffffffffff)
	if sum == 0 {
		return 1
	}
	return sum
}

func savedReactionTagListHash(tags []domain.SavedReactionTag) int64 {
	if len(tags) == 0 {
		return 0
	}
	h := fnv.New64a()
	for _, tag := range tags {
		_, _ = h.Write([]byte(tag.Reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(tag.Reaction.Emoticon))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(tag.Title))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.Itoa(tag.Count)))
		_, _ = h.Write([]byte{0xff})
	}
	sum := int64(h.Sum64() & 0x7fffffffffffffff)
	if sum == 0 {
		return 1
	}
	return sum
}

func validateReactionClass(reaction tg.ReactionClass) error {
	switch typed := reaction.(type) {
	case *tg.ReactionEmoji:
		if strings.TrimSpace(typed.Emoticon) == "" || utf8.RuneCountInString(typed.Emoticon) > maxReportOptionLength {
			return reactionInvalidErr()
		}
	case *tg.ReactionCustomEmoji:
		if typed.DocumentID <= 0 {
			return reactionInvalidErr()
		}
	case *tg.ReactionPaid:
	case nil, *tg.ReactionEmpty:
		return reactionInvalidErr()
	default:
		return inputConstructorInvalidErr()
	}
	return nil
}

func validatePollOptions(options [][]byte, allowEmpty bool) error {
	if len(options) == 0 {
		if allowEmpty {
			return nil
		}
		return optionInvalidErr()
	}
	if len(options) > maxPollVoteOptions {
		return optionsTooMuchErr()
	}
	for _, option := range options {
		if err := validatePollOption(option); err != nil {
			return err
		}
	}
	return nil
}

func validatePollOption(option []byte) error {
	if len(option) == 0 || len(option) > maxPollOptionBytes {
		return optionInvalidErr()
	}
	return nil
}

func validatePollAnswer(answer tg.PollAnswerClass) error {
	if answer == nil {
		return pollAnswerInvalidErr()
	}
	text := answer.GetText()
	if strings.TrimSpace(text.Text) == "" || utf8.RuneCountInString(text.Text) > maxTodoTitleLength {
		return pollAnswerInvalidErr()
	}
	if len(text.Entities) > maxMessageEntityCount {
		return limitInvalidErr()
	}
	switch typed := answer.(type) {
	case *tg.PollAnswer:
		if err := validatePollOption(typed.Option); err != nil {
			return err
		}
		if typed.Media != nil {
			return mediaInvalidErr()
		}
	case *tg.InputPollAnswer:
		if typed.Media != nil {
			return mediaInvalidErr()
		}
	default:
		return inputConstructorInvalidErr()
	}
	return nil
}

func validateTodoItems(items []tg.TodoItem) error {
	if len(items) == 0 {
		return todoItemsEmptyErr()
	}
	if len(items) > maxTodoItems {
		return limitInvalidErr()
	}
	seen := make(map[int]struct{}, len(items))
	for _, item := range items {
		if item.ID < 0 || item.ID > maxTodoItems {
			return messageIDInvalidErr()
		}
		if item.ID != 0 {
			if _, ok := seen[item.ID]; ok {
				return tgerr.New(400, "TODO_ITEM_DUPLICATE")
			}
			seen[item.ID] = struct{}{}
		}
		if strings.TrimSpace(item.Title.Text) == "" || utf8.RuneCountInString(item.Title.Text) > maxTodoTitleLength {
			return limitInvalidErr()
		}
		if len(item.Title.Entities) > maxMessageEntityCount {
			return limitInvalidErr()
		}
	}
	return nil
}

func validateTodoIDVector(vectors ...[]int) error {
	total := 0
	seen := map[int]struct{}{}
	for _, ids := range vectors {
		total += len(ids)
		if total > maxTodoItems {
			return limitInvalidErr()
		}
		for _, id := range ids {
			if id <= 0 || id > maxTodoItems {
				return messageIDInvalidErr()
			}
			if _, ok := seen[id]; ok {
				return todoNotModifiedErr()
			}
			seen[id] = struct{}{}
		}
	}
	return nil
}

func validateHistoryBounds(offsetID, addOffset, limit, maxID, minID int) error {
	if offsetID < 0 || offsetID > domain.MaxMessageBoxID || maxID < 0 || maxID > domain.MaxMessageBoxID || minID < 0 || minID > domain.MaxMessageBoxID {
		return messageIDInvalidErr()
	}
	if addOffset < -100 || addOffset > 100 || limit < 0 || limit > maxSearchResultsLimit {
		return limitInvalidErr()
	}
	return nil
}

func validateSavedHistoryBounds(offsetID, offsetDate, addOffset, limit, maxID, minID int) error {
	if err := validateHistoryBounds(offsetID, addOffset, limit, maxID, minID); err != nil {
		return err
	}
	if offsetDate < 0 {
		return limitInvalidErr()
	}
	return nil
}

func (r *Router) validateSavedHistoryParentPeer(ctx context.Context, userID int64, getParent func() (tg.InputPeerClass, bool)) (domain.Peer, bool, error) {
	parentPeer, ok := getParent()
	if !ok {
		return domain.Peer{}, false, nil
	}
	if err := r.validateRequiredSavedHistoryParentPeer(ctx, userID, parentPeer); err != nil {
		return domain.Peer{}, false, err
	}
	parent, _ := r.domainPeerFromInputPeer(userID, parentPeer)
	return parent, true, nil
}

func (r *Router) validateRequiredSavedHistoryParentPeer(ctx context.Context, userID int64, parentPeer tg.InputPeerClass) error {
	parent, err := r.checkedDomainPeerFromInputPeer(ctx, userID, parentPeer)
	if err != nil || parent.Type != domain.PeerTypeChannel {
		return parentPeerInvalidErr()
	}
	return nil
}

func (r *Router) savedHistoryChats(ctx context.Context, userID int64, hasParent bool, parent domain.Peer, peer tg.InputPeerClass) []tg.ChatClass {
	if r.deps.Channels == nil {
		return []tg.ChatClass{}
	}
	seen := make(map[int64]struct{}, 2)
	out := make([]tg.ChatClass, 0, 2)
	add := func(channelID int64) {
		if channelID == 0 {
			return
		}
		if _, ok := seen[channelID]; ok {
			return
		}
		seen[channelID] = struct{}{}
		view, err := r.deps.Channels.GetChannel(ctx, userID, channelID)
		if err != nil || view.Channel.ID == 0 {
			return
		}
		out = append(out, tgChannelChat(userID, view.Channel, &view.Self))
	}
	if hasParent && parent.Type == domain.PeerTypeChannel {
		add(parent.ID)
	}
	if p, ok := r.domainPeerFromInputPeer(userID, peer); ok && p.Type == domain.PeerTypeChannel {
		add(p.ID)
	}
	return out
}

func (r *Router) onMessagesGetDefaultHistoryTTL(ctx context.Context) (*tg.DefaultHistoryTTL, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	return &tg.DefaultHistoryTTL{Period: 0}, nil
}

func (r *Router) onMessagesGetSponsoredMessages(ctx context.Context, req *tg.MessagesGetSponsoredMessagesRequest) (tg.MessagesSponsoredMessagesClass, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	return &tg.MessagesSponsoredMessagesEmpty{}, nil
}

func (r *Router) onMessagesReadMessageContents(ctx context.Context, ids []int) (*tg.MessagesAffectedMessages, error) {
	id, _ := AuthKeyIDFrom(ctx)
	sessionID, _ := SessionIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(ids) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	for _, msgID := range ids {
		if msgID <= 0 || msgID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
	}
	read := domain.ReadMessageContentsResult{OwnerUserID: userID}
	if r.deps.Messages != nil {
		read, err = r.deps.Messages.ReadMessageContents(ctx, userID, domain.ReadMessageContentsRequest{
			OwnerUserID:     userID,
			IDs:             ids,
			Date:            int(r.clock.Now().Unix()),
			OriginAuthKeyID: id,
			OriginSessionID: sessionID,
		})
		if err != nil {
			if errors.Is(err, domain.ErrMessageIDInvalid) {
				return nil, messageIDInvalidErr()
			}
			return nil, internalErr()
		}
	}
	affected := &tg.MessagesAffectedMessages{Pts: read.Event.Pts, PtsCount: read.Event.PtsCount}
	if read.Event.Pts == 0 {
		affected, err = r.affectedMessages(ctx, id, userID)
		if err != nil {
			return nil, err
		}
	}
	if contentIDs := readMessageContentIDs(read.MessageIDs); len(contentIDs) > 0 {
		r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateReadMessagesContents{
				Messages: contentIDs,
				Pts:      affected.Pts,
				PtsCount: affected.PtsCount,
			}},
			Date: int(r.clock.Now().Unix()),
			Seq:  0,
		})
	}
	return affected, nil
}

func readMessageContentIDs(ids []int) []int {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int, 0, len(ids))
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
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

func (r *Router) onMessagesGetMessagesViews(ctx context.Context, req *tg.MessagesGetMessagesViewsRequest) (*tg.MessagesMessageViews, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(req.ID) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	for _, msgID := range req.ID {
		if msgID <= 0 || msgID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
	}
	views := make([]tg.MessageViews, len(req.ID))
	peer, peerErr := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if peerErr != nil {
		return nil, peerErr
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil && len(req.ID) > 0 {
		history, err := r.deps.Channels.GetMessages(ctx, userID, peer.ID, req.ID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		viewCounters, err := r.deps.Channels.GetMessageViews(ctx, userID, domain.ChannelMessageViewsRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			IDs:       req.ID,
			Increment: req.Increment,
			Date:      int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		byID := make(map[int]domain.ChannelMessage, len(history.Messages))
		for _, msg := range history.Messages {
			byID[msg.ID] = msg
		}
		for i, id := range req.ID {
			if count, ok := viewCounters.Views[id]; ok {
				views[i].SetViews(count)
			}
			if replies := tgChannelMessageReplies(byID[id].Replies); replies != nil {
				views[i].SetReplies(*replies)
			}
		}
		channels := make([]domain.Channel, 0, 1+len(history.Channels))
		channels = append(channels, history.Channel)
		channels = append(channels, history.Channels...)
		return &tg.MessagesMessageViews{
			Views: views,
			Chats: tgChannels(userID, channels),
			Users: r.tgUsers(history.Users),
		}, nil
	}
	return &tg.MessagesMessageViews{
		Views: views,
		Chats: r.chatsForInputPeer(ctx, userID, req.Peer),
		Users: []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesGetUnreadMentions(ctx context.Context, req *tg.MessagesGetUnreadMentionsRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.Limit < 0 || req.Limit > domain.MaxChannelUnreadMentionsLimit {
		return nil, limitInvalidErr()
	}
	if req.TopMsgID < 0 || req.TopMsgID > domain.MaxMessageBoxID ||
		req.OffsetID < 0 || req.OffsetID > domain.MaxMessageBoxID ||
		req.MaxID < 0 || req.MaxID > domain.MaxMessageBoxID ||
		req.MinID < 0 || req.MinID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		history, err := r.deps.Channels.GetUnreadMentions(ctx, userID, domain.ChannelUnreadMentionsFilter{
			ChannelID: peer.ID,
			TopMsgID:  req.TopMsgID,
			OffsetID:  req.OffsetID,
			AddOffset: req.AddOffset,
			Limit:     req.Limit,
			MaxID:     req.MaxID,
			MinID:     req.MinID,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return tgChannelHistoryMessages(userID, r.enrichChannelHistory(ctx, userID, history)), nil
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesReadMentions(ctx context.Context, req *tg.MessagesReadMentionsRequest) (*tg.MessagesAffectedHistory, error) {
	id, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.TopMsgID < 0 || req.TopMsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		res, err := r.deps.Channels.ReadMentions(ctx, userID, domain.ReadChannelMentionsRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			TopMsgID:  req.TopMsgID,
			Limit:     domain.MaxChannelReadMentionsBatch,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		return &tg.MessagesAffectedHistory{Pts: res.ChannelPts, PtsCount: 0, Offset: res.Offset}, nil
	}
	return r.affectedHistory(ctx, id, userID, 0)
}

func (r *Router) onMessagesGetSearchCounters(ctx context.Context, req *tg.MessagesGetSearchCountersRequest) ([]tg.MessagesSearchCounter, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(req.Filters) > maxMessageSearchFilters {
		return nil, limitInvalidErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	counters := make([]tg.MessagesSearchCounter, 0, len(req.Filters))
	for _, filter := range req.Filters {
		if filter == nil {
			continue
		}
		counters = append(counters, tg.MessagesSearchCounter{Filter: filter, Count: 0})
	}
	return counters, nil
}

func (r *Router) onMessagesGetReplies(ctx context.Context, req *tg.MessagesGetRepliesRequest) (tg.MessagesMessagesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := validateHistoryBounds(req.OffsetID, req.AddOffset, req.Limit, req.MaxID, req.MinID); err != nil {
		return nil, err
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel && r.deps.Channels != nil {
		replies, err := r.deps.Channels.GetReplies(ctx, userID, domain.ChannelRepliesFilter{
			ChannelID:     peer.ID,
			RootMessageID: req.MsgID,
			OffsetID:      req.OffsetID,
			OffsetDate:    req.OffsetDate,
			AddOffset:     req.AddOffset,
			Limit:         req.Limit,
			MaxID:         req.MaxID,
			MinID:         req.MinID,
			Hash:          req.Hash,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if req.Hash != 0 && replies.Hash == req.Hash {
			return &tg.MessagesMessagesNotModified{Count: replies.Count}, nil
		}
		return tgChannelHistoryMessages(userID, r.enrichChannelHistory(ctx, userID, replies)), nil
	}
	return &tg.MessagesMessages{
		Messages: []tg.MessageClass{},
		Chats:    r.chatsForInputPeer(ctx, userID, req.Peer),
		Users:    []tg.UserClass{},
	}, nil
}

func (r *Router) onMessagesGetDiscussionMessage(ctx context.Context, req *tg.MessagesGetDiscussionMessageRequest) (*tg.MessagesDiscussionMessage, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel {
		return nil, peerIDInvalidErr()
	}
	if r.deps.Channels == nil {
		return &tg.MessagesDiscussionMessage{Chats: r.chatsForInputPeer(ctx, userID, req.Peer)}, nil
	}
	discussion, err := r.deps.Channels.GetDiscussionMessage(ctx, userID, peer.ID, req.MsgID)
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	return tgMessagesDiscussionMessage(userID, discussion), nil
}

func (r *Router) onMessagesReadDiscussion(ctx context.Context, req *tg.MessagesReadDiscussionRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID || req.ReadMaxID < 0 || req.ReadMaxID > domain.MaxMessageBoxID {
		return false, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return false, err
	}
	if peer.Type != domain.PeerTypeChannel {
		return false, peerIDInvalidErr()
	}
	if r.deps.Channels == nil {
		return true, nil
	}
	discussion, err := r.deps.Channels.GetDiscussionMessage(ctx, userID, peer.ID, req.MsgID)
	if err != nil {
		return false, channelInvalidErr(err)
	}
	readChannelID := peer.ID
	if discussion.DiscussionChannel.ID != 0 {
		readChannelID = discussion.DiscussionChannel.ID
	}
	read, err := r.deps.Channels.ReadHistory(ctx, userID, domain.ReadChannelHistoryRequest{
		UserID:    userID,
		ChannelID: readChannelID,
		MaxID:     req.ReadMaxID,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		return false, channelInvalidErr(err)
	}
	if _, err := r.recordChannelReadInbox(ctx, userID, read); err != nil {
		return false, err
	}
	r.pushChannelReadOutboxUpdates(ctx, read.ChannelID, read.OutboxUpdates)
	return read.Changed, nil
}

func (r *Router) onMessagesGetForumTopics(ctx context.Context, req *tg.MessagesGetForumTopicsRequest) (*tg.MessagesForumTopics, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.Limit < 0 || req.Limit > 100 || utf8.RuneCountInString(req.Q) > maxMessageSearchQLength {
		return nil, limitInvalidErr()
	}
	view, err := r.forumTopicPeerView(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if !view.Channel.Forum {
		return nil, channelForumMissingErr()
	}
	includeGeneral := req.Limit > 0 && forumTopicQueryMatchesGeneral(req.Q)
	list := domain.ChannelForumTopicList{}
	if r.deps.Channels != nil && req.Limit > 0 {
		limit := req.Limit
		if includeGeneral {
			limit--
		}
		if limit > 0 {
			list, err = r.deps.Channels.GetForumTopics(ctx, userID, domain.ChannelForumTopicFilter{
				ChannelID:   view.Channel.ID,
				Query:       req.Q,
				OffsetDate:  req.OffsetDate,
				OffsetID:    req.OffsetID,
				OffsetTopic: req.OffsetTopic,
				Limit:       limit,
			})
			if err != nil {
				return nil, forumTopicError(err)
			}
		}
	}
	return r.forumTopicsResponse(ctx, userID, view, list, includeGeneral), nil
}

func (r *Router) onMessagesGetForumTopicsByID(ctx context.Context, req *tg.MessagesGetForumTopicsByIDRequest) (*tg.MessagesForumTopics, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(req.Topics) > maxForumTopicIDs {
		return nil, limitInvalidErr()
	}
	view, err := r.forumTopicPeerView(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if !view.Channel.Forum {
		return nil, channelForumMissingErr()
	}
	includeGeneral := false
	ids := make([]int, 0, len(req.Topics))
	for _, topicID := range req.Topics {
		if topicID <= 0 || topicID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
		if topicID == forumGeneralTopicID {
			includeGeneral = true
			continue
		}
		ids = append(ids, topicID)
	}
	list := domain.ChannelForumTopicList{}
	if r.deps.Channels != nil && len(ids) > 0 {
		list, err = r.deps.Channels.GetForumTopicsByID(ctx, userID, view.Channel.ID, ids)
		if err != nil {
			return nil, forumTopicError(err)
		}
	}
	return r.forumTopicsResponse(ctx, userID, view, list, includeGeneral), nil
}

func (r *Router) onMessagesGetOnlines(ctx context.Context, peer tg.InputPeerClass) (*tg.ChatOnlines, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	domainPeer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer)
	if err != nil {
		return nil, err
	}
	if domainPeer.Type == domain.PeerTypeChannel && domainPeer.ID != 0 {
		return &tg.ChatOnlines{Onlines: r.channelOnlineCount(ctx, userID, domainPeer.ID)}, nil
	}
	return &tg.ChatOnlines{Onlines: 1}, nil
}

func (r *Router) channelOnlineCount(ctx context.Context, userID, channelID int64) int {
	if channelID == 0 || r.deps.Channels == nil || r.deps.Sessions == nil {
		return 1
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return 1
	}
	online := provider.OnlineChannelUserIDs(channelID, domain.MaxChannelRealtimeFanout)
	candidates := make([]int64, 0, len(online)+1)
	if userID != 0 {
		candidates = append(candidates, userID)
	}
	candidates = append(candidates, online...)
	active, err := r.deps.Channels.FilterActiveMemberIDs(ctx, channelID, candidates)
	if err != nil {
		return 1
	}
	return len(active)
}

func (r *Router) onMessagesSetTyping(ctx context.Context, req *tg.MessagesSetTypingRequest) (bool, error) {
	if req == nil {
		return false, inputRequestInvalidErr()
	}
	topMsgID, topMsgIDSet := req.GetTopMsgID()
	if !topMsgIDSet && req.TopMsgID != 0 {
		topMsgID, topMsgIDSet = req.TopMsgID, true
	}
	if topMsgIDSet && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return false, msgIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return false, err
	}
	if peer.Type != domain.PeerTypeUser || peer.ID == 0 || peer.ID == userID {
		if peer.Type == domain.PeerTypeChannel && peer.ID != 0 && r.deps.Channels != nil {
			action := req.Action
			if action == nil {
				action = &tg.SendMessageCancelAction{}
			}
			updates := &tg.Updates{
				Updates: []tg.UpdateClass{&tg.UpdateChannelUserTyping{
					ChannelID: peer.ID,
					FromID:    &tg.PeerUser{UserID: userID},
					TopMsgID:  topMsgID,
					Action:    action,
				}},
				Date: int(r.clock.Now().Unix()),
			}
			r.pushChannelViewerUpdates(ctx, 0, peer.ID, nil, func(int64) *tg.Updates {
				return updates
			})
		}
		return true, nil
	}
	action := req.Action
	if action == nil {
		action = &tg.SendMessageCancelAction{}
	}
	update := &tg.UpdateUserTyping{
		UserID:   userID,
		TopMsgID: topMsgID,
		Action:   action,
	}
	updates := &tg.UpdateShort{
		Update: update,
		Date:   int(r.clock.Now().Unix()),
	}
	r.pushTypingUpdate(ctx, peer.ID, updates)
	return true, nil
}

func (r *Router) pushTypingUpdate(ctx context.Context, targetUserID int64, updates *tg.UpdateShort) {
	r.pushUserMessage(ctx, targetUserID, "push typing update", updates)
}

func (r *Router) onMessagesGetMessages(ctx context.Context, ids []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
	if len(ids) > maxGetMessagesIDs {
		return nil, limitInvalidErr()
	}
	if r.deps.Messages == nil || len(ids) == 0 {
		return &tg.MessagesMessages{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}

	out := make([]tg.MessageClass, 0, len(ids))
	requestedIDs := make([]int, 0, len(ids))
	for _, input := range ids {
		id, ok := inputMessageBoxID(input)
		if !ok || id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		requestedIDs = append(requestedIDs, id)
	}
	list, err := r.deps.Messages.GetMessages(ctx, userID, requestedIDs)
	if err != nil {
		return nil, internalErr()
	}
	foundByID := make(map[int]domain.Message, len(list.Messages))
	for _, msg := range list.Messages {
		foundByID[msg.ID] = msg
	}
	found := make([]domain.Message, 0, len(list.Messages))
	for _, input := range ids {
		id, ok := inputMessageBoxID(input)
		if !ok || id <= 0 || id > domain.MaxMessageBoxID {
			out = append(out, &tg.MessageEmpty{ID: id})
			continue
		}
		msg, ok := foundByID[id]
		if !ok {
			out = append(out, &tg.MessageEmpty{ID: id})
			continue
		}
		found = append(found, msg)
		out = append(out, tgMessage(msg))
	}
	chats := r.chatsForMessageUpdates(ctx, userID, found)
	return &tg.MessagesMessages{
		Messages: out,
		Users:    r.usersForMessageUpdates(ctx, userID, found),
		Chats:    chats,
	}, nil
}

func inputMessageBoxID(input tg.InputMessageClass) (int, bool) {
	switch msg := input.(type) {
	case *tg.InputMessageID:
		return msg.ID, true
	default:
		return 0, false
	}
}

func (r *Router) lookupOwnerMessage(ctx context.Context, userID int64, id int) (domain.Message, bool, error) {
	filter := domain.MessageFilter{
		MinID: id - 1,
		Limit: 1,
	}
	if id < domain.MaxMessageBoxID {
		filter.MaxID = id + 1
	}
	list, err := r.deps.Messages.Search(ctx, userID, filter)
	if err != nil {
		return domain.Message{}, false, err
	}
	if len(list.Messages) == 0 || list.Messages[0].ID != id {
		return domain.Message{}, false, nil
	}
	return list.Messages[0], true, nil
}

func (r *Router) onMessagesSaveDraft(ctx context.Context, req *tg.MessagesSaveDraftRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return false, err
	}
	peerTL := tgPeer(peer)
	if peerTL == nil {
		return true, nil
	}
	date := int(r.clock.Now().Unix())
	draft, err := r.dialogDraftFromSaveDraft(ctx, userID, peer, req, date)
	if err != nil {
		return false, err
	}
	if r.deps.Dialogs != nil {
		if err := r.deps.Dialogs.SaveDraft(ctx, userID, draft); err != nil {
			return false, dialogDraftErr(err)
		}
	}
	update := &tg.UpdateDraftMessage{
		Peer:  peerTL,
		Draft: tgDraftMessageFromSaveDraft(req, date),
	}
	if draft.TopMessageID > 0 {
		update.SetTopMsgID(draft.TopMessageID)
	}
	updates := &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Users:   r.usersForDraftUpdate(ctx, userID, peer),
		Chats:   r.chatsForDraftUpdate(ctx, userID, peer),
		Date:    date,
		Seq:     0,
	}
	r.pushDraftUpdate(ctx, userID, updates)
	return true, nil
}

func (r *Router) onMessagesGetAllDrafts(ctx context.Context) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	date := int(r.clock.Now().Unix())
	if r.deps.Dialogs == nil {
		return &tg.Updates{Updates: []tg.UpdateClass{}, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}, Date: date, Seq: 0}, nil
	}
	drafts, err := r.deps.Dialogs.ListDrafts(ctx, userID, domain.MaxDialogDraftsPerUser)
	if err != nil {
		return nil, dialogDraftErr(err)
	}
	updates := make([]tg.UpdateClass, 0, len(drafts))
	users := r.usersForDrafts(ctx, userID, drafts)
	chats := r.chatsForDrafts(ctx, userID, drafts)
	for _, draft := range drafts {
		peer := tgPeer(draft.Peer)
		if peer == nil {
			continue
		}
		update := &tg.UpdateDraftMessage{Peer: peer, Draft: tgDialogDraft(draft)}
		if draft.TopMessageID > 0 {
			update.SetTopMsgID(draft.TopMessageID)
		}
		updates = append(updates, update)
	}
	return &tg.Updates{Updates: updates, Users: users, Chats: chats, Date: date, Seq: 0}, nil
}

func (r *Router) onMessagesClearAllDrafts(ctx context.Context) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if r.deps.Dialogs == nil {
		return true, nil
	}
	drafts, err := r.deps.Dialogs.ClearDrafts(ctx, userID, domain.MaxDialogDraftsPerUser)
	if err != nil {
		return false, dialogDraftErr(err)
	}
	if len(drafts) == 0 {
		return true, nil
	}
	date := int(r.clock.Now().Unix())
	updates := make([]tg.UpdateClass, 0, len(drafts))
	for _, draft := range drafts {
		update := draftClearUpdate(draft.Peer, draft.TopMessageID, date)
		if update != nil {
			updates = append(updates, update)
		}
	}
	r.pushDraftUpdate(ctx, userID, &tg.Updates{
		Updates: updates,
		Users:   r.usersForDrafts(ctx, userID, drafts),
		Chats:   r.chatsForDrafts(ctx, userID, drafts),
		Date:    date,
		Seq:     0,
	})
	return true, nil
}

func (r *Router) dialogDraftFromSaveDraft(ctx context.Context, userID int64, peer domain.Peer, req *tg.MessagesSaveDraftRequest, date int) (domain.DialogDraft, error) {
	if req == nil {
		return domain.DialogDraft{Peer: peer, Date: date}, nil
	}
	if utf8.RuneCountInString(req.Message) > maxSendMessageTextLength {
		return domain.DialogDraft{}, messageTooLongErr()
	}
	if len(req.Entities) > maxMessageEntityCount {
		return domain.DialogDraft{}, limitInvalidErr()
	}
	if !req.SuggestedPost.Zero() {
		return domain.DialogDraft{}, suggestedPostPeerInvalidErr()
	}
	replyTo, err := r.messageReplyFromInput(ctx, userID, peer, req.ReplyTo)
	if err != nil {
		return domain.DialogDraft{}, err
	}
	webpage, err := dialogDraftWebPageFromInput(req.Media)
	if err != nil {
		return domain.DialogDraft{}, err
	}
	topMessageID := 0
	if replyTo != nil && peer.Type == domain.PeerTypeChannel && replyTo.TopMessageID > 0 {
		topMessageID = replyTo.TopMessageID
	}
	return domain.DialogDraft{
		Peer:         peer,
		TopMessageID: topMessageID,
		Date:         date,
		NoWebpage:    req.NoWebpage,
		InvertMedia:  req.InvertMedia,
		Message:      req.Message,
		Entities:     domainMessageEntities(req.Entities),
		ReplyTo:      replyTo,
		WebPage:      webpage,
		Effect:       req.Effect,
	}, nil
}

func dialogDraftWebPageFromInput(media tg.InputMediaClass) (*domain.DialogDraftWebPage, error) {
	switch m := media.(type) {
	case nil, *tg.InputMediaEmpty:
		return nil, nil
	case *tg.InputMediaWebPage:
		if m.URL == "" {
			return nil, mediaInvalidErr()
		}
		return &domain.DialogDraftWebPage{
			URL:             m.URL,
			ForceLargeMedia: m.ForceLargeMedia,
			ForceSmallMedia: m.ForceSmallMedia,
			Optional:        m.Optional,
		}, nil
	default:
		return nil, mediaInvalidErr()
	}
}

func draftClearUpdate(peer domain.Peer, topMessageID, date int) *tg.UpdateDraftMessage {
	peerTL := tgPeer(peer)
	if peerTL == nil {
		return nil
	}
	draft := &tg.DraftMessageEmpty{}
	draft.SetDate(date)
	update := &tg.UpdateDraftMessage{Peer: peerTL, Draft: draft}
	if topMessageID > 0 {
		update.SetTopMsgID(topMessageID)
	}
	return update
}

func tgDraftMessageFromSaveDraft(req *tg.MessagesSaveDraftRequest, date int) tg.DraftMessageClass {
	if req == nil || saveDraftIsEmpty(req) {
		draft := &tg.DraftMessageEmpty{}
		draft.SetDate(date)
		return draft
	}
	return &tg.DraftMessage{
		NoWebpage:     req.NoWebpage,
		InvertMedia:   req.InvertMedia,
		ReplyTo:       req.ReplyTo,
		Message:       req.Message,
		Entities:      req.Entities,
		Media:         draftInputMedia(req.Media),
		Date:          date,
		Effect:        req.Effect,
		SuggestedPost: req.SuggestedPost,
	}
}

func saveDraftIsEmpty(req *tg.MessagesSaveDraftRequest) bool {
	return !req.NoWebpage &&
		!req.InvertMedia &&
		draftReplyIsEmpty(req.ReplyTo) &&
		req.Message == "" &&
		len(req.Entities) == 0 &&
		draftInputMedia(req.Media) == nil &&
		req.Effect == 0 &&
		req.SuggestedPost.Zero()
}

func draftReplyIsEmpty(reply tg.InputReplyToClass) bool {
	if reply == nil {
		return true
	}
	input, ok := reply.(*tg.InputReplyToMessage)
	if !ok {
		return false
	}
	topMsgID, hasTopMsgID := input.GetTopMsgID()
	return input.ReplyToMsgID == 0 && hasTopMsgID && topMsgID > 0
}

func draftInputMedia(media tg.InputMediaClass) tg.InputMediaClass {
	switch media.(type) {
	case nil, *tg.InputMediaEmpty:
		return nil
	default:
		return media
	}
}

func (r *Router) usersForDraftUpdate(ctx context.Context, userID int64, peer domain.Peer) []tg.UserClass {
	if r.deps.Users == nil {
		return []tg.UserClass{}
	}
	users := make([]tg.UserClass, 0, 2)
	seen := map[int64]struct{}{}
	if self, err := r.deps.Users.Self(ctx, userID); err == nil && self.ID != 0 {
		users = append(users, r.tgSelfUser(self))
		seen[self.ID] = struct{}{}
	}
	if peer.Type == domain.PeerTypeUser {
		if _, ok := seen[peer.ID]; !ok {
			if u, found, err := r.deps.Users.ByID(ctx, userID, peer.ID); err == nil && found && u.ID != 0 {
				users = append(users, r.tgUser(u))
			}
		}
	}
	return users
}

func (r *Router) usersForDrafts(ctx context.Context, userID int64, drafts []domain.DialogDraft) []tg.UserClass {
	users := make([]tg.UserClass, 0, len(drafts)+1)
	seen := map[int64]struct{}{}
	if r.deps.Users != nil {
		if self, err := r.deps.Users.Self(ctx, userID); err == nil && self.ID != 0 {
			users = append(users, r.tgSelfUser(self))
			seen[self.ID] = struct{}{}
		}
	}
	for _, draft := range drafts {
		if draft.Peer.Type != domain.PeerTypeUser || draft.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[draft.Peer.ID]; ok {
			continue
		}
		if r.deps.Users != nil {
			if u, found, err := r.deps.Users.ByID(ctx, userID, draft.Peer.ID); err == nil && found && u.ID != 0 {
				users = append(users, r.tgUser(u))
				seen[u.ID] = struct{}{}
			}
		}
	}
	return users
}

func (r *Router) chatsForDraftUpdate(ctx context.Context, userID int64, peer domain.Peer) []tg.ChatClass {
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 || r.deps.Channels == nil {
		return []tg.ChatClass{}
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, peer.ID)
	if err != nil || view.Channel.ID == 0 {
		return []tg.ChatClass{}
	}
	return []tg.ChatClass{tgChannelChat(userID, view.Channel, &view.Self)}
}

func (r *Router) chatsForDrafts(ctx context.Context, userID int64, drafts []domain.DialogDraft) []tg.ChatClass {
	if r.deps.Channels == nil {
		return []tg.ChatClass{}
	}
	chats := make([]tg.ChatClass, 0)
	seen := map[int64]struct{}{}
	for _, draft := range drafts {
		if draft.Peer.Type != domain.PeerTypeChannel || draft.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[draft.Peer.ID]; ok {
			continue
		}
		seen[draft.Peer.ID] = struct{}{}
		view, err := r.deps.Channels.GetChannel(ctx, userID, draft.Peer.ID)
		if err != nil || view.Channel.ID == 0 {
			continue
		}
		chats = append(chats, tgChannelChat(userID, view.Channel, &view.Self))
	}
	return chats
}

func dialogDraftErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrReplyMessageIDInvalid):
		return replyMessageIDInvalidErr()
	case errors.Is(err, domain.ErrChannelInvalid):
		return peerIDInvalidErr()
	default:
		return internalErr()
	}
}

func (r *Router) pushDraftUpdate(ctx context.Context, userID int64, updates *tg.Updates) {
	r.pushUserMessage(ctx, userID, "push draft update", updates)
}

func (r *Router) clearDraftAfterSend(ctx context.Context, userID int64, peer domain.Peer, replyTo *domain.MessageReply) {
	if r.deps.Dialogs == nil || userID == 0 || peer.ID == 0 {
		return
	}
	topMessageID := 0
	if peer.Type == domain.PeerTypeChannel && replyTo != nil && replyTo.TopMessageID > 0 {
		topMessageID = replyTo.TopMessageID
	}
	changed, err := r.deps.Dialogs.DeleteDraft(ctx, userID, peer, topMessageID)
	if err != nil {
		r.log.Debug("clear draft after send", zap.Int64("user_id", userID), zap.Error(err))
		return
	}
	if !changed {
		return
	}
	date := int(r.clock.Now().Unix())
	update := draftClearUpdate(peer, topMessageID, date)
	if update == nil {
		return
	}
	r.pushDraftUpdate(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Users:   r.usersForDraftUpdate(ctx, userID, peer),
		Chats:   r.chatsForDraftUpdate(ctx, userID, peer),
		Date:    date,
		Seq:     0,
	})
}

func (r *Router) onMessagesSearchGlobal(ctx context.Context, req *tg.MessagesSearchGlobalRequest) (tg.MessagesMessagesClass, error) {
	if req.BroadcastsOnly && req.GroupsOnly {
		return &tg.MessagesMessages{}, nil
	}
	query := normalizeSearchQuery(req.Q)
	if query == "" {
		return nil, searchQueryEmptyErr()
	}
	if utf8.RuneCountInString(query) > maxMessageSearchQLength {
		return nil, limitInvalidErr()
	}
	if searchFilterNeedsMediaStore(req.Filter) {
		return &tg.MessagesMessages{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelGlobalSearchLimit {
		limit = domain.MaxChannelGlobalSearchLimit
	}
	folderID, hasFolderID := req.GetFolderID()
	if hasFolderID && folderID < 0 {
		return nil, folderIDInvalidErr()
	}
	channelOffsetID, err := r.searchGlobalChannelOffsetID(ctx, userID, req.OffsetPeer)
	if err != nil {
		return nil, err
	}
	var private domain.MessageList
	if !req.BroadcastsOnly && !req.GroupsOnly && r.deps.Messages != nil {
		filter := domain.MessageFilter{
			Query:      query,
			OffsetID:   req.OffsetID,
			OffsetDate: req.OffsetRate,
			Limit:      limit + 1,
		}
		if req.MaxDate > 0 {
			filter.OffsetDate = req.MaxDate
		}
		if req.UsersOnly || !req.BroadcastsOnly && !req.GroupsOnly {
			private, err = r.deps.Messages.Search(ctx, userID, filter)
			if err != nil {
				return nil, internalErr()
			}
			private = r.withMessageListPresence(private)
		}
	}
	if req.UsersOnly || r.deps.Channels == nil {
		return tgMessagesMessages(r.withMessageListPresence(limitMessageList(private, limit))), nil
	}
	channelHistory, err := r.deps.Channels.SearchJoinedMessages(ctx, userID, domain.ChannelGlobalSearchRequest{
		Query:           query,
		BroadcastsOnly:  req.BroadcastsOnly,
		GroupsOnly:      req.GroupsOnly,
		HasFolderID:     hasFolderID,
		FolderID:        folderID,
		OffsetRate:      req.OffsetRate,
		OffsetChannelID: channelOffsetID,
		OffsetID:        req.OffsetID,
		MinDate:         req.MinDate,
		MaxDate:         req.MaxDate,
		Limit:           limit,
	})
	if err != nil {
		return nil, channelInvalidErr(err)
	}
	channelHistory = r.enrichChannelHistory(ctx, userID, channelHistory)
	if req.BroadcastsOnly || req.GroupsOnly {
		return tgGlobalChannelMessages(userID, limitChannelHistory(channelHistory, limit)), nil
	}
	return tgGlobalSearchMessages(userID, limit, private, channelHistory), nil
}

func (r *Router) searchGlobalChannelOffsetID(ctx context.Context, userID int64, peer tg.InputPeerClass) (int64, error) {
	if peer == nil {
		return 0, nil
	}
	switch peer.(type) {
	case *tg.InputPeerEmpty:
		return 0, nil
	}
	ref, ok := inputPeerChannelRef(peer)
	if !ok {
		return 0, nil
	}
	if ref.ID <= 0 {
		return 0, peerIDInvalidErr()
	}
	if ref.CheckAccessHash && r.deps.Channels != nil {
		view, err := r.deps.Channels.GetChannel(ctx, userID, ref.ID)
		if err != nil {
			return 0, channelInvalidErr(err)
		}
		if !inputChannelAccessHashMatches(ref, view.Channel) {
			return 0, channelInvalidErr(domain.ErrChannelPrivate)
		}
	}
	return ref.ID, nil
}

func limitMessageList(list domain.MessageList, limit int) domain.MessageList {
	if limit <= 0 {
		limit = domain.MaxChannelGlobalSearchLimit
	}
	if len(list.Messages) > limit {
		list.Messages = list.Messages[:limit]
		list.Count = limit + 1
	}
	return list
}

func limitChannelHistory(history domain.ChannelHistory, limit int) domain.ChannelHistory {
	if limit <= 0 {
		limit = domain.MaxChannelGlobalSearchLimit
	}
	if len(history.Messages) > limit {
		history.Messages = history.Messages[:limit]
		history.Count = limit + 1
	}
	return history
}

func tgGlobalChannelMessages(viewerUserID int64, history domain.ChannelHistory) tg.MessagesMessagesClass {
	out := tgChannelHistoryMessages(viewerUserID, history)
	if slice, ok := out.(*tg.MessagesMessagesSlice); ok && len(history.Messages) > 0 {
		slice.SetNextRate(history.Messages[len(history.Messages)-1].Date)
	}
	return out
}

type globalSearchHit struct {
	date      int
	peerRank  int64
	messageID int
	message   tg.MessageClass
}

func tgGlobalSearchMessages(viewerUserID int64, limit int, private domain.MessageList, channel domain.ChannelHistory) tg.MessagesMessagesClass {
	if limit <= 0 {
		limit = domain.MaxChannelGlobalSearchLimit
	}
	hits := make([]globalSearchHit, 0, len(private.Messages)+len(channel.Messages))
	for _, msg := range private.Messages {
		item := tgMessage(msg)
		if item == nil {
			continue
		}
		hits = append(hits, globalSearchHit{
			date:      msg.Date,
			peerRank:  msg.Peer.ID,
			messageID: msg.ID,
			message:   item,
		})
	}
	for _, msg := range channel.Messages {
		item := tgChannelMessage(viewerUserID, msg)
		if item == nil {
			continue
		}
		hits = append(hits, globalSearchHit{
			date:      msg.Date,
			peerRank:  msg.ChannelID,
			messageID: msg.ID,
			message:   item,
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i], hits[j]
		if a.date != b.date {
			return a.date > b.date
		}
		if a.peerRank != b.peerRank {
			return a.peerRank > b.peerRank
		}
		return a.messageID > b.messageID
	})
	hasMore := private.Count > len(private.Messages) || channel.Count > len(channel.Messages) || len(hits) > limit
	if len(hits) > limit {
		hits = hits[:limit]
	}
	messages := make([]tg.MessageClass, 0, len(hits))
	for _, hit := range hits {
		messages = append(messages, hit.message)
	}
	users := append(tgUsers(private.Users), tgUsers(channel.Users)...)
	chats := tgChannels(viewerUserID, channel.Channels)
	if hasMore {
		out := &tg.MessagesMessagesSlice{
			Count:    limit + 1,
			Messages: messages,
			Chats:    chats,
			Users:    users,
		}
		if len(hits) > 0 {
			out.SetNextRate(hits[len(hits)-1].date)
		}
		return out
	}
	return &tg.MessagesMessages{Messages: messages, Chats: chats, Users: users}
}

func (r *Router) onMessagesGetDialogFilters(ctx context.Context) (*tg.MessagesDialogFilters, error) {
	if r.deps.Dialogs == nil {
		return tgDialogFilters(domain.DialogFolderList{}), nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	list, err := r.deps.Dialogs.GetDialogFolders(ctx, userID)
	if err != nil {
		return nil, internalErr()
	}
	return tgDialogFilters(list), nil
}

func (r *Router) onMessagesUpdateDialogFilter(ctx context.Context, req *tg.MessagesUpdateDialogFilterRequest) (bool, error) {
	if req.ID < domain.DialogCustomFolderMinID {
		return false, filterIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	filter, ok := req.GetFilter()
	var folder *domain.DialogFolder
	if ok {
		parsed, err := r.dialogFolderFromTG(ctx, userID, req.ID, filter)
		if err != nil {
			return false, err
		}
		folder = &parsed
		if r.deps.Dialogs != nil {
			if err := r.deps.Dialogs.SaveDialogFolder(ctx, userID, parsed); err != nil {
				return false, internalErr()
			}
		}
	} else if r.deps.Dialogs != nil {
		if err := r.deps.Dialogs.DeleteDialogFolder(ctx, userID, req.ID); err != nil {
			return false, internalErr()
		}
	}
	event := domain.UpdateEvent{
		Type:         domain.UpdateEventDialogFilter,
		FilterID:     req.ID,
		DialogFilter: folder,
		Date:         int(r.clock.Now().Unix()),
	}
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		event, _, err = r.deps.Updates.RecordDialogFilter(ctx, authKeyID, userID, req.ID, folder, sessionID)
		if err != nil {
			return false, internalErr()
		}
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, tgUpdateForOutboxEvent(event))
	return true, nil
}

func (r *Router) onMessagesUpdateDialogFiltersOrder(ctx context.Context, order []int) (bool, error) {
	if len(order) > domain.MaxDialogFolders {
		return false, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	clean := cleanDialogFilterOrder(order)
	if r.deps.Dialogs != nil {
		if err := r.deps.Dialogs.ReorderDialogFolders(ctx, userID, clean); err != nil {
			return false, internalErr()
		}
	}
	event := domain.UpdateEvent{
		Type:        domain.UpdateEventDialogFilterOrder,
		FilterOrder: clean,
		Date:        int(r.clock.Now().Unix()),
	}
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		event, _, err = r.deps.Updates.RecordDialogFilterOrder(ctx, authKeyID, userID, clean, sessionID)
		if err != nil {
			return false, internalErr()
		}
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, tgUpdateForOutboxEvent(event))
	return true, nil
}

func (r *Router) onMessagesToggleDialogFilterTags(ctx context.Context, enabled bool) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if r.deps.Dialogs != nil {
		if err := r.deps.Dialogs.ToggleDialogFolderTags(ctx, userID, enabled); err != nil {
			return false, internalErr()
		}
	}
	event := domain.UpdateEvent{
		Type:        domain.UpdateEventDialogFilters,
		TagsEnabled: enabled,
		Date:        int(r.clock.Now().Unix()),
	}
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		event, _, err = r.deps.Updates.RecordDialogFiltersReload(ctx, authKeyID, userID, sessionID)
		if err != nil {
			return false, internalErr()
		}
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, tgUpdateForOutboxEvent(event))
	return true, nil
}

func (r *Router) onMessagesSaveDefaultSendAs(ctx context.Context, req *tg.MessagesSaveDefaultSendAsRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if userID == 0 {
		return false, peerIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return false, err
	}
	if peer.Type != domain.PeerTypeChannel {
		return false, peerIDInvalidErr()
	}
	sendAs, err := r.sendAsPeerFromInput(ctx, userID, peer, req.SendAs)
	if err != nil {
		return false, err
	}
	if r.deps.Channels == nil {
		return false, channelInvalidErr(domain.ErrChannelInvalid)
	}
	if _, err := r.deps.Channels.SaveDefaultSendAs(ctx, userID, domain.SaveChannelDefaultSendAsRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		SendAs:    sendAs,
	}); err != nil {
		return false, channelInvalidErr(err)
	}
	return true, nil
}

func (r *Router) sendAsPeerFromInput(ctx context.Context, userID int64, to domain.Peer, input tg.InputPeerClass) (*domain.Peer, error) {
	if input == nil {
		return nil, nil
	}
	if to.Type != domain.PeerTypeChannel {
		return nil, sendAsPeerInvalidErr()
	}
	sendAs, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input)
	if err != nil {
		return nil, sendAsPeerInvalidErr()
	}
	switch sendAs.Type {
	case domain.PeerTypeUser:
		if sendAs.ID != userID {
			return nil, sendAsPeerInvalidErr()
		}
		return nil, nil
	case domain.PeerTypeChannel:
		if sendAs.ID != to.ID {
			return nil, sendAsPeerInvalidErr()
		}
		if err := r.validateCurrentChannelSendAs(ctx, userID, to.ID); err != nil {
			return nil, err
		}
		out := sendAs
		return &out, nil
	default:
		return nil, sendAsPeerInvalidErr()
	}
}

func (r *Router) resolveSendAsPeer(ctx context.Context, userID int64, to domain.Peer, input tg.InputPeerClass) (*domain.Peer, error) {
	if input != nil {
		return r.sendAsPeerFromInput(ctx, userID, to, input)
	}
	if to.Type != domain.PeerTypeChannel || r.deps.Channels == nil {
		return nil, nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, to.ID)
	if err != nil {
		return nil, nil
	}
	return validDefaultSendAsPeer(view), nil
}

func validDefaultSendAsPeer(view domain.ChannelView) *domain.Peer {
	if view.Dialog.DefaultSendAs == nil || view.Dialog.DefaultSendAs.ID == 0 {
		return nil
	}
	switch view.Dialog.DefaultSendAs.Type {
	case domain.PeerTypeUser:
		return nil
	case domain.PeerTypeChannel:
		if view.Dialog.DefaultSendAs.ID != view.Channel.ID || !canCurrentChannelSendAs(view) {
			return nil
		}
		out := *view.Dialog.DefaultSendAs
		return &out
	default:
		return nil
	}
}

func (r *Router) validateCurrentChannelSendAs(ctx context.Context, userID, channelID int64) error {
	if r.deps.Channels == nil {
		return sendAsPeerInvalidErr()
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, channelID)
	if err != nil {
		return sendAsPeerInvalidErr()
	}
	if canCurrentChannelSendAs(view) {
		return nil
	}
	return sendAsPeerInvalidErr()
}

func canCurrentChannelSendAs(view domain.ChannelView) bool {
	if view.Self.Role == domain.ChannelRoleCreator {
		return true
	}
	if view.Channel.Broadcast && view.Self.Role == domain.ChannelRoleAdmin && view.Self.AdminRights.PostMessages {
		return true
	}
	if !view.Channel.Broadcast && view.Self.Role == domain.ChannelRoleAdmin && view.Self.AdminRights.Anonymous {
		return true
	}
	return false
}

func (r *Router) onMessagesSendMessage(ctx context.Context, req *tg.MessagesSendMessageRequest) (tg.UpdatesClass, error) {
	start := r.clock.Now()
	var duplicate bool
	var sendErr error
	defer func() {
		r.metrics().MessageSend(r.clock.Now().Sub(start), duplicate, sendErr)
	}()
	if req.Message == "" {
		sendErr = messageEmptyErr()
		return nil, messageEmptyErr()
	}
	if utf8.RuneCountInString(req.Message) > maxSendMessageTextLength {
		sendErr = messageTooLongErr()
		return nil, sendErr
	}
	if len(req.Entities) > maxMessageEntityCount {
		sendErr = limitInvalidErr()
		return nil, sendErr
	}
	if req.RandomID == 0 {
		sendErr = randomIDEmptyErr()
		return nil, sendErr
	}
	if req.ScheduleDate != 0 || req.ScheduleRepeatPeriod != 0 {
		sendErr = scheduleDateInvalidErr()
		return nil, sendErr
	}
	if err := sendMessageUnsupportedOptionErr(req); err != nil {
		sendErr = err
		return nil, sendErr
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		sendErr = internalErr()
		return nil, sendErr
	}
	if userID == 0 {
		sendErr = peerIDInvalidErr()
		return nil, sendErr
	}
	if r.deps.Limiter != nil {
		allowed, retryAfter, err := r.deps.Limiter.Allow(ctx, "messages:send:"+strconv.FormatInt(userID, 10), sendMessageRateLimit, sendMessageRateWindow)
		if err != nil {
			sendErr = internalErr()
			return nil, sendErr
		}
		if !allowed {
			r.metrics().MessageRateLimited(retryAfter)
			sendErr = floodWaitErr(retryAfter)
			return nil, sendErr
		}
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		sendErr = err
		return nil, sendErr
	}
	updates, dup, err := r.sendOutgoing(ctx, userID, peer, outgoingSend{
		randomID:     req.RandomID,
		message:      req.Message,
		entities:     req.Entities,
		silent:       req.Silent,
		noforwards:   req.Noforwards,
		replyToInput: req.ReplyTo,
		sendAsInput:  req.SendAs,
		clearDraft:   req.ClearDraft,
	})
	duplicate = dup
	if err != nil {
		sendErr = err
		return nil, sendErr
	}
	return updates, nil
}

func (r *Router) onMessagesForwardMessages(ctx context.Context, req *tg.MessagesForwardMessagesRequest) (tg.UpdatesClass, error) {
	if len(req.ID) == 0 || len(req.ID) != len(req.RandomID) {
		return nil, inputRequestInvalidErr()
	}
	if len(req.ID) > domain.MaxForwardMessageIDs {
		return nil, limitInvalidErr()
	}
	if req.ScheduleDate != 0 || req.ScheduleRepeatPeriod != 0 {
		return nil, scheduleDateInvalidErr()
	}
	if err := forwardMessagesUnsupportedOptionErr(req); err != nil {
		return nil, err
	}
	topMsgID, topMsgIDSet := req.GetTopMsgID()
	if !topMsgIDSet && req.TopMsgID != 0 {
		topMsgID, topMsgIDSet = req.TopMsgID, true
	}
	if topMsgIDSet && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, replyMessageIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, peerIDInvalidErr()
	}
	fromPeer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.FromPeer)
	if err != nil {
		return nil, err
	}
	toPeer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.ToPeer)
	if err != nil {
		return nil, err
	}
	sendAs, err := r.resolveSendAsPeer(ctx, userID, toPeer, req.SendAs)
	if err != nil {
		return nil, err
	}
	replyTo, err := r.messageReplyFromInput(ctx, userID, toPeer, req.ReplyTo)
	if err != nil {
		return nil, err
	}
	replyTo, err = mergeForwardTopMsgID(toPeer, replyTo, topMsgID, topMsgIDSet)
	if err != nil {
		return nil, err
	}
	if r.deps.Users != nil {
		for _, peer := range []domain.Peer{fromPeer, toPeer} {
			if peer.Type != domain.PeerTypeUser || peer.ID == userID {
				continue
			}
			if _, found, err := r.deps.Users.ByID(ctx, userID, peer.ID); err != nil {
				return nil, internalErr()
			} else if !found {
				return nil, peerIDInvalidErr()
			}
		}
	}
	for i, id := range req.ID {
		if id <= 0 || id > domain.MaxMessageBoxID || req.RandomID[i] == 0 {
			return nil, messageIDInvalidErr()
		}
	}
	if r.deps.Limiter != nil {
		allowed, retryAfter, err := r.deps.Limiter.Allow(ctx, "messages:forward:"+strconv.FormatInt(userID, 10), sendMessageRateLimit, sendMessageRateWindow)
		if err != nil {
			return nil, internalErr()
		}
		if !allowed {
			r.metrics().MessageRateLimited(retryAfter)
			return nil, floodWaitErr(retryAfter)
		}
	}
	if toPeer.Type == domain.PeerTypeChannel {
		if r.deps.Channels == nil {
			return nil, peerIDInvalidErr()
		}
		sources, err := r.forwardSources(ctx, userID, fromPeer, req.ID)
		if err != nil {
			return nil, messageForwardErr(err)
		}
		recipients := make([]int64, 0)
		results := make([]domain.SendChannelMessageResult, 0, len(sources))
		extraUserIDs := make([]int64, 0, len(sources))
		for i, source := range sources {
			forward := source.forward
			if req.DropAuthor {
				forward = nil
			}
			res, err := r.deps.Channels.SendMessage(ctx, userID, domain.SendChannelMessageRequest{
				UserID:     userID,
				ChannelID:  toPeer.ID,
				RandomID:   req.RandomID[i],
				Message:    source.body,
				Entities:   source.entities,
				Media:      source.media,
				Silent:     req.Silent,
				NoForwards: req.Noforwards,
				ReplyTo:    replyTo,
				Forward:    forward,
				SendAs:     sendAs,
				Date:       int(r.clock.Now().Unix()),
			})
			if err != nil {
				return nil, channelInvalidErr(err)
			}
			results = append(results, res)
			recipients = append(recipients, res.Recipients...)
			if sourceUserID := source.userID(); sourceUserID != 0 {
				extraUserIDs = append(extraUserIDs, sourceUserID)
			}
		}
		updates := r.channelMessagesUpdates(ctx, userID, results, req.RandomID, true, extraUserIDs)
		r.pushChannelUpdates(ctx, userID, toPeer.ID, recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelMessagesUpdates(ctx, viewerUserID, results, nil, false, extraUserIDs)
		})
		for _, res := range results {
			r.pushChannelDiscussionUpdate(ctx, userID, res.Discussion)
		}
		return updates, nil
	}
	if fromPeer.Type == domain.PeerTypeChannel && toPeer.Type == domain.PeerTypeUser {
		if r.deps.Channels == nil || r.deps.Messages == nil {
			return nil, peerIDInvalidErr()
		}
		recipientBlocked, err := r.peerBlocksUser(ctx, userID, toPeer.ID)
		if err != nil {
			return nil, err
		}
		sources, err := r.forwardSources(ctx, userID, fromPeer, req.ID)
		if err != nil {
			return nil, messageForwardErr(err)
		}
		sessionID, _ := SessionIDFrom(ctx)
		authKeyID, _ := AuthKeyIDFrom(ctx)
		res := domain.ForwardPrivateMessagesResult{OwnerUserID: userID}
		for i, source := range sources {
			forward := source.forward
			if req.DropAuthor {
				forward = nil
			}
			sent, err := r.deps.Messages.SendPrivateText(ctx, userID, domain.SendPrivateTextRequest{
				SenderUserID:     userID,
				RecipientUserID:  toPeer.ID,
				RandomID:         req.RandomID[i],
				Message:          source.body,
				Entities:         source.entities,
				Media:            source.media,
				Silent:           req.Silent,
				NoForwards:       req.Noforwards,
				ReplyTo:          replyTo,
				Forward:          forward,
				Date:             int(r.clock.Now().Unix()),
				OriginAuthKeyID:  authKeyID,
				OriginSessionID:  sessionID,
				RecipientBlocked: recipientBlocked,
			})
			if err != nil {
				return nil, messageForwardErr(err)
			}
			res.SenderMessages = append(res.SenderMessages, sent.SenderMessage)
			res.RecipientMessages = append(res.RecipientMessages, sent.RecipientMessage)
			res.SenderEvents = append(res.SenderEvents, sent.SenderEvent)
			res.RecipientEvents = append(res.RecipientEvents, sent.RecipientEvent)
			res.Duplicates = append(res.Duplicates, sent.Duplicate)
		}
		return tgForwardMessagesUpdates(res, req.RandomID, r.usersForMessageUpdates(ctx, userID, res.SenderMessages), r.chatsForMessageUpdates(ctx, userID, res.SenderMessages)), nil
	}
	if fromPeer.Type != domain.PeerTypeUser || toPeer.Type != domain.PeerTypeUser || r.deps.Messages == nil {
		return nil, peerIDInvalidErr()
	}
	sessionID, _ := SessionIDFrom(ctx)
	authKeyID, _ := AuthKeyIDFrom(ctx)
	recipientBlocked, err := r.peerBlocksUser(ctx, userID, toPeer.ID)
	if err != nil {
		return nil, err
	}
	res, err := r.deps.Messages.ForwardPrivateMessages(ctx, userID, domain.ForwardPrivateMessagesRequest{
		OwnerUserID:      userID,
		FromPeer:         fromPeer,
		ToUserID:         toPeer.ID,
		MessageIDs:       append([]int(nil), req.ID...),
		RandomIDs:        append([]int64(nil), req.RandomID...),
		Silent:           req.Silent,
		NoForwards:       req.Noforwards,
		DropAuthor:       req.DropAuthor,
		ReplyTo:          replyTo,
		Date:             int(r.clock.Now().Unix()),
		OriginAuthKeyID:  authKeyID,
		OriginSessionID:  sessionID,
		RecipientBlocked: recipientBlocked,
	})
	if err != nil {
		return nil, messageForwardErr(err)
	}
	return tgForwardMessagesUpdates(res, req.RandomID, r.usersForMessageUpdates(ctx, userID, res.SenderMessages), r.chatsForMessageUpdates(ctx, userID, res.SenderMessages)), nil
}

func mergeForwardTopMsgID(toPeer domain.Peer, replyTo *domain.MessageReply, topMsgID int, topMsgIDSet bool) (*domain.MessageReply, error) {
	if !topMsgIDSet || topMsgID == 0 {
		return replyTo, nil
	}
	if topMsgID < 0 || topMsgID > domain.MaxMessageBoxID || toPeer.Type != domain.PeerTypeChannel {
		return nil, replyMessageIDInvalidErr()
	}
	if replyTo == nil {
		return &domain.MessageReply{
			Peer:         toPeer,
			TopMessageID: topMsgID,
			ForumTopic:   true,
		}, nil
	}
	if replyTo.Peer.ID != 0 && replyTo.Peer != toPeer {
		return nil, replyMessageIDInvalidErr()
	}
	if replyTo.TopMessageID != 0 && replyTo.TopMessageID != topMsgID {
		return nil, replyMessageIDInvalidErr()
	}
	merged := *replyTo
	merged.Peer = toPeer
	merged.TopMessageID = topMsgID
	merged.QuoteEntities = append([]domain.MessageEntity(nil), replyTo.QuoteEntities...)
	if merged.MessageID == 0 {
		merged.ForumTopic = true
	}
	return &merged, nil
}

type forwardSource struct {
	body      string
	entities  []domain.MessageEntity
	media     *domain.MessageMedia
	forward   *domain.MessageForward
	from      domain.Peer
	date      int
	noForward bool
}

func (s forwardSource) userID() int64 {
	if s.from.Type == domain.PeerTypeUser {
		return s.from.ID
	}
	if s.forward != nil && s.forward.From.Type == domain.PeerTypeUser {
		return s.forward.From.ID
	}
	return 0
}

func (r *Router) forwardSources(ctx context.Context, userID int64, fromPeer domain.Peer, ids []int) ([]forwardSource, error) {
	out := make([]forwardSource, 0, len(ids))
	for _, id := range ids {
		switch fromPeer.Type {
		case domain.PeerTypeUser:
			if r.deps.Messages == nil {
				return nil, domain.ErrMessageIDInvalid
			}
			list, err := r.deps.Messages.GetMessages(ctx, userID, []int{id})
			if err != nil || len(list.Messages) != 1 || list.Messages[0].ID != id {
				return nil, domain.ErrMessageIDInvalid
			}
			msg := list.Messages[0]
			if msg.Peer != fromPeer {
				return nil, domain.ErrMessageIDInvalid
			}
			if msg.NoForwards {
				return nil, domain.ErrChatForwardsRestricted
			}
			forward := cloneDomainMessageForward(msg.Forward)
			if forward == nil {
				forward = &domain.MessageForward{From: msg.From, Date: msg.Date}
			}
			out = append(out, forwardSource{
				body: msg.Body,
				entities: append([]domain.MessageEntity(nil),
					msg.Entities...),
				media:   msg.Media,
				forward: forward,
				from:    msg.From,
				date:    msg.Date,
			})
		case domain.PeerTypeChannel:
			if r.deps.Channels == nil {
				return nil, domain.ErrMessageIDInvalid
			}
			history, err := r.deps.Channels.GetMessages(ctx, userID, fromPeer.ID, []int{id})
			if err != nil || len(history.Messages) != 1 || history.Messages[0].ID != id {
				return nil, domain.ErrMessageIDInvalid
			}
			msg := history.Messages[0]
			if msg.NoForwards || history.Channel.NoForwards {
				return nil, domain.ErrChatForwardsRestricted
			}
			if msg.Action != nil || (msg.Body == "" && msg.Media.IsZero()) {
				return nil, domain.ErrMessageIDInvalid
			}
			forward := cloneDomainMessageForward(msg.Forward)
			from := msg.From
			if from.ID == 0 && msg.SenderUserID != 0 {
				from = domain.Peer{Type: domain.PeerTypeUser, ID: msg.SenderUserID}
			}
			if msg.Post {
				from = domain.Peer{Type: domain.PeerTypeChannel, ID: msg.ChannelID}
			}
			if forward == nil {
				forward = &domain.MessageForward{From: from, Date: msg.Date}
				if from.Type == domain.PeerTypeChannel {
					forward.ChannelPost = msg.ID
				}
			}
			out = append(out, forwardSource{
				body: msg.Body,
				entities: append([]domain.MessageEntity(nil),
					msg.Entities...),
				media:   msg.Media,
				forward: forward,
				from:    from,
				date:    msg.Date,
			})
		default:
			return nil, domain.ErrMessageIDInvalid
		}
	}
	return out, nil
}

func cloneDomainMessageForward(in *domain.MessageForward) *domain.MessageForward {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (r *Router) onMessagesEditMessage(ctx context.Context, req *tg.MessagesEditMessageRequest) (tg.UpdatesClass, error) {
	if _, ok := req.GetScheduleDate(); ok {
		return nil, scheduleDateInvalidErr()
	}
	if _, ok := req.GetScheduleRepeatPeriod(); ok {
		return nil, scheduleDateInvalidErr()
	}
	if _, ok := req.GetQuickReplyShortcutID(); ok {
		return nil, messageIDInvalidErr()
	}
	if media, ok := req.GetMedia(); ok && !editMessageMediaCanDegradeToText(media) {
		return nil, mediaInvalidErr()
	}
	if _, ok := req.GetReplyMarkup(); ok {
		return nil, replyMarkupInvalidErr()
	}
	message, ok := req.GetMessage()
	if !ok {
		return nil, messageEmptyErr()
	}
	if message == "" {
		return nil, messageEmptyErr()
	}
	if utf8.RuneCountInString(message) > maxSendMessageTextLength {
		return nil, messageTooLongErr()
	}
	entities, _ := req.GetEntities()
	if len(entities) > maxMessageEntityCount {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, peerIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel {
		if r.deps.Channels == nil {
			return nil, peerIDInvalidErr()
		}
		res, err := r.deps.Channels.EditMessage(ctx, userID, domain.EditChannelMessageRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			ID:        req.ID,
			Message:   message,
			Entities:  domainMessageEntities(entities),
			EditDate:  int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, channelEditErr(err)
		}
		updates := r.channelEditMessageUpdates(ctx, userID, res)
		r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
			return r.channelEditMessageUpdates(ctx, viewerUserID, res)
		})
		return updates, nil
	}
	if peer.Type != domain.PeerTypeUser || r.deps.Messages == nil {
		return nil, peerIDInvalidErr()
	}
	blocked, err := r.peerBlocksUser(ctx, userID, peer.ID)
	if err != nil {
		return nil, err
	}
	if blocked {
		return nil, messageEditForbiddenErr()
	}
	sessionID, _ := SessionIDFrom(ctx)
	authKeyID, _ := AuthKeyIDFrom(ctx)
	res, err := r.deps.Messages.EditMessage(ctx, userID, domain.EditMessageRequest{
		OwnerUserID:     userID,
		Peer:            peer,
		ID:              req.ID,
		Message:         message,
		Entities:        domainMessageEntities(entities),
		EditDate:        int(r.clock.Now().Unix()),
		OriginAuthKeyID: authKeyID,
		OriginSessionID: sessionID,
	})
	if err != nil {
		return nil, messageEditErr(err)
	}
	self := res.Self()
	if self.Event.Pts == 0 || self.Message.ID == 0 {
		return nil, messageIDInvalidErr()
	}
	users := r.usersForMessageUpdate(ctx, userID, self.Message)
	chats := r.chatsForMessageUpdate(ctx, userID, self.Message)
	return tgEditMessageUpdates(self.Event, self.Message, users, chats), nil
}

func editMessageMediaCanDegradeToText(media tg.InputMediaClass) bool {
	switch media.(type) {
	case *tg.InputMediaEmpty, *tg.InputMediaWebPage:
		return true
	default:
		return false
	}
}

func (r *Router) onMessagesGetMessageEditData(ctx context.Context, req *tg.MessagesGetMessageEditDataRequest) (*tg.MessagesMessageEditData, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	switch peer.Type {
	case domain.PeerTypeChannel:
		if r.deps.Channels == nil {
			return nil, peerIDInvalidErr()
		}
		history, err := r.deps.Channels.GetHistory(ctx, userID, domain.ChannelHistoryFilter{
			ChannelID: peer.ID,
			Limit:     1,
			MaxID:     req.ID,
			MinID:     req.ID - 1,
		})
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if len(history.Messages) != 1 || history.Messages[0].ID != req.ID {
			return nil, messageIDInvalidErr()
		}
		msg := history.Messages[0]
		if msg.Deleted || msg.Action != nil {
			return nil, messageIDInvalidErr()
		}
		view, err := r.deps.Channels.GetChannel(ctx, userID, peer.ID)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		if msg.SenderUserID != userID && !canEditChannelMessageForRPC(view.Self) {
			return nil, messageAuthorRequiredErr()
		}
	case domain.PeerTypeUser:
		if r.deps.Messages == nil {
			return nil, messageIDInvalidErr()
		}
		msg, ok, err := r.lookupOwnerMessage(ctx, userID, req.ID)
		if err != nil {
			return nil, internalErr()
		}
		if !ok || msg.Peer != peer {
			return nil, messageIDInvalidErr()
		}
		if !msg.Out || msg.From.ID != userID {
			return nil, messageAuthorRequiredErr()
		}
	default:
		return nil, peerIDInvalidErr()
	}
	return &tg.MessagesMessageEditData{}, nil
}

func canEditChannelMessageForRPC(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && member.AdminRights.EditMessages)
}

func (r *Router) onMessagesGetOutboxReadDate(ctx context.Context, req *tg.MessagesGetOutboxReadDateRequest) (*tg.OutboxReadDate, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 || r.deps.Messages == nil {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	date, err := r.deps.Messages.GetOutboxReadDate(ctx, userID, domain.OutboxReadDateRequest{
		OwnerUserID: userID,
		Peer:        peer,
		ID:          req.MsgID,
	})
	if err != nil {
		return nil, messageReadDateErr(err)
	}
	return &tg.OutboxReadDate{Date: date}, nil
}

func (r *Router) onMessagesGetMessageReadParticipants(ctx context.Context, req *tg.MessagesGetMessageReadParticipantsRequest) ([]tg.ReadParticipantDate, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return nil, peerIDInvalidErr()
	}
	if r.deps.Channels == nil {
		return []tg.ReadParticipantDate{}, nil
	}
	res, err := r.deps.Channels.GetMessageReadParticipants(ctx, userID, domain.ChannelReadParticipantsRequest{
		UserID:    userID,
		ChannelID: peer.ID,
		MessageID: req.MsgID,
		Limit:     domain.MaxChannelReadParticipants,
		Date:      int(r.clock.Now().Unix()),
	})
	if err != nil {
		if errors.Is(err, domain.ErrMessageIDInvalid) {
			return nil, messageIDInvalidErr()
		}
		return nil, channelInvalidErr(err)
	}
	out := make([]tg.ReadParticipantDate, 0, len(res.Participants))
	for _, p := range res.Participants {
		if p.UserID == 0 {
			continue
		}
		out = append(out, tg.ReadParticipantDate{UserID: p.UserID, Date: p.Date})
	}
	return out, nil
}

func (r *Router) onMessagesDeleteMessages(ctx context.Context, req *tg.MessagesDeleteMessagesRequest) (*tg.MessagesAffectedMessages, error) {
	authKeyID, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(req.ID) == 0 || r.deps.Messages == nil {
		return r.affectedMessages(ctx, authKeyID, userID)
	}
	if len(req.ID) > domain.MaxDeleteMessageIDs {
		return nil, limitInvalidErr()
	}
	if req.GetRevoke() {
		list, err := r.deps.Messages.GetMessages(ctx, userID, req.ID)
		if err != nil {
			return nil, internalErr()
		}
		blocked, err := r.messagesTouchBlockedPeer(ctx, userID, list.Messages)
		if err != nil {
			return nil, err
		}
		if blocked {
			return nil, messageDeleteForbiddenErr()
		}
	}
	sessionID, _ := SessionIDFrom(ctx)
	res, err := r.deps.Messages.DeleteMessages(ctx, userID, domain.DeleteMessagesRequest{
		OwnerUserID:     userID,
		IDs:             req.ID,
		Revoke:          req.GetRevoke(),
		Date:            int(r.clock.Now().Unix()),
		OriginAuthKeyID: authKeyID,
		OriginSessionID: sessionID,
	})
	if err != nil {
		return nil, internalErr()
	}
	self := res.Self()
	if len(self.MessageIDs) == 0 || self.Event.Pts == 0 {
		return r.affectedMessages(ctx, authKeyID, userID)
	}
	return &tg.MessagesAffectedMessages{Pts: self.Event.Pts, PtsCount: self.Event.PtsCount}, nil
}

func (r *Router) onMessagesDeleteHistory(ctx context.Context, req *tg.MessagesDeleteHistoryRequest) (*tg.MessagesAffectedHistory, error) {
	authKeyID, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, ok := req.GetMinDate(); ok {
		return r.affectedHistory(ctx, authKeyID, userID, 0)
	}
	if _, ok := req.GetMaxDate(); ok {
		return r.affectedHistory(ctx, authKeyID, userID, 0)
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	if peer.Type == domain.PeerTypeChannel {
		if r.deps.Channels == nil {
			return r.affectedHistory(ctx, authKeyID, userID, 0)
		}
		if req.MaxID < 0 || req.MaxID > domain.MaxMessageBoxID {
			return nil, messageIDInvalidErr()
		}
		res, err := r.deps.Channels.DeleteHistory(ctx, userID, domain.DeleteChannelHistoryRequest{
			UserID:      userID,
			ChannelID:   peer.ID,
			MaxID:       req.MaxID,
			ForEveryone: req.GetRevoke(),
			Date:        int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, channelDeleteErr(err)
		}
		if res.Event.Pts != 0 {
			r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
				return r.channelDeleteMessagesUpdates(viewerUserID, res.Channel, res.Event)
			})
			return &tg.MessagesAffectedHistory{Pts: res.Event.Pts, PtsCount: res.Event.PtsCount, Offset: res.Offset}, nil
		}
		if res.AvailableMinID > 0 {
			event := r.recordChannelAvailableMessages(ctx, userID, res.Channel.ID, res.AvailableMinID)
			updates := r.channelAvailableMessagesUpdates(userID, res.Channel, event.MaxID)
			r.pushUserUpdates(ctx, userID, updates)
			if event.Pts != 0 {
				return &tg.MessagesAffectedHistory{Pts: event.Pts, PtsCount: event.PtsCount, Offset: res.Offset}, nil
			}
		}
		return &tg.MessagesAffectedHistory{Pts: res.Channel.Pts, PtsCount: 0, Offset: res.Offset}, nil
	}
	if peer.Type != domain.PeerTypeUser {
		return nil, peerIDInvalidErr()
	}
	if r.deps.Messages == nil {
		return r.affectedHistory(ctx, authKeyID, userID, 0)
	}
	if req.GetRevoke() {
		blocked, err := r.peerBlocksUser(ctx, userID, peer.ID)
		if err != nil {
			return nil, err
		}
		if blocked {
			return nil, messageDeleteForbiddenErr()
		}
	}
	sessionID, _ := SessionIDFrom(ctx)
	res, err := r.deps.Messages.DeleteHistory(ctx, userID, domain.DeleteHistoryRequest{
		OwnerUserID:     userID,
		Peer:            peer,
		MaxID:           req.MaxID,
		JustClear:       req.GetJustClear(),
		Revoke:          req.GetRevoke(),
		Date:            int(r.clock.Now().Unix()),
		OriginAuthKeyID: authKeyID,
		OriginSessionID: sessionID,
	})
	if err != nil {
		return nil, internalErr()
	}
	self := res.Self()
	if len(self.MessageIDs) == 0 || self.Event.Pts == 0 {
		return r.affectedHistory(ctx, authKeyID, userID, 0)
	}
	return &tg.MessagesAffectedHistory{
		Pts:      self.Event.Pts,
		PtsCount: self.Event.PtsCount,
		Offset:   res.Offset,
	}, nil
}

func messageEditErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	case errors.Is(err, domain.ErrMessageAuthorRequired):
		return messageAuthorRequiredErr()
	case errors.Is(err, domain.ErrMessageNotModified):
		return messageNotModifiedErr()
	default:
		return internalErr()
	}
}

func channelEditErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	case errors.Is(err, domain.ErrMessageAuthorRequired):
		return messageAuthorRequiredErr()
	case errors.Is(err, domain.ErrMessageNotModified):
		return messageNotModifiedErr()
	default:
		return channelInvalidErr(err)
	}
}

func messageReadDateErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	case errors.Is(err, domain.ErrMessageNotReadYet):
		return messageNotReadYetErr()
	default:
		return internalErr()
	}
}

func messageReactionErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	default:
		return internalErr()
	}
}

func messageSendErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrReplyMessageIDInvalid):
		return replyMessageIDInvalidErr()
	default:
		return internalErr()
	}
}

func (r *Router) peerBlocksUser(ctx context.Context, userID, peerUserID int64) (bool, error) {
	if userID == 0 || peerUserID == 0 || userID == peerUserID || r.deps.Contacts == nil {
		return false, nil
	}
	blocked, err := r.deps.Contacts.IsBlocked(ctx, peerUserID, userID)
	if err != nil {
		return false, internalErr()
	}
	return blocked, nil
}

func (r *Router) messagesTouchBlockedPeer(ctx context.Context, userID int64, messages []domain.Message) (bool, error) {
	seen := make(map[int64]struct{}, len(messages))
	for _, msg := range messages {
		if msg.Peer.Type != domain.PeerTypeUser || msg.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[msg.Peer.ID]; ok {
			continue
		}
		seen[msg.Peer.ID] = struct{}{}
		blocked, err := r.peerBlocksUser(ctx, userID, msg.Peer.ID)
		if err != nil {
			return false, err
		}
		if blocked {
			return true, nil
		}
	}
	return false, nil
}

func messageForwardErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	case errors.Is(err, domain.ErrChatForwardsRestricted):
		return chatForwardsRestrictedErr()
	case errors.Is(err, domain.ErrReplyMessageIDInvalid):
		return replyMessageIDInvalidErr()
	default:
		return internalErr()
	}
}

func (r *Router) messageReplyFromInput(ctx context.Context, userID int64, peer domain.Peer, input tg.InputReplyToClass) (*domain.MessageReply, error) {
	if input == nil {
		return nil, nil
	}
	reply, ok := input.(*tg.InputReplyToMessage)
	if !ok {
		switch input.(type) {
		case *tg.InputReplyToStory:
			return nil, storyIDInvalidErr()
		case *tg.InputReplyToMonoForum:
			return nil, replyToMonoforumPeerInvalidErr()
		default:
			return nil, inputConstructorInvalidErr()
		}
	}
	if _, ok := reply.GetMonoforumPeerID(); ok {
		return nil, replyToMonoforumPeerInvalidErr()
	}
	if _, ok := reply.GetTodoItemID(); ok {
		return nil, replyMessageIDInvalidErr()
	}
	if _, ok := reply.GetPollOption(); ok {
		return nil, pollOptionInvalidErr()
	}
	replyPeer := peer
	if inputPeer, ok := reply.GetReplyToPeerID(); ok {
		parsed, err := r.checkedDomainPeerFromInputPeer(ctx, userID, inputPeer)
		if err != nil || parsed != peer {
			return nil, replyMessageIDInvalidErr()
		}
		replyPeer = parsed
	}
	topMsgID, ok := reply.GetTopMsgID()
	if ok && (topMsgID < 0 || topMsgID > domain.MaxMessageBoxID) {
		return nil, replyMessageIDInvalidErr()
	}
	if reply.ReplyToMsgID < 0 || reply.ReplyToMsgID > domain.MaxMessageBoxID {
		return nil, replyMessageIDInvalidErr()
	}
	if reply.ReplyToMsgID == 0 && topMsgID == 0 {
		return nil, replyMessageIDInvalidErr()
	}
	quoteText, _ := reply.GetQuoteText()
	if utf8.RuneCountInString(quoteText) > maxReplyQuoteLength {
		return nil, limitInvalidErr()
	}
	quoteEntities, _ := reply.GetQuoteEntities()
	if len(quoteEntities) > maxMessageEntityCount {
		return nil, limitInvalidErr()
	}
	quoteOffset, ok := reply.GetQuoteOffset()
	if ok && (quoteOffset < 0 || quoteOffset > domain.MaxMessageReplyQuoteOffset) {
		return nil, replyMessageIDInvalidErr()
	}
	return &domain.MessageReply{
		MessageID:     reply.ReplyToMsgID,
		Peer:          replyPeer,
		TopMessageID:  topMsgID,
		QuoteText:     quoteText,
		QuoteEntities: domainMessageEntities(quoteEntities),
		QuoteOffset:   quoteOffset,
	}, nil
}

func sendMessageUnsupportedOptionErr(req *tg.MessagesSendMessageRequest) error {
	switch {
	case req.ReplyMarkup != nil:
		return replyMarkupInvalidErr()
	case req.QuickReplyShortcut != nil:
		return shortcutInvalidErr()
	case req.Effect != 0:
		return effectIDInvalidErr()
	case req.AllowPaidStars < 0:
		return starsAmountInvalidErr()
	case req.AllowPaidStars > 0 || req.AllowPaidFloodskip:
		return paymentUnsupportedErr()
	case !req.SuggestedPost.Zero():
		return suggestedPostPeerInvalidErr()
	default:
		return nil
	}
}

func forwardMessagesUnsupportedOptionErr(req *tg.MessagesForwardMessagesRequest) error {
	switch {
	case req.QuickReplyShortcut != nil:
		return shortcutInvalidErr()
	case req.Effect != 0:
		return effectIDInvalidErr()
	case req.VideoTimestamp != 0:
		return mediaInvalidErr()
	case req.AllowPaidStars < 0:
		return starsAmountInvalidErr()
	case req.AllowPaidStars > 0 || req.AllowPaidFloodskip:
		return paymentUnsupportedErr()
	case !req.SuggestedPost.Zero():
		return suggestedPostPeerInvalidErr()
	default:
		return nil
	}
}

func (r *Router) onMessagesGetPeerSettings(ctx context.Context, input tg.InputPeerClass) (*tg.MessagesPeerSettings, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input)
	if err != nil {
		return nil, err
	}
	settings := domain.PeerSettings{}
	if r.deps.Contacts != nil {
		settings, err = r.deps.Contacts.GetPeerSettings(ctx, userID, peer)
		if err != nil {
			return nil, internalErr()
		}
	}
	if r.deps.Dialogs != nil {
		hidden, err := r.deps.Dialogs.PeerSettingsBarHidden(ctx, userID, peer)
		if err != nil {
			return nil, internalErr()
		}
		settings.HiddenPeerSettingsBar = hidden
	}
	return &tg.MessagesPeerSettings{
		Settings: tgPeerSettings(settings),
		Users:    r.peerSettingsUsers(ctx, userID, input),
	}, nil
}

func (r *Router) onMessagesToggleDialogPin(ctx context.Context, req *tg.MessagesToggleDialogPinRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	peers, err := r.dialogPeersFromInput(ctx, userID, []tg.InputDialogPeerClass{req.Peer})
	if err != nil {
		return false, err
	}
	if len(peers) != 1 {
		return false, peerIDInvalidErr()
	}
	pinned := req.GetPinned()
	if r.deps.Dialogs == nil {
		return true, nil
	}
	changed, err := r.deps.Dialogs.TogglePinned(ctx, userID, peers[0], pinned)
	if err != nil {
		return false, internalErr()
	}
	if changed {
		date := int(r.clock.Now().Unix())
		if r.deps.Updates != nil {
			authKeyID, _ := AuthKeyIDFrom(ctx)
			sessionID, _ := SessionIDFrom(ctx)
			_, state, err := r.deps.Updates.RecordDialogPinned(ctx, authKeyID, userID, peers[0], pinned, sessionID)
			if err != nil {
				return false, internalErr()
			}
			date = state.Date
		}
		r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateDialogPinned{
				Pinned: pinned,
				Peer:   tgDialogPeer(peers[0]),
			}},
			Date: date,
			Seq:  0,
		})
	}
	return true, nil
}

func (r *Router) onMessagesReorderPinnedDialogs(ctx context.Context, req *tg.MessagesReorderPinnedDialogsRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if req.FolderID != 0 {
		return false, folderIDInvalidErr()
	}
	if len(req.Order) > maxDialogInputPeers {
		return false, limitInvalidErr()
	}
	peers, err := r.dialogPeersFromInput(ctx, userID, req.Order)
	if err != nil {
		return false, err
	}
	if r.deps.Dialogs == nil {
		return true, nil
	}
	if err := r.deps.Dialogs.ReorderPinned(ctx, userID, peers, req.GetForce()); err != nil {
		return false, internalErr()
	}
	date := int(r.clock.Now().Unix())
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		_, state, err := r.deps.Updates.RecordPinnedDialogs(ctx, authKeyID, userID, peers, sessionID)
		if err != nil {
			return false, internalErr()
		}
		date = state.Date
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePinnedDialogs{Order: tgDialogPeers(peers)}},
		Date:    date,
		Seq:     0,
	})
	return true, nil
}

func (r *Router) onMessagesMarkDialogUnread(ctx context.Context, req *tg.MessagesMarkDialogUnreadRequest) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if parentPeer, ok := req.GetParentPeer(); ok {
		if err := r.validateDialogUnreadParentPeer(ctx, userID, parentPeer); err != nil {
			return false, err
		}
		peers, err := r.dialogPeersFromInput(ctx, userID, []tg.InputDialogPeerClass{req.Peer})
		if err != nil {
			return false, err
		}
		if len(peers) != 1 {
			return false, peerIDInvalidErr()
		}
		return true, nil
	}
	peers, err := r.dialogPeersFromInput(ctx, userID, []tg.InputDialogPeerClass{req.Peer})
	if err != nil {
		return false, err
	}
	if len(peers) != 1 {
		return false, peerIDInvalidErr()
	}
	unread := req.GetUnread()
	if r.deps.Dialogs == nil {
		return true, nil
	}
	changed, err := r.deps.Dialogs.MarkUnread(ctx, userID, peers[0], unread)
	if err != nil {
		return false, internalErr()
	}
	if changed {
		date := int(r.clock.Now().Unix())
		if r.deps.Updates != nil {
			authKeyID, _ := AuthKeyIDFrom(ctx)
			sessionID, _ := SessionIDFrom(ctx)
			_, state, err := r.deps.Updates.RecordDialogUnreadMark(ctx, authKeyID, userID, peers[0], unread, sessionID)
			if err != nil {
				return false, internalErr()
			}
			date = state.Date
		}
		r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateDialogUnreadMark{
				Unread: unread,
				Peer:   tgDialogPeer(peers[0]),
			}},
			Date: date,
			Seq:  0,
		})
	}
	return true, nil
}

func (r *Router) onMessagesGetDialogUnreadMarks(ctx context.Context, req *tg.MessagesGetDialogUnreadMarksRequest) ([]tg.DialogPeerClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if parentPeer, ok := req.GetParentPeer(); ok {
		if err := r.validateDialogUnreadParentPeer(ctx, userID, parentPeer); err != nil {
			return nil, err
		}
		return []tg.DialogPeerClass{}, nil
	}
	if r.deps.Dialogs == nil {
		return nil, nil
	}
	peers, err := r.deps.Dialogs.UnreadMarks(ctx, userID)
	if err != nil {
		return nil, internalErr()
	}
	return tgDialogPeers(peers), nil
}

func (r *Router) validateDialogUnreadParentPeer(ctx context.Context, userID int64, parentPeer tg.InputPeerClass) error {
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, parentPeer)
	if err != nil {
		return parentPeerInvalidErr()
	}
	if peer.Type != domain.PeerTypeChannel {
		return parentPeerInvalidErr()
	}
	return nil
}

func (r *Router) onMessagesHidePeerSettingsBar(ctx context.Context, input tg.InputPeerClass) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, input)
	if err != nil {
		return false, err
	}
	changed := true
	if r.deps.Dialogs != nil {
		var err error
		changed, err = r.deps.Dialogs.HidePeerSettingsBar(ctx, userID, peer)
		if err != nil {
			return false, internalErr()
		}
	}
	if !changed {
		return true, nil
	}
	date := int(r.clock.Now().Unix())
	if r.deps.Updates != nil {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		sessionID, _ := SessionIDFrom(ctx)
		_, state, err := r.deps.Updates.RecordPeerSettings(ctx, authKeyID, userID, peer, domain.PeerSettings{HiddenPeerSettingsBar: true}, sessionID)
		if err != nil {
			return false, internalErr()
		}
		date = state.Date
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePeerSettings{
			Peer:     tgPeer(peer),
			Settings: tg.PeerSettings{},
		}},
		Date: date,
		Seq:  0,
	})
	return true, nil
}

func (r *Router) metrics() Metrics {
	if r.deps.Metrics == nil {
		return NopMetrics{}
	}
	return r.deps.Metrics
}

func (r *Router) dialogFilterFromRequest(ctx context.Context, userID int64, req *tg.MessagesGetDialogsRequest) (domain.DialogFilter, error) {
	limit := req.Limit
	if limit > 500 {
		limit = 500
	}
	filter := domain.DialogFilter{
		ExcludePinned: req.ExcludePinned,
		OffsetDate:    req.OffsetDate,
		OffsetID:      req.OffsetID,
		Limit:         limit,
		Hash:          req.Hash,
	}
	if folderID, ok := req.GetFolderID(); ok {
		filter.HasFolderID = true
		filter.FolderID = folderID
	}
	if peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.OffsetPeer); err == nil {
		filter.HasOffsetPeer = true
		filter.OffsetPeer = peer
	} else if _, ok := req.OffsetPeer.(*tg.InputPeerEmpty); !ok && req.OffsetPeer != nil {
		return domain.DialogFilter{}, err
	}
	return filter, nil
}

func (r *Router) dialogPeersFromInput(ctx context.Context, userID int64, items []tg.InputDialogPeerClass) ([]domain.Peer, error) {
	if len(items) > maxDialogInputPeers {
		return nil, limitInvalidErr()
	}
	peers := make([]domain.Peer, 0, len(items))
	hasFolder := false
	for _, item := range items {
		switch p := item.(type) {
		case *tg.InputDialogPeer:
			peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, p.Peer)
			if err != nil {
				return nil, err
			}
			peers = append(peers, peer)
		case *tg.InputDialogPeerFolder:
			if hasFolder {
				return nil, folderIDInvalidErr()
			}
			hasFolder = true
			// 第一阶段不维护 archived/folder 会话。若请求同时包含普通 peer，
			// 按 Telegram 企业版路径优先返回普通 peer；纯 folder 请求返回空摘要。
		default:
			return nil, inputConstructorInvalidErr()
		}
	}
	return peers, nil
}

func (r *Router) dialogFolderFromTG(ctx context.Context, userID int64, id int, filter tg.DialogFilterClass) (domain.DialogFolder, error) {
	if id < domain.DialogCustomFolderMinID {
		return domain.DialogFolder{}, filterIDInvalidErr()
	}
	switch f := filter.(type) {
	case *tg.DialogFilter:
		title := f.Title.Text
		if title == "" {
			return domain.DialogFolder{}, filterTitleEmptyErr()
		}
		if utf8.RuneCountInString(title) > domain.MaxDialogFolderTitleRunes {
			return domain.DialogFolder{}, limitInvalidErr()
		}
		pinned, err := r.dialogFolderPeersFromInput(ctx, userID, f.PinnedPeers)
		if err != nil {
			return domain.DialogFolder{}, err
		}
		include, err := r.dialogFolderPeersFromInput(ctx, userID, f.IncludePeers)
		if err != nil {
			return domain.DialogFolder{}, err
		}
		exclude, err := r.dialogFolderPeersFromInput(ctx, userID, f.ExcludePeers)
		if err != nil {
			return domain.DialogFolder{}, err
		}
		emoticon, hasEmoticon := f.GetEmoticon()
		color, hasColor := f.GetColor()
		return domain.DialogFolder{
			ID:              id,
			Contacts:        f.Contacts,
			NonContacts:     f.NonContacts,
			Groups:          f.Groups,
			Broadcasts:      f.Broadcasts,
			Bots:            f.Bots,
			ExcludeMuted:    f.ExcludeMuted,
			ExcludeRead:     f.ExcludeRead,
			ExcludeArchived: f.ExcludeArchived,
			TitleNoanimate:  f.TitleNoanimate,
			Title:           title,
			TitleEntities:   domainMessageEntities(f.Title.Entities),
			Emoticon:        emoticon,
			HasEmoticon:     hasEmoticon,
			Color:           color,
			HasColor:        hasColor,
			PinnedPeers:     pinned,
			IncludePeers:    include,
			ExcludePeers:    exclude,
		}, nil
	case *tg.DialogFilterChatlist:
		title := f.Title.Text
		if title == "" {
			return domain.DialogFolder{}, filterTitleEmptyErr()
		}
		if utf8.RuneCountInString(title) > domain.MaxDialogFolderTitleRunes {
			return domain.DialogFolder{}, limitInvalidErr()
		}
		pinned, err := r.dialogFolderPeersFromInput(ctx, userID, f.PinnedPeers)
		if err != nil {
			return domain.DialogFolder{}, err
		}
		include, err := r.dialogFolderPeersFromInput(ctx, userID, f.IncludePeers)
		if err != nil {
			return domain.DialogFolder{}, err
		}
		emoticon, hasEmoticon := f.GetEmoticon()
		color, hasColor := f.GetColor()
		return domain.DialogFolder{
			ID:             id,
			TitleNoanimate: f.TitleNoanimate,
			Title:          title,
			TitleEntities:  domainMessageEntities(f.Title.Entities),
			Emoticon:       emoticon,
			HasEmoticon:    hasEmoticon,
			Color:          color,
			HasColor:       hasColor,
			PinnedPeers:    pinned,
			IncludePeers:   include,
			IsChatlist:     true,
		}, nil
	default:
		return domain.DialogFolder{}, inputConstructorInvalidErr()
	}
}

func (r *Router) dialogFolderPeersFromInput(ctx context.Context, userID int64, peers []tg.InputPeerClass) ([]domain.DialogFolderPeer, error) {
	if len(peers) > domain.MaxDialogFolderPeers {
		return nil, limitInvalidErr()
	}
	out := make([]domain.DialogFolderPeer, 0, len(peers))
	seen := make(map[domain.Peer]struct{}, len(peers))
	for _, input := range peers {
		peer, accessHash, err := r.domainFolderPeerFromInputPeer(ctx, userID, input)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		out = append(out, domain.DialogFolderPeer{Peer: peer, AccessHash: accessHash})
	}
	return out, nil
}

func (r *Router) domainFolderPeerFromInputPeer(ctx context.Context, userID int64, peer tg.InputPeerClass) (domain.Peer, int64, error) {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return domain.Peer{Type: domain.PeerTypeUser, ID: p.UserID}, p.AccessHash, nil
	case *tg.InputPeerChannel:
		out := domain.Peer{Type: domain.PeerTypeChannel, ID: p.ChannelID}
		if err := r.validateInputPeerChannelAccess(ctx, userID, peer, p.ChannelID); err != nil {
			return domain.Peer{}, 0, err
		}
		return out, p.AccessHash, nil
	case *tg.InputPeerChannelFromMessage:
		out := domain.Peer{Type: domain.PeerTypeChannel, ID: p.ChannelID}
		if p.ChannelID <= 0 {
			return domain.Peer{}, 0, peerIDInvalidErr()
		}
		return out, 0, nil
	case *tg.InputPeerSelf:
		if userID == 0 {
			return domain.Peer{}, 0, peerIDInvalidErr()
		}
		var accessHash int64
		if r.deps.Users != nil {
			if self, err := r.deps.Users.Self(ctx, userID); err == nil {
				accessHash = self.AccessHash
			}
		}
		return domain.Peer{Type: domain.PeerTypeUser, ID: userID}, accessHash, nil
	default:
		return domain.Peer{}, 0, peerIDInvalidErr()
	}
}

func (r *Router) domainPeerFromInputPeer(userID int64, peer tg.InputPeerClass) (domain.Peer, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerEmpty, nil:
		return domain.Peer{}, false
	case *tg.InputPeerUser:
		return domain.Peer{Type: domain.PeerTypeUser, ID: p.UserID}, true
	case *tg.InputPeerChannel:
		return domain.Peer{Type: domain.PeerTypeChannel, ID: p.ChannelID}, true
	case *tg.InputPeerChannelFromMessage:
		return domain.Peer{Type: domain.PeerTypeChannel, ID: p.ChannelID}, p.ChannelID > 0
	case *tg.InputPeerChat:
		return domain.Peer{Type: domain.PeerTypeChannel, ID: p.ChatID}, p.ChatID > 0
	case *tg.InputPeerSelf:
		if userID == 0 {
			return domain.Peer{}, false
		}
		return domain.Peer{Type: domain.PeerTypeUser, ID: userID}, true
	default:
		return domain.Peer{}, false
	}
}

func isLegacyInputPeerChat(peer tg.InputPeerClass) bool {
	_, ok := peer.(*tg.InputPeerChat)
	return ok
}

func inputPeerChannelRef(peer tg.InputPeerClass) (channelInputRef, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return channelInputRef{
			ID:              p.ChannelID,
			AccessHash:      p.AccessHash,
			CheckAccessHash: p.AccessHash != 0,
		}, p.ChannelID > 0
	case *tg.InputPeerChannelFromMessage:
		return channelInputRef{ID: p.ChannelID}, p.ChannelID > 0
	default:
		return channelInputRef{}, false
	}
}

func (r *Router) validateInputPeerChannelAccess(ctx context.Context, userID int64, peer tg.InputPeerClass, channelID int64) error {
	ref, ok := inputPeerChannelRef(peer)
	if !ok || ref.ID != channelID || channelID <= 0 {
		return nil
	}
	if !ref.CheckAccessHash || r.deps.Channels == nil {
		return nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, channelID)
	if err != nil {
		return channelInvalidErr(err)
	}
	if !inputChannelAccessHashMatches(ref, view.Channel) {
		return channelInvalidErr(domain.ErrChannelPrivate)
	}
	return nil
}

func (r *Router) checkedDomainPeerFromInputPeer(ctx context.Context, userID int64, peer tg.InputPeerClass) (domain.Peer, error) {
	out, ok := r.domainPeerFromInputPeer(userID, peer)
	if !ok || out.ID == 0 {
		return domain.Peer{}, peerIDInvalidErr()
	}
	if out.Type == domain.PeerTypeChannel {
		if err := r.validateInputPeerChannelAccess(ctx, userID, peer, out.ID); err != nil {
			return domain.Peer{}, err
		}
	}
	return out, nil
}

func cleanDialogFilterOrder(order []int) []int {
	out := make([]int, 0, len(order))
	seen := make(map[int]struct{}, len(order))
	for _, id := range order {
		if id < domain.DialogCustomFolderMinID {
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

func (r *Router) messageFilterFromHistoryRequest(userID int64, req *tg.MessagesGetHistoryRequest) (domain.MessageFilter, bool) {
	peer, ok := r.domainPeerFromInputPeer(userID, req.Peer)
	if !ok {
		return domain.MessageFilter{}, false
	}
	limit := req.Limit
	if limit > 50 {
		limit = 50
	}
	return domain.MessageFilter{
		HasPeer:    true,
		Peer:       peer,
		OffsetID:   req.OffsetID,
		OffsetDate: req.OffsetDate,
		AddOffset:  domain.ClampMessageHistoryAddOffset(req.AddOffset),
		Limit:      limit,
		MaxID:      req.MaxID,
		MinID:      req.MinID,
		Hash:       req.Hash,
	}, true
}

func (r *Router) messageFilterFromSearchRequest(userID int64, req *tg.MessagesSearchRequest) domain.MessageFilter {
	limit := req.Limit
	if limit > 500 {
		limit = 500
	}
	filter := domain.MessageFilter{
		Query:          req.Q,
		OffsetID:       req.OffsetID,
		AddOffset:      domain.ClampMessageHistoryAddOffset(req.AddOffset),
		Limit:          limit,
		MaxID:          req.MaxID,
		MinID:          req.MinID,
		Hash:           req.Hash,
		NeedTotalCount: req.OffsetID == 0 && req.MinDate == 0 && req.MaxDate == 0 && req.AddOffset >= 0 && req.Hash == 0,
	}
	if peer, ok := r.domainPeerFromInputPeer(userID, req.Peer); ok {
		filter.HasPeer = true
		filter.Peer = peer
	}
	return filter
}

func (r *Router) channelHistoryFilterFromSearchRequest(userID int64, req *tg.MessagesSearchRequest, channelID int64) (domain.ChannelHistoryFilter, bool) {
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	filter := domain.ChannelHistoryFilter{
		ChannelID: channelID,
		Query:     req.Q,
		OffsetID:  req.OffsetID,
		AddOffset: domain.ClampMessageHistoryAddOffset(req.AddOffset),
		Limit:     limit,
		MinDate:   req.MinDate,
		MaxDate:   req.MaxDate,
		MaxID:     req.MaxID,
		MinID:     req.MinID,
		Hash:      req.Hash,
	}
	if req.FromID != nil {
		from, ok := r.domainPeerFromInputPeer(userID, req.FromID)
		if !ok || from.Type != domain.PeerTypeUser || from.ID == 0 {
			return domain.ChannelHistoryFilter{}, false
		}
		filter.SenderUserID = from.ID
	}
	return filter, true
}

func searchFilterNeedsMediaStore(filter tg.MessagesFilterClass) bool {
	switch filter.(type) {
	case nil, *tg.InputMessagesFilterEmpty:
		return false
	case *tg.InputMessagesFilterPhotos,
		*tg.InputMessagesFilterVideo,
		*tg.InputMessagesFilterPhotoVideo,
		*tg.InputMessagesFilterDocument,
		*tg.InputMessagesFilterURL,
		*tg.InputMessagesFilterGif,
		*tg.InputMessagesFilterVoice,
		*tg.InputMessagesFilterRoundVoice,
		*tg.InputMessagesFilterRoundVideo,
		*tg.InputMessagesFilterMusic,
		*tg.InputMessagesFilterPoll:
		return true
	default:
		return false
	}
}

func (r *Router) peerSettingsUsers(ctx context.Context, userID int64, peer tg.InputPeerClass) []tg.UserClass {
	if r.deps.Users == nil {
		return nil
	}
	switch p := peer.(type) {
	case *tg.InputPeerSelf:
		u, err := r.deps.Users.Self(ctx, userID)
		if err == nil && u.ID != 0 {
			return []tg.UserClass{r.tgSelfUser(u)}
		}
	case *tg.InputPeerUser:
		u, found, err := r.deps.Users.ByID(ctx, userID, p.UserID)
		if err == nil && found {
			return []tg.UserClass{r.tgUser(u)}
		}
	}
	return nil
}

func (r *Router) affectedMessages(ctx context.Context, authKeyID [8]byte, userID int64) (*tg.MessagesAffectedMessages, error) {
	st := domain.UpdateState{Date: int(r.clock.Now().Unix())}
	if r.deps.Updates != nil {
		var err error
		st, err = r.deps.Updates.GetState(ctx, authKeyID, userID)
		if err != nil {
			return nil, internalErr()
		}
	}
	return &tg.MessagesAffectedMessages{Pts: st.Pts, PtsCount: 0}, nil
}

func (r *Router) mentionedUserIDsFromMessage(ctx context.Context, currentUserID int64, message string, entities []tg.MessageEntityClass) ([]int64, error) {
	if r.deps.Users == nil {
		return nil, nil
	}
	identity, _ := r.deps.Users.(UserIdentityService)
	seen := make(map[int64]struct{})
	out := make([]int64, 0)
	add := func(id int64) {
		if id == 0 {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, entity := range entities {
		input, ok := entity.(*tg.InputMessageEntityMentionName)
		if !ok || input.UserID == nil {
			continue
		}
		user, found, err := r.userFromInput(ctx, currentUserID, input.UserID)
		if err != nil {
			return nil, internalErr()
		}
		if found {
			add(user.ID)
		}
		if len(out) >= domain.MaxChannelMentionRecipients {
			return out, nil
		}
	}
	if identity != nil {
		for _, username := range extractMentionUsernames(message, domain.MaxChannelMentionRecipients-len(out)) {
			user, found, err := identity.ResolveUsername(ctx, currentUserID, username)
			if err != nil {
				return nil, internalErr()
			}
			if found {
				add(user.ID)
			}
			if len(out) >= domain.MaxChannelMentionRecipients {
				return out, nil
			}
		}
	}
	return out, nil
}

func extractMentionUsernames(message string, limit int) []string {
	if limit <= 0 || message == "" {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for i := 0; i < len(message); i++ {
		if message[i] != '@' {
			continue
		}
		if i > 0 && isUsernameByte(message[i-1]) {
			continue
		}
		j := i + 1
		for j < len(message) && isUsernameByte(message[j]) {
			j++
		}
		if j == i+1 {
			continue
		}
		username := strings.ToLower(message[i+1 : j])
		if _, ok := seen[username]; ok {
			continue
		}
		seen[username] = struct{}{}
		out = append(out, username)
		if len(out) == limit {
			return out
		}
		i = j - 1
	}
	return out
}

func isUsernameByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func (r *Router) affectedHistory(ctx context.Context, authKeyID [8]byte, userID int64, offset int) (*tg.MessagesAffectedHistory, error) {
	st := domain.UpdateState{Date: int(r.clock.Now().Unix())}
	if r.deps.Updates != nil {
		var err error
		st, err = r.deps.Updates.GetState(ctx, authKeyID, userID)
		if err != nil {
			return nil, internalErr()
		}
	}
	return &tg.MessagesAffectedHistory{Pts: st.Pts, PtsCount: 0, Offset: offset}, nil
}

func (r *Router) chatsForInputPeer(ctx context.Context, userID int64, peer tg.InputPeerClass) []tg.ChatClass {
	p, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer)
	if err != nil || p.Type != domain.PeerTypeChannel || p.ID == 0 || r.deps.Channels == nil {
		return []tg.ChatClass{}
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, p.ID)
	if err != nil || view.Channel.ID == 0 {
		return []tg.ChatClass{}
	}
	return []tg.ChatClass{tgChannelChat(userID, view.Channel, &view.Self)}
}

func (r *Router) forumTopicPeerView(ctx context.Context, userID int64, peer tg.InputPeerClass) (domain.ChannelView, error) {
	p, err := r.checkedDomainPeerFromInputPeer(ctx, userID, peer)
	if err != nil {
		return domain.ChannelView{}, err
	}
	if p.Type != domain.PeerTypeChannel || p.ID == 0 {
		return domain.ChannelView{}, peerIDInvalidErr()
	}
	if r.deps.Channels == nil {
		return domain.ChannelView{}, nil
	}
	view, err := r.deps.Channels.GetChannel(ctx, userID, p.ID)
	if err != nil {
		return domain.ChannelView{}, channelInvalidErr(err)
	}
	return view, nil
}

func (r *Router) forumTopicsResponse(ctx context.Context, userID int64, view domain.ChannelView, list domain.ChannelForumTopicList, includeGeneral bool) *tg.MessagesForumTopics {
	if list.Channel.ID != 0 {
		view.Channel = list.Channel
	}
	if list.Dialog.ChannelID != 0 {
		view.Dialog = list.Dialog
	}
	channels := []domain.Channel{view.Channel}
	messages := []tg.MessageClass{}
	messageIDs := map[int]struct{}{}
	topics := []tg.ForumTopicClass{}
	userIDs := make([]int64, 0, len(list.Topics)+len(list.Messages)+1)
	count := list.Count
	if includeGeneral {
		count++
		topics = append(topics, tgForumGeneralTopic(userID, view))
		userIDs = append(userIDs, view.Channel.CreatorUserID)
		if view.Channel.TopMessageID > 0 && r.deps.Channels != nil {
			if history, err := r.deps.Channels.GetMessages(ctx, userID, view.Channel.ID, []int{view.Channel.TopMessageID}); err == nil {
				for _, msg := range history.Messages {
					if _, ok := messageIDs[msg.ID]; ok {
						continue
					}
					messageIDs[msg.ID] = struct{}{}
					if item := tgChannelMessage(userID, msg); item != nil {
						messages = append(messages, item)
					}
					userIDs = append(userIDs, msg.SenderUserID)
				}
				channels = append(channels, history.Channels...)
				for _, u := range history.Users {
					userIDs = append(userIDs, u.ID)
				}
			}
		}
	}
	for _, topic := range list.Topics {
		topics = append(topics, tgForumTopicFromDomain(userID, topic))
		userIDs = append(userIDs, topic.CreatorUserID)
	}
	for _, msg := range list.Messages {
		if _, ok := messageIDs[msg.ID]; ok {
			continue
		}
		messageIDs[msg.ID] = struct{}{}
		if item := tgChannelMessage(userID, msg); item != nil {
			messages = append(messages, item)
		}
		userIDs = append(userIDs, msg.SenderUserID)
		if msg.SendAs != nil && msg.SendAs.Type == domain.PeerTypeUser {
			userIDs = append(userIDs, msg.SendAs.ID)
		}
	}
	for _, u := range list.Users {
		userIDs = append(userIDs, u.ID)
	}
	return &tg.MessagesForumTopics{
		Count:    count,
		Topics:   topics,
		Messages: messages,
		Chats:    tgChannels(userID, channels),
		Users:    r.tgUsersForIDs(ctx, userID, userIDs),
		Pts:      view.Channel.Pts,
	}
}

func tgForumGeneralTopic(viewerUserID int64, view domain.ChannelView) *tg.ForumTopic {
	return &tg.ForumTopic{
		My:                  view.Channel.CreatorUserID == viewerUserID && viewerUserID != 0,
		ID:                  forumGeneralTopicID,
		Date:                view.Channel.Date,
		Peer:                &tg.PeerChannel{ChannelID: view.Channel.ID},
		Title:               "General",
		IconColor:           forumGeneralIconColor,
		TopMessage:          view.Channel.TopMessageID,
		ReadInboxMaxID:      view.Dialog.ReadInboxMaxID,
		ReadOutboxMaxID:     view.Dialog.ReadOutboxMaxID,
		UnreadCount:         view.Dialog.UnreadCount,
		UnreadMentionsCount: view.Dialog.UnreadMentions,
		FromID:              &tg.PeerUser{UserID: view.Channel.CreatorUserID},
		NotifySettings:      *tdesktop.NotifySettings(),
	}
}

func tgForumTopicFromDomain(viewerUserID int64, topic domain.ChannelForumTopic) *tg.ForumTopic {
	iconColor := topic.IconColor
	if iconColor == 0 {
		iconColor = domain.DefaultForumTopicIconColor
	}
	return &tg.ForumTopic{
		My:                   topic.CreatorUserID == viewerUserID && viewerUserID != 0,
		Closed:               topic.Closed,
		Pinned:               topic.Pinned,
		Hidden:               topic.Hidden,
		TitleMissing:         topic.TitleMissing,
		ID:                   topic.TopicID,
		Date:                 topic.Date,
		Peer:                 &tg.PeerChannel{ChannelID: topic.ChannelID},
		Title:                topic.Title,
		IconColor:            iconColor,
		IconEmojiID:          topic.IconEmojiID,
		TopMessage:           topic.TopMessageID,
		ReadInboxMaxID:       topic.ReadInboxMaxID,
		ReadOutboxMaxID:      topic.ReadOutboxMaxID,
		UnreadCount:          topic.UnreadCount,
		UnreadMentionsCount:  topic.UnreadMentionsCount,
		UnreadReactionsCount: topic.UnreadReactionsCount,
		UnreadPollVotesCount: topic.UnreadPollVotesCount,
		FromID:               &tg.PeerUser{UserID: topic.CreatorUserID},
		NotifySettings:       *tdesktop.NotifySettings(),
	}
}

func (r *Router) pinnedForumTopicUpdates(viewerUserID int64, channel domain.Channel, topicID int, pinned bool) *tg.Updates {
	update := &tg.UpdatePinnedForumTopic{
		Peer:    &tg.PeerChannel{ChannelID: channel.ID},
		TopicID: topicID,
	}
	update.SetPinned(pinned)
	return &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func (r *Router) pinnedForumTopicsOrderUpdates(viewerUserID int64, channel domain.Channel, order []int) *tg.Updates {
	update := &tg.UpdatePinnedForumTopics{
		Peer:  &tg.PeerChannel{ChannelID: channel.ID},
		Order: append([]int(nil), order...),
	}
	if order != nil {
		update.SetOrder(append([]int(nil), order...))
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Chats:   []tg.ChatClass{tgChannelChat(viewerUserID, channel, nil)},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

func forumTopicQueryMatchesGeneral(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	return query == "" || strings.Contains("general", query)
}

func (r *Router) pushReadHistoryEvent(ctx context.Context, userID int64, event domain.UpdateEvent) {
	if r.hasReliableUpdateDispatch() {
		return
	}
	if r.deps.Sessions == nil || userID == 0 {
		return
	}
	var update tg.UpdateClass
	switch event.Type {
	case domain.UpdateEventReadHistoryInbox:
		update = tgReadHistoryInboxUpdate(event)
	case domain.UpdateEventReadHistoryOutbox:
		update = tgReadHistoryOutboxUpdate(event)
	}
	if update == nil {
		return
	}
	updates := &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Date:    event.Date,
		Seq:     0,
	}
	r.pushUserMessage(ctx, userID, "push read history", updates)
}

func tgReadHistoryInbox(event domain.UpdateEvent) *tg.UpdateReadHistoryInbox {
	peer := tgPeer(event.Peer)
	if peer == nil {
		return nil
	}
	return &tg.UpdateReadHistoryInbox{
		Peer:             peer,
		MaxID:            event.MaxID,
		StillUnreadCount: event.StillUnreadCount,
		Pts:              event.Pts,
		PtsCount:         event.PtsCount,
	}
}

func tgReadHistoryInboxUpdate(event domain.UpdateEvent) tg.UpdateClass {
	if event.Peer.Type == domain.PeerTypeChannel && event.Peer.ID != 0 {
		return &tg.UpdateReadChannelInbox{
			ChannelID:        event.Peer.ID,
			MaxID:            event.MaxID,
			StillUnreadCount: event.StillUnreadCount,
			Pts:              event.Pts,
		}
	}
	return tgReadHistoryInbox(event)
}

func tgReadHistoryOutbox(event domain.UpdateEvent) *tg.UpdateReadHistoryOutbox {
	peer := tgPeer(event.Peer)
	if peer == nil {
		return nil
	}
	return &tg.UpdateReadHistoryOutbox{
		Peer:     peer,
		MaxID:    event.MaxID,
		Pts:      event.Pts,
		PtsCount: event.PtsCount,
	}
}

func tgReadHistoryOutboxUpdate(event domain.UpdateEvent) tg.UpdateClass {
	if event.Peer.Type == domain.PeerTypeChannel && event.Peer.ID != 0 {
		return &tg.UpdateReadChannelOutbox{
			ChannelID: event.Peer.ID,
			MaxID:     event.MaxID,
		}
	}
	return tgReadHistoryOutbox(event)
}

func messagesNotModifiedOrEmpty(hash int64) tg.MessagesMessagesClass {
	if hash != 0 {
		return &tg.MessagesMessagesNotModified{Count: 0}
	}
	return &tg.MessagesMessages{}
}

func messagesAllStickersEmpty(hash int64) tg.MessagesAllStickersClass {
	if hash != 0 {
		return &tg.MessagesAllStickersNotModified{}
	}
	return &tg.MessagesAllStickers{Sets: []tg.StickerSet{}}
}

func messagesFeaturedStickersEmpty(hash int64) tg.MessagesFeaturedStickersClass {
	if hash != 0 {
		return &tg.MessagesFeaturedStickersNotModified{Count: 0}
	}
	return &tg.MessagesFeaturedStickers{
		Count:  0,
		Sets:   []tg.StickerSetCoveredClass{},
		Unread: []int64{},
	}
}

func tgEditMessageUpdates(event domain.UpdateEvent, msg domain.Message, users []tg.UserClass, chats []tg.ChatClass) *tg.Updates {
	update := tgOtherUpdateFromEvent(domain.UpdateEvent{
		Type:     domain.UpdateEventEditMessage,
		Pts:      event.Pts,
		PtsCount: event.PtsCount,
		Message:  msg,
	})
	if update == nil {
		return &tg.Updates{Date: event.Date}
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Users:   users,
		Chats:   chats,
		Date:    event.Date,
		Seq:     0,
	}
}

func tgPrivateMessageUpdates(event domain.UpdateEvent, msg domain.Message, randomID int64, includeMessageID bool, users []tg.UserClass, chats []tg.ChatClass) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, 2)
	if includeMessageID {
		updates = append(updates, &tg.UpdateMessageID{ID: msg.ID, RandomID: randomID})
	}
	item := tgMessage(msg)
	if item == nil {
		item = &tg.MessageEmpty{ID: msg.ID}
	}
	updates = append(updates, &tg.UpdateNewMessage{
		Message:  item,
		Pts:      event.Pts,
		PtsCount: event.PtsCount,
	})
	date := event.Date
	if date == 0 {
		date = msg.Date
	}
	return &tg.Updates{
		Updates: updates,
		Users:   users,
		Chats:   chats,
		Date:    date,
		Seq:     0, // 私聊不维护账号级 seq，恒 0（客户端仅靠 pts 同步）
	}
}

func tgForwardMessagesUpdates(res domain.ForwardPrivateMessagesResult, randomIDs []int64, users []tg.UserClass, chats []tg.ChatClass) *tg.Updates {
	updates := make([]tg.UpdateClass, 0, len(res.SenderMessages)*2)
	date := 0
	for i, msg := range res.SenderMessages {
		randomID := int64(0)
		if i < len(randomIDs) {
			randomID = randomIDs[i]
		}
		updates = append(updates, &tg.UpdateMessageID{ID: msg.ID, RandomID: randomID})
		event := domain.UpdateEvent{}
		if i < len(res.SenderEvents) {
			event = res.SenderEvents[i]
		}
		item := tgMessage(msg)
		if item == nil {
			item = &tg.MessageEmpty{ID: msg.ID}
		}
		pts := event.Pts
		if pts == 0 {
			pts = msg.Pts
		}
		ptsCount := event.PtsCount
		if ptsCount == 0 {
			ptsCount = 1
		}
		updates = append(updates, &tg.UpdateNewMessage{
			Message:  item,
			Pts:      pts,
			PtsCount: ptsCount,
		})
		if date == 0 {
			date = event.Date
		}
		if date == 0 {
			date = msg.Date
		}
	}
	return &tg.Updates{
		Updates: updates,
		Users:   users,
		Chats:   chats,
		Date:    date,
		Seq:     0,
	}
}

func (r *Router) usersForMessageUpdate(ctx context.Context, ownerUserID int64, msg domain.Message) []tg.UserClass {
	seen := make(map[int64]struct{}, 2)
	users := make([]tg.UserClass, 0, 2)
	add := func(id int64) {
		if id == 0 {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		switch {
		case id == domain.OfficialSystemUserID:
			users = append(users, r.tgUser(domain.OfficialSystemUser()))
		case id == ownerUserID:
			if r.deps.Users == nil {
				return
			}
			u, err := r.deps.Users.Self(ctx, ownerUserID)
			if err == nil && u.ID != 0 {
				users = append(users, r.tgSelfUser(u))
			}
		default:
			if r.deps.Users == nil {
				return
			}
			u, found, err := r.deps.Users.ByID(ctx, ownerUserID, id)
			if err == nil && found {
				users = append(users, r.tgUser(u))
			}
		}
	}
	if msg.From.Type == domain.PeerTypeUser {
		add(msg.From.ID)
	}
	if msg.Peer.Type == domain.PeerTypeUser {
		add(msg.Peer.ID)
	}
	if msg.Forward != nil && msg.Forward.From.Type == domain.PeerTypeUser {
		add(msg.Forward.From.ID)
	}
	if msg.ReplyTo != nil && msg.ReplyTo.Peer.Type == domain.PeerTypeUser {
		add(msg.ReplyTo.Peer.ID)
	}
	return users
}

func (r *Router) usersForMessageUpdates(ctx context.Context, ownerUserID int64, messages []domain.Message) []tg.UserClass {
	seen := make(map[int64]struct{}, len(messages)*2)
	ids := make([]int64, 0, len(messages)*2)
	addID := func(id int64) {
		if id == 0 {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, msg := range messages {
		if msg.From.Type == domain.PeerTypeUser {
			addID(msg.From.ID)
		}
		if msg.Peer.Type == domain.PeerTypeUser {
			addID(msg.Peer.ID)
		}
		if msg.Forward != nil && msg.Forward.From.Type == domain.PeerTypeUser {
			addID(msg.Forward.From.ID)
		}
		if msg.ReplyTo != nil && msg.ReplyTo.Peer.Type == domain.PeerTypeUser {
			addID(msg.ReplyTo.Peer.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	loaded := make(map[int64]domain.User, len(ids))
	if r.deps.Users != nil {
		if users, err := r.deps.Users.ByIDs(ctx, ownerUserID, ids); err == nil {
			for _, user := range users {
				loaded[user.ID] = user
			}
		}
	}
	users := make([]tg.UserClass, 0, len(ids))
	for _, id := range ids {
		switch {
		case id == domain.OfficialSystemUserID:
			users = append(users, r.tgUser(domain.OfficialSystemUser()))
		case id == ownerUserID:
			if user, ok := loaded[id]; ok {
				users = append(users, r.tgSelfUser(user))
			}
		default:
			if user, ok := loaded[id]; ok {
				users = append(users, r.tgUser(user))
			}
		}
	}
	return users
}

func (r *Router) chatsForMessageUpdate(ctx context.Context, ownerUserID int64, msg domain.Message) []tg.ChatClass {
	if r.deps.Channels == nil {
		return nil
	}
	seen := make(map[int64]struct{}, 2)
	chats := make([]tg.ChatClass, 0, 2)
	add := func(id int64) {
		if id == 0 {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		view, err := r.deps.Channels.GetChannel(ctx, ownerUserID, id)
		if err != nil || view.Channel.ID == 0 {
			return
		}
		chats = append(chats, tgChannelChat(ownerUserID, view.Channel, &view.Self))
	}
	if msg.From.Type == domain.PeerTypeChannel {
		add(msg.From.ID)
	}
	if msg.Peer.Type == domain.PeerTypeChannel {
		add(msg.Peer.ID)
	}
	if msg.Forward != nil && msg.Forward.From.Type == domain.PeerTypeChannel {
		add(msg.Forward.From.ID)
	}
	if msg.ReplyTo != nil && msg.ReplyTo.Peer.Type == domain.PeerTypeChannel {
		add(msg.ReplyTo.Peer.ID)
	}
	return chats
}

func (r *Router) chatsForMessageUpdates(ctx context.Context, ownerUserID int64, messages []domain.Message) []tg.ChatClass {
	seen := make(map[int64]struct{}, len(messages))
	chats := make([]tg.ChatClass, 0, len(messages))
	for _, msg := range messages {
		for _, chat := range r.chatsForMessageUpdate(ctx, ownerUserID, msg) {
			channel, ok := chat.(*tg.Channel)
			if !ok || channel.ID == 0 {
				continue
			}
			if _, ok := seen[channel.ID]; ok {
				continue
			}
			seen[channel.ID] = struct{}{}
			chats = append(chats, chat)
		}
	}
	return chats
}
