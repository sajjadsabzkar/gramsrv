package rpc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// 本文件实现 messages.uploadMedia / sendMedia / sendMultiMedia 的 photo/document/sticker 主路径，
// 并抽取 sendOutgoing 作为「已校验的一条出站消息（文本或媒体）落地」的共享实现，私聊与频道共用。

// outgoingSend 是 sendOutgoing 的入参：一条已校验的出站消息。
type outgoingSend struct {
	randomID     int64
	message      string
	entities     []tg.MessageEntityClass
	media        *domain.MessageMedia
	silent       bool
	noforwards   bool
	replyToInput tg.InputReplyToClass
	sendAsInput  tg.InputPeerClass
	clearDraft   bool
}

// sendOutgoing 把一条出站消息落地到私聊或频道，返回 *tg.Updates、是否重复、错误。
// media 为空即纯文本。校验（长度/random_id/限流）由调用方完成。
func (r *Router) sendOutgoing(ctx context.Context, userID int64, peer domain.Peer, p outgoingSend) (tg.UpdatesClass, bool, error) {
	sendAs, err := r.resolveSendAsPeer(ctx, userID, peer, p.sendAsInput)
	if err != nil {
		return nil, false, err
	}
	if peer.Type == domain.PeerTypeChannel {
		if r.deps.Channels == nil {
			return nil, false, peerIDInvalidErr()
		}
		replyTo, err := r.messageReplyFromInput(ctx, userID, peer, p.replyToInput)
		if err != nil {
			return nil, false, err
		}
		mentionUserIDs, err := r.mentionedUserIDsFromMessage(ctx, userID, p.message, p.entities)
		if err != nil {
			return nil, false, err
		}
		res, err := r.deps.Channels.SendMessage(ctx, userID, domain.SendChannelMessageRequest{
			UserID:         userID,
			ChannelID:      peer.ID,
			RandomID:       p.randomID,
			Message:        p.message,
			Entities:       domainMessageEntities(p.entities),
			Media:          p.media,
			MentionUserIDs: mentionUserIDs,
			Silent:         p.silent,
			NoForwards:     p.noforwards,
			ReplyTo:        replyTo,
			SendAs:         sendAs,
			Date:           int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, false, channelInvalidErr(err)
		}
		updates := r.channelMessageUpdates(ctx, userID, res, p.randomID)
		if !res.Duplicate {
			r.pushChannelUpdates(ctx, userID, res.Channel.ID, res.Recipients, func(viewerUserID int64) *tg.Updates {
				return r.channelMessageUpdates(ctx, viewerUserID, res, 0)
			})
			r.pushChannelDiscussionUpdate(ctx, userID, res.Discussion)
		}
		if p.clearDraft {
			r.clearDraftAfterSend(ctx, userID, peer, replyTo)
		}
		return updates, res.Duplicate, nil
	}
	if peer.Type != domain.PeerTypeUser {
		return nil, false, peerIDInvalidErr()
	}
	if r.deps.Messages == nil {
		return nil, false, peerIDInvalidErr()
	}
	if r.deps.Users != nil && peer.ID != userID {
		if _, found, err := r.deps.Users.ByID(ctx, userID, peer.ID); err != nil {
			return nil, false, internalErr()
		} else if !found {
			return nil, false, peerIDInvalidErr()
		}
	}
	replyTo, err := r.messageReplyFromInput(ctx, userID, peer, p.replyToInput)
	if err != nil {
		return nil, false, err
	}
	sessionID, _ := SessionIDFrom(ctx)
	authKeyID, _ := AuthKeyIDFrom(ctx)
	res, err := r.deps.Messages.SendPrivateText(ctx, userID, domain.SendPrivateTextRequest{
		SenderUserID:    userID,
		RecipientUserID: peer.ID,
		RandomID:        p.randomID,
		Message:         p.message,
		Entities:        domainMessageEntities(p.entities),
		Media:           p.media,
		Silent:          p.silent,
		NoForwards:      p.noforwards,
		ReplyTo:         replyTo,
		Date:            int(r.clock.Now().Unix()),
		OriginAuthKeyID: authKeyID,
		OriginSessionID: sessionID,
	})
	if err != nil {
		return nil, false, messageSendErr(err)
	}
	users := r.usersForMessageUpdate(ctx, userID, res.SenderMessage)
	chats := r.chatsForMessageUpdate(ctx, userID, res.SenderMessage)
	if p.clearDraft {
		r.clearDraftAfterSend(ctx, userID, peer, replyTo)
	}
	return tgPrivateMessageUpdates(res.SenderEvent, res.SenderMessage, p.randomID, true, users, chats), res.Duplicate, nil
}

// onMessagesUploadMedia 解析 InputMedia（上传或引用），返回可复用的 tg.MessageMedia。
func (r *Router) onMessagesUploadMedia(ctx context.Context, req *tg.MessagesUploadMediaRequest) (tg.MessageMediaClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, mediaInvalidErr()
	}
	if len(req.BusinessConnectionID) > maxBusinessConnIDLength {
		return nil, limitInvalidErr()
	}
	if _, ok := req.Peer.(*tg.InputPeerEmpty); !ok {
		if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
			return nil, err
		}
	}
	if _, ok := req.Media.(*tg.InputMediaEmpty); ok {
		return &tg.MessageMediaEmpty{}, nil
	}
	media, err := r.resolveInputMedia(ctx, userID, req.Media)
	if err != nil {
		return nil, err
	}
	if media == nil {
		return nil, mediaInvalidErr()
	}
	return tgMessageMedia(media), nil
}

// onMessagesSendMedia 发送一条带媒体的消息（photo/document/sticker），私聊与频道均支持。
func (r *Router) onMessagesSendMedia(ctx context.Context, req *tg.MessagesSendMediaRequest) (tg.UpdatesClass, error) {
	if req.RandomID == 0 {
		return nil, randomIDEmptyErr()
	}
	if utf8.RuneCountInString(req.Message) > maxSendMessageTextLength {
		return nil, mediaCaptionTooLongErr()
	}
	if len(req.Entities) > maxMessageEntityCount {
		return nil, limitInvalidErr()
	}
	if req.ScheduleDate != 0 || req.ScheduleRepeatPeriod != 0 {
		return nil, scheduleDateInvalidErr()
	}
	if req.Media == nil {
		return nil, mediaInvalidErr()
	}
	// InputMediaEmpty / WebPage：退化为纯文本发送（复用 sendMessage 校验与流程）。
	switch req.Media.(type) {
	case *tg.InputMediaEmpty, *tg.InputMediaWebPage:
		return r.onMessagesSendMessage(ctx, sendMessageRequestFromSendMedia(req))
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, peerIDInvalidErr()
	}
	if r.deps.Limiter != nil {
		allowed, retryAfter, err := r.deps.Limiter.Allow(ctx, "messages:send:"+strconv.FormatInt(userID, 10), sendMessageRateLimit, sendMessageRateWindow)
		if err != nil {
			return nil, internalErr()
		}
		if !allowed {
			r.metrics().MessageRateLimited(retryAfter)
			return nil, floodWaitErr(retryAfter)
		}
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	media, err := r.resolveInputMedia(ctx, userID, req.Media)
	if err != nil {
		return nil, err
	}
	if media == nil {
		return nil, mediaInvalidErr()
	}
	updates, _, err := r.sendOutgoing(ctx, userID, peer, outgoingSend{
		randomID:     req.RandomID,
		message:      req.Message,
		entities:     req.Entities,
		media:        media,
		silent:       req.Silent,
		noforwards:   req.Noforwards,
		replyToInput: req.ReplyTo,
		sendAsInput:  req.SendAs,
		clearDraft:   req.ClearDraft,
	})
	if err != nil {
		return nil, err
	}
	return updates, nil
}

// onMessagesSendMultiMedia 发送相册（多条媒体）。本阶段不绑定 grouped_id（各条作为独立消息呈现）。
func (r *Router) onMessagesSendMultiMedia(ctx context.Context, req *tg.MessagesSendMultiMediaRequest) (tg.UpdatesClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if userID == 0 {
		return nil, peerIDInvalidErr()
	}
	if len(req.MultiMedia) == 0 || len(req.MultiMedia) > maxSendMultiMediaItems {
		return nil, limitInvalidErr()
	}
	if req.ScheduleDate != 0 {
		return nil, scheduleDateInvalidErr()
	}
	peer, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer)
	if err != nil {
		return nil, err
	}
	for _, item := range req.MultiMedia {
		if item.RandomID == 0 {
			return nil, randomIDEmptyErr()
		}
		if utf8.RuneCountInString(item.Message) > maxSendMessageTextLength {
			return nil, mediaCaptionTooLongErr()
		}
		if len(item.Entities) > maxMessageEntityCount {
			return nil, limitInvalidErr()
		}
		if item.Media == nil {
			return nil, mediaInvalidErr()
		}
	}

	combined := make([]tg.UpdateClass, 0, len(req.MultiMedia)*2)
	usersByID := map[int64]tg.UserClass{}
	chatsByID := map[int64]tg.ChatClass{}
	date := 0
	for _, item := range req.MultiMedia {
		media, err := r.resolveInputMedia(ctx, userID, item.Media)
		if err != nil {
			return nil, err
		}
		if media == nil {
			return nil, mediaInvalidErr()
		}
		result, _, err := r.sendOutgoing(ctx, userID, peer, outgoingSend{
			randomID:     item.RandomID,
			message:      item.Message,
			entities:     item.Entities,
			media:        media,
			silent:       req.Silent,
			noforwards:   req.Noforwards,
			replyToInput: req.ReplyTo,
			sendAsInput:  req.SendAs,
		})
		if err != nil {
			return nil, err
		}
		if upd, ok := result.(*tg.Updates); ok {
			combined = append(combined, upd.Updates...)
			for _, u := range upd.Users {
				if id := userClassID(u); id != 0 {
					usersByID[id] = u
				}
			}
			for _, c := range upd.Chats {
				if id := chatClassID(c); id != 0 {
					chatsByID[id] = c
				}
			}
			if upd.Date != 0 {
				date = upd.Date
			}
		}
	}
	return &tg.Updates{
		Updates: combined,
		Users:   mapValuesUsers(usersByID),
		Chats:   mapValuesChats(chatsByID),
		Date:    date,
	}, nil
}

// resolveInputMedia 把 tg.InputMedia 解析为 domain.MessageMedia（上传则落库，引用则加载）。
// 返回 nil 表示 InputMediaEmpty（调用方退化为纯文本）。
func (r *Router) resolveInputMedia(ctx context.Context, userID int64, input tg.InputMediaClass) (*domain.MessageMedia, error) {
	if r.deps.Files == nil {
		return nil, mediaInvalidErr()
	}
	switch in := input.(type) {
	case *tg.InputMediaEmpty:
		return nil, nil
	case *tg.InputMediaUploadedPhoto:
		if in.File == nil {
			return nil, mediaInvalidErr()
		}
		ref, ok := uploadedFileRef(userID, in.File)
		if !ok {
			return nil, fileReferenceInvalidErr()
		}
		photo, err := r.deps.Files.CreatePhotoFromUpload(ctx, ref)
		if err != nil {
			return nil, mediaUploadErr(err)
		}
		return &domain.MessageMedia{Kind: domain.MessageMediaKindPhoto, Photo: &photo, Spoiler: in.Spoiler, TTLSeconds: in.TTLSeconds}, nil
	case *tg.InputMediaUploadedDocument:
		if in.File == nil {
			return nil, mediaInvalidErr()
		}
		ref, ok := uploadedFileRef(userID, in.File)
		if !ok {
			return nil, fileReferenceInvalidErr()
		}
		spec := domain.DocumentSpec{
			MimeType:   in.MimeType,
			Attributes: domainDocumentAttributes(in.Attributes),
			ForceFile:  in.ForceFile,
		}
		if thumb, ok := in.GetThumb(); ok {
			if tref, ok := uploadedFileRef(userID, thumb); ok {
				spec.Thumb = &tref
			}
		}
		doc, err := r.deps.Files.CreateDocumentFromUpload(ctx, ref, spec)
		if err != nil {
			return nil, mediaUploadErr(err)
		}
		return messageMediaFromDocument(doc, in.Spoiler, in.TTLSeconds), nil
	case *tg.InputMediaPhoto:
		photoID, ok := inputPhotoID(in.ID)
		if !ok {
			return nil, photoInvalidErr()
		}
		photo, found, err := r.deps.Files.GetPhoto(ctx, photoID)
		if err != nil {
			return nil, internalErr()
		}
		if !found {
			return nil, photoInvalidErr()
		}
		return &domain.MessageMedia{Kind: domain.MessageMediaKindPhoto, Photo: &photo, Spoiler: in.Spoiler, TTLSeconds: in.TTLSeconds}, nil
	case *tg.InputMediaDocument:
		docIDs, ok := inputDocumentCandidateIDs(in.ID)
		if !ok {
			r.log.Warn("sendMedia InputMediaDocument unresolvable id", zap.String("id_type", fmt.Sprintf("%T", in.ID)))
			return nil, mediaInvalidErr()
		}
		var doc domain.Document
		found := false
		for _, docID := range docIDs {
			var err error
			doc, found, err = r.deps.Files.GetDocument(ctx, docID)
			if err != nil {
				return nil, internalErr()
			}
			if found {
				break
			}
		}
		if !found {
			r.log.Warn("sendMedia references unknown document", zap.Int64s("doc_ids", docIDs), zap.Int64("user_id", userID))
			return nil, mediaInvalidErr()
		}
		return messageMediaFromDocument(doc, in.Spoiler, in.TTLSeconds), nil
	default:
		// geo / contact / poll / venue / dice / story / 等本阶段不支持。
		return nil, mediaInvalidErr()
	}
}

// messageMediaFromDocument 由 Document 构造 MessageMedia，并从属性推导 Video/Round/Voice 标志。
func messageMediaFromDocument(doc domain.Document, spoiler bool, ttl int) *domain.MessageMedia {
	media := &domain.MessageMedia{Kind: domain.MessageMediaKindDocument, Document: &doc, Spoiler: spoiler, TTLSeconds: ttl}
	for _, attr := range doc.Attributes {
		switch attr.Kind {
		case domain.DocAttrVideo:
			media.Video = true
			if attr.RoundMessage {
				media.Round = true
			}
		case domain.DocAttrAudio:
			if attr.Voice {
				media.Voice = true
			}
		}
	}
	return media
}

func inputPhotoID(input tg.InputPhotoClass) (int64, bool) {
	if p, ok := input.(*tg.InputPhoto); ok && p.ID != 0 {
		return p.ID, true
	}
	return 0, false
}

func inputDocumentID(input tg.InputDocumentClass) (int64, bool) {
	if d, ok := input.(*tg.InputDocument); ok && d.ID != 0 {
		return d.ID, true
	}
	return 0, false
}

func inputDocumentCandidateIDs(input tg.InputDocumentClass) ([]int64, bool) {
	if d, ok := input.(*tg.InputDocument); ok && d.ID != 0 {
		return []int64{d.ID}, true
	}
	return nil, false
}

// domainDocumentAttributes 把 tg.DocumentAttribute 反向转为 domain（InputMediaUploadedDocument 用）。
func domainDocumentAttributes(attrs []tg.DocumentAttributeClass) []domain.DocumentAttribute {
	out := make([]domain.DocumentAttribute, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.(type) {
		case *tg.DocumentAttributeImageSize:
			out = append(out, domain.DocumentAttribute{Kind: domain.DocAttrImageSize, W: v.W, H: v.H})
		case *tg.DocumentAttributeAnimated:
			out = append(out, domain.DocumentAttribute{Kind: domain.DocAttrAnimated})
		case *tg.DocumentAttributeSticker:
			attr := domain.DocumentAttribute{Kind: domain.DocAttrSticker, Alt: v.Alt, Mask: v.Mask}
			if id, hash, ok := inputStickerSetIDs(v.Stickerset); ok {
				attr.StickerSetID = id
				attr.StickerSetAccessHash = hash
			}
			out = append(out, attr)
		case *tg.DocumentAttributeVideo:
			out = append(out, domain.DocumentAttribute{Kind: domain.DocAttrVideo, W: v.W, H: v.H, Duration: v.Duration, RoundMessage: v.RoundMessage, SupportsStreaming: v.SupportsStreaming})
		case *tg.DocumentAttributeAudio:
			out = append(out, domain.DocumentAttribute{Kind: domain.DocAttrAudio, AudioDuration: v.Duration, Voice: v.Voice, Title: v.Title, Performer: v.Performer, Waveform: v.Waveform})
		case *tg.DocumentAttributeFilename:
			out = append(out, domain.DocumentAttribute{Kind: domain.DocAttrFilename, FileName: v.FileName})
		case *tg.DocumentAttributeCustomEmoji:
			attr := domain.DocumentAttribute{Kind: domain.DocAttrCustomEmoji, Alt: v.Alt, Free: v.Free, TextColor: v.TextColor}
			if id, hash, ok := inputStickerSetIDs(v.Stickerset); ok {
				attr.StickerSetID = id
				attr.StickerSetAccessHash = hash
			}
			out = append(out, attr)
		}
	}
	return out
}

func inputStickerSetIDs(input tg.InputStickerSetClass) (int64, int64, bool) {
	if s, ok := input.(*tg.InputStickerSetID); ok {
		return s.ID, s.AccessHash, true
	}
	return 0, 0, false
}

// sendMessageRequestFromSendMedia 把 sendMedia（空媒体）的字段映射到 sendMessage 请求。
func sendMessageRequestFromSendMedia(req *tg.MessagesSendMediaRequest) *tg.MessagesSendMessageRequest {
	return &tg.MessagesSendMessageRequest{
		Silent:                 req.Silent,
		Background:             req.Background,
		ClearDraft:             req.ClearDraft,
		Noforwards:             req.Noforwards,
		UpdateStickersetsOrder: req.UpdateStickersetsOrder,
		InvertMedia:            req.InvertMedia,
		AllowPaidFloodskip:     req.AllowPaidFloodskip,
		Peer:                   req.Peer,
		ReplyTo:                req.ReplyTo,
		Message:                req.Message,
		RandomID:               req.RandomID,
		ReplyMarkup:            req.ReplyMarkup,
		Entities:               req.Entities,
		ScheduleDate:           req.ScheduleDate,
		ScheduleRepeatPeriod:   req.ScheduleRepeatPeriod,
		SendAs:                 req.SendAs,
		QuickReplyShortcut:     req.QuickReplyShortcut,
		Effect:                 req.Effect,
		AllowPaidStars:         req.AllowPaidStars,
		SuggestedPost:          req.SuggestedPost,
	}
}

func mediaUploadErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrFilePartsInvalid):
		return filePartsInvalidErr()
	case errors.Is(err, domain.ErrPhotoInvalid):
		return photoInvalidErr()
	case errors.Is(err, domain.ErrDocumentInvalid):
		return mediaInvalidErr()
	default:
		return internalErr()
	}
}

func userClassID(u tg.UserClass) int64 {
	if v, ok := u.(*tg.User); ok {
		return v.ID
	}
	return 0
}

func chatClassID(c tg.ChatClass) int64 {
	switch v := c.(type) {
	case *tg.Channel:
		return v.ID
	case *tg.Chat:
		return v.ID
	}
	return 0
}

func mapValuesUsers(m map[int64]tg.UserClass) []tg.UserClass {
	out := make([]tg.UserClass, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func mapValuesChats(m map[int64]tg.ChatClass) []tg.ChatClass {
	out := make([]tg.ChatClass, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
