package postgres

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// MessageStore 用 PostgreSQL 实现 store.MessageStore。
type MessageStore struct {
	db     sqlcgen.DBTX
	q      *sqlcgen.Queries
	boxIDs store.BoxIDAllocator
	pts    store.PtsAllocator
}

type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// MessageStoreOption 调整 PostgreSQL MessageStore 依赖。
type MessageStoreOption func(*MessageStore)

// WithMessageAllocators 注入 Redis-backed allocator；未注入时使用 PG max+1 兜底，仅用于测试。
func WithMessageAllocators(boxIDs store.BoxIDAllocator, pts store.PtsAllocator) MessageStoreOption {
	return func(s *MessageStore) {
		s.boxIDs = boxIDs
		s.pts = pts
	}
}

// NewMessageStore 基于 pgx 连接池（或事务）创建 MessageStore。
func NewMessageStore(db sqlcgen.DBTX, opts ...MessageStoreOption) *MessageStore {
	s := &MessageStore{db: db, q: sqlcgen.New(db)}
	for _, opt := range opts {
		opt(s)
	}
	if s.boxIDs == nil {
		s.boxIDs = pgBoxIDAllocator{s: s}
	}
	if s.pts == nil {
		s.pts = pgPtsAllocator{events: NewUpdateEventStore(db)}
	}
	return s
}

func (s *MessageStore) Create(ctx context.Context, msg domain.Message) (domain.Message, error) {
	if err := s.ensureOfficialSystemUser(ctx, msg); err != nil {
		return domain.Message{}, err
	}
	entities, err := encodeMessageEntities(msg.Entities)
	if err != nil {
		return domain.Message{}, err
	}
	if msg.Date == 0 {
		msg.Date = int(time.Now().Unix())
	}
	if msg.ID == 0 {
		msg.ID, err = s.boxIDs.NextBoxID(ctx, msg.OwnerUserID)
		if err != nil {
			return domain.Message{}, fmt.Errorf("allocate login message box id: %w", err)
		}
	}
	row, err := s.q.CreateMessage(ctx, sqlcgen.CreateMessageParams{
		OwnerUserID:  msg.OwnerUserID,
		BoxID:        int32(msg.ID),
		PeerType:     string(msg.Peer.Type),
		PeerID:       msg.Peer.ID,
		FromUserID:   msg.From.ID,
		MessageDate:  int32(msg.Date),
		Outgoing:     msg.Out,
		Body:         msg.Body,
		EntitiesJson: entities,
		Pts:          int32(msg.Pts),
	})
	if err != nil {
		return domain.Message{}, fmt.Errorf("create message: %w", err)
	}
	return messageFromCreateRow(row)
}

func (s *MessageStore) ensureOfficialSystemUser(ctx context.Context, msg domain.Message) error {
	if msg.Peer.Type != domain.PeerTypeUser && msg.From.Type != domain.PeerTypeUser {
		return nil
	}
	if msg.Peer.ID != domain.OfficialSystemUserID && msg.From.ID != domain.OfficialSystemUserID {
		return nil
	}
	u := domain.OfficialSystemUser()
	if _, err := s.db.Exec(ctx, `
INSERT INTO users (id, access_hash, phone, first_name, last_name, username, country_code, verified, support, about)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
  access_hash = EXCLUDED.access_hash,
  phone = EXCLUDED.phone,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  username = EXCLUDED.username,
  country_code = EXCLUDED.country_code,
  verified = EXCLUDED.verified,
  support = EXCLUDED.support,
  about = EXCLUDED.about,
  updated_at = now()
`, u.ID, u.AccessHash, u.Phone, u.FirstName, u.LastName, u.Username, u.CountryCode, u.Verified, u.Support, u.About); err != nil {
		return fmt.Errorf("ensure official system user: %w", err)
	}
	return nil
}

func (s *MessageStore) SendPrivateText(ctx context.Context, req domain.SendPrivateTextRequest) (res domain.SendPrivateTextResult, err error) {
	if req.SenderUserID == 0 || req.RecipientUserID == 0 {
		return domain.SendPrivateTextResult{}, fmt.Errorf("send private text: missing user id")
	}
	if req.RandomID == 0 {
		return domain.SendPrivateTextResult{}, fmt.Errorf("send private text: missing random id")
	}
	if req.Message == "" && req.Media.IsZero() {
		return domain.SendPrivateTextResult{}, fmt.Errorf("send private text: empty message")
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	entities, err := encodeMessageEntities(req.Entities)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	mediaJSON, err := encodeMessageMedia(req.Media)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	senderReply, recipientReply, err := s.resolvePrivateSendReply(ctx, req)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	senderMeta, err := messageMetadataParamsFrom(req.Silent, req.NoForwards, senderReply, req.Forward)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	recipientMeta, err := messageMetadataParamsFrom(req.Silent, req.NoForwards, recipientReply, req.Forward)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.SendPrivateTextResult{}, fmt.Errorf("send private text: db does not support transactions")
	}

	// Redis pts/box_id 分配移到事务外：分配走 Redis（本就不属 PG 事务），放在 Begin 前可避免在
	// 持有 PG 连接（与行锁）期间空等 Redis 往返，显著降低高并发下的连接占用。代价是分配→提交窗口变长、
	// 瞬时 pts 空洞窗口变大，但已由 getState/getDifference 只暴露「连续 pts」兜底（见 internal/app/updates）。
	// box_id 空洞无害（消息 id 允许不连续）；只有 pts 必须无洞，故仅把 pts 计入 reserved 做补洞。
	var reserved []reservedPts
	senderBoxID, err := s.boxIDs.NextBoxID(ctx, req.SenderUserID)
	if err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("allocate sender box id: %w", err)
	}
	senderPts, err := s.pts.NextPts(ctx, req.SenderUserID)
	if err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("allocate sender pts: %w", err)
	}
	reserved = append(reserved, reservedPts{userID: req.SenderUserID, pts: senderPts})

	var recipientBoxID, recipientPts int
	selfMessage := req.RecipientUserID == req.SenderUserID
	deliverRecipient := !selfMessage && !req.RecipientBlocked
	if deliverRecipient {
		recipientBoxID, err = s.boxIDs.NextBoxID(ctx, req.RecipientUserID)
		if err != nil {
			s.recordPtsGaps(ctx, reserved, req.Date)
			return domain.SendPrivateTextResult{}, fmt.Errorf("allocate recipient box id: %w", err)
		}
		recipientPts, err = s.pts.NextPts(ctx, req.RecipientUserID)
		if err != nil {
			s.recordPtsGaps(ctx, reserved, req.Date)
			return domain.SendPrivateTextResult{}, fmt.Errorf("allocate recipient pts: %w", err)
		}
		reserved = append(reserved, reservedPts{userID: req.RecipientUserID, pts: recipientPts})
	}

	tx, err := beginner.Begin(ctx)
	if err != nil {
		s.recordPtsGaps(ctx, reserved, req.Date)
		return domain.SendPrivateTextResult{}, fmt.Errorf("begin send message tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		// 未提交（出错或 random_id 重复）：已分配的 pts 不会落进真实事件，补 noop 占位避免 pts 永久空洞。
		s.recordPtsGaps(ctx, reserved, req.Date)
	}()

	// 事务级 advisory lock 串行化涉及收发双方的并发写，在任何行锁之前获取，消除 watermark/dialog
	// 行锁的 AB-BA 死锁（A↔B 反向并发 send/read/edit）。
	if err := lockUsersForUpdate(ctx, tx, req.SenderUserID, req.RecipientUserID); err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("lock send users: %w", err)
	}

	privateArg := sqlcgen.CreatePrivateMessageParams{
		SenderUserID:    req.SenderUserID,
		RecipientUserID: req.RecipientUserID,
		RandomID:        req.RandomID,
		MessageDate:     int32(req.Date),
		Body:            req.Message,
		EntitiesJson:    entities,
		MediaJson:       mediaJSON,
	}
	applyCreatePrivateMessageMetadata(&privateArg, senderMeta)
	pm, err := qtx.CreatePrivateMessage(ctx, privateArg)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// 幂等重复：返回原消息盒；defer 会把本次（白白分配的）pts 补成 noop。
			dup, dupErr := s.duplicateSendResult(ctx, req.SenderUserID, req.RecipientUserID, req.RandomID)
			if dupErr != nil {
				return domain.SendPrivateTextResult{}, dupErr
			}
			dup.Duplicate = true
			return dup, nil
		}
		return domain.SendPrivateTextResult{}, fmt.Errorf("create private message: %w", err)
	}

	senderArg := sqlcgen.CreateMessageBoxParams{
		OwnerUserID:      req.SenderUserID,
		BoxID:            int32(senderBoxID),
		PrivateMessageID: pm.ID,
		MessageSenderID:  req.SenderUserID,
		PeerType:         string(domain.PeerTypeUser),
		PeerID:           req.RecipientUserID,
		FromUserID:       req.SenderUserID,
		MessageDate:      int32(req.Date),
		Outgoing:         true,
		Body:             req.Message,
		EntitiesJson:     entities,
		Pts:              int32(senderPts),
		MediaJson:        mediaJSON,
		MediaUnread:      false,
		ReactionUnread:   false,
	}
	applyCreateMessageBoxMetadata(&senderArg, senderMeta)
	senderRow, err := qtx.CreateMessageBox(ctx, senderArg)
	if err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("create sender box: %w", err)
	}
	sender := messageFromBoxRow(senderRow)
	if err := qtx.UpsertOutboxDialog(ctx, sqlcgen.UpsertOutboxDialogParams{
		UserID:         req.SenderUserID,
		PeerType:       string(domain.PeerTypeUser),
		PeerID:         req.RecipientUserID,
		TopMessageID:   int32(sender.ID),
		TopMessageDate: int32(sender.Date),
	}); err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("upsert sender dialog: %w", err)
	}
	if err := appendNewMessageEvent(ctx, qtx, sender); err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
		TargetUserID:     req.SenderUserID,
		Pts:              int32(senderPts),
		EventType:        string(domain.UpdateEventNewMessage),
		ExcludeAuthKeyID: authKeyIDToInt64(req.OriginAuthKeyID),
		ExcludeSessionID: req.OriginSessionID,
	}); err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("enqueue sender dispatch: %w", err)
	}

	recipient := domain.Message{}
	if selfMessage {
		recipient = sender
	}
	if deliverRecipient {
		recipientArg := sqlcgen.CreateMessageBoxParams{
			OwnerUserID:      req.RecipientUserID,
			BoxID:            int32(recipientBoxID),
			PrivateMessageID: pm.ID,
			MessageSenderID:  req.SenderUserID,
			PeerType:         string(domain.PeerTypeUser),
			PeerID:           req.SenderUserID,
			FromUserID:       req.SenderUserID,
			MessageDate:      int32(req.Date),
			Outgoing:         false,
			Body:             req.Message,
			EntitiesJson:     entities,
			Pts:              int32(recipientPts),
			MediaJson:        mediaJSON,
			MediaUnread:      !req.Media.IsZero(),
			ReactionUnread:   false,
		}
		applyCreateMessageBoxMetadata(&recipientArg, recipientMeta)
		recipientRow, err := qtx.CreateMessageBox(ctx, recipientArg)
		if err != nil {
			return domain.SendPrivateTextResult{}, fmt.Errorf("create recipient box: %w", err)
		}
		recipient = messageFromBoxRow(recipientRow)
		if err := qtx.UpsertInboxDialog(ctx, sqlcgen.UpsertInboxDialogParams{
			UserID:         req.RecipientUserID,
			PeerType:       string(domain.PeerTypeUser),
			PeerID:         req.SenderUserID,
			TopMessageID:   int32(recipient.ID),
			TopMessageDate: int32(recipient.Date),
		}); err != nil {
			return domain.SendPrivateTextResult{}, fmt.Errorf("upsert recipient dialog: %w", err)
		}
		if err := appendNewMessageEvent(ctx, qtx, recipient); err != nil {
			return domain.SendPrivateTextResult{}, err
		}
		if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
			TargetUserID:     req.RecipientUserID,
			Pts:              int32(recipientPts),
			EventType:        string(domain.UpdateEventNewMessage),
			ExcludeAuthKeyID: 0,
			ExcludeSessionID: 0,
		}); err != nil {
			return domain.SendPrivateTextResult{}, fmt.Errorf("enqueue recipient dispatch: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("commit send message tx: %w", err)
	}
	committed = true
	return domain.SendPrivateTextResult{
		SenderMessage:    sender,
		RecipientMessage: recipient,
		SenderEvent:      eventFromMessage(sender),
		RecipientEvent:   eventFromMessage(recipient),
	}, nil
}

func (s *MessageStore) duplicateSendResult(ctx context.Context, senderUserID, recipientUserID, randomID int64) (domain.SendPrivateTextResult, error) {
	pm, err := s.q.GetPrivateMessageByRandomID(ctx, sqlcgen.GetPrivateMessageByRandomIDParams{
		SenderUserID: senderUserID,
		RandomID:     randomID,
	})
	if err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("get duplicate private message: %w", err)
	}
	senderRow, err := s.q.GetMessageBoxByPrivateMessage(ctx, sqlcgen.GetMessageBoxByPrivateMessageParams{
		OwnerUserID:      senderUserID,
		PrivateMessageID: pm.ID,
	})
	if err != nil {
		return domain.SendPrivateTextResult{}, fmt.Errorf("get duplicate sender box: %w", err)
	}
	sender := messageFromGetBoxRow(senderRow)
	recipient := domain.Message{}
	if recipientUserID == senderUserID {
		recipient = sender
	}
	if recipientUserID != senderUserID {
		recipientRow, err := s.q.GetMessageBoxByPrivateMessage(ctx, sqlcgen.GetMessageBoxByPrivateMessageParams{
			OwnerUserID:      recipientUserID,
			PrivateMessageID: pm.ID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.SendPrivateTextResult{
					SenderMessage:  sender,
					SenderEvent:    eventFromMessage(sender),
					RecipientEvent: domain.UpdateEvent{},
				}, nil
			}
			return domain.SendPrivateTextResult{}, fmt.Errorf("get duplicate recipient box: %w", err)
		}
		recipient = messageFromGetBoxRow(recipientRow)
	}
	return domain.SendPrivateTextResult{
		SenderMessage:    sender,
		RecipientMessage: recipient,
		SenderEvent:      eventFromMessage(sender),
		RecipientEvent:   eventFromMessage(recipient),
	}, nil
}

func (s *MessageStore) resolvePrivateSendReply(ctx context.Context, req domain.SendPrivateTextRequest) (*domain.MessageReply, *domain.MessageReply, error) {
	if req.ReplyTo == nil {
		return nil, nil, nil
	}
	if req.ReplyTo.MessageID <= 0 || req.ReplyTo.MessageID > domain.MaxMessageBoxID {
		return nil, nil, domain.ErrReplyMessageIDInvalid
	}
	peer := req.ReplyTo.Peer
	if peer.ID == 0 {
		peer = domain.Peer{Type: domain.PeerTypeUser, ID: req.RecipientUserID}
	}
	if peer.Type != domain.PeerTypeUser || peer.ID != req.RecipientUserID {
		return nil, nil, domain.ErrReplyMessageIDInvalid
	}
	source, err := s.q.GetMessageBoxForReply(ctx, sqlcgen.GetMessageBoxForReplyParams{
		OwnerUserID: req.SenderUserID,
		PeerType:    string(peer.Type),
		PeerID:      peer.ID,
		BoxID:       int32(req.ReplyTo.MessageID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, domain.ErrReplyMessageIDInvalid
		}
		return nil, nil, fmt.Errorf("get reply message: %w", err)
	}
	senderReply := cloneMessageReply(req.ReplyTo)
	senderReply.MessageID = int(source.BoxID)
	senderReply.Peer = peer
	if req.SenderUserID == req.RecipientUserID {
		return senderReply, cloneMessageReply(senderReply), nil
	}

	recipientRow, err := s.q.GetMessageBoxByPrivateMessage(ctx, sqlcgen.GetMessageBoxByPrivateMessageParams{
		OwnerUserID:      req.RecipientUserID,
		PrivateMessageID: source.PrivateMessageID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return senderReply, nil, nil
		}
		return nil, nil, fmt.Errorf("get recipient reply message: %w", err)
	}
	recipientReply := cloneMessageReply(senderReply)
	recipientReply.MessageID = int(recipientRow.BoxID)
	recipientReply.Peer = domain.Peer{Type: domain.PeerTypeUser, ID: req.SenderUserID}
	return senderReply, recipientReply, nil
}

func cloneMessageReply(reply *domain.MessageReply) *domain.MessageReply {
	if reply == nil {
		return nil
	}
	clone := *reply
	clone.QuoteEntities = append([]domain.MessageEntity(nil), reply.QuoteEntities...)
	return &clone
}

func cloneMessageForward(forward *domain.MessageForward) *domain.MessageForward {
	if forward == nil {
		return nil
	}
	clone := *forward
	return &clone
}

func cloneChannelMessageAction(action *domain.ChannelMessageAction) *domain.ChannelMessageAction {
	if action == nil {
		return nil
	}
	clone := *action
	clone.UserIDs = append([]int64(nil), action.UserIDs...)
	if action.Closed != nil {
		v := *action.Closed
		clone.Closed = &v
	}
	if action.Hidden != nil {
		v := *action.Hidden
		clone.Hidden = &v
	}
	return &clone
}

func (s *MessageStore) ForwardPrivateMessages(ctx context.Context, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error) {
	res := domain.ForwardPrivateMessagesResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.ToUserID == 0 {
		return res, fmt.Errorf("forward private messages: missing user id")
	}
	if req.FromPeer.Type != domain.PeerTypeUser || req.FromPeer.ID == 0 {
		return res, fmt.Errorf("forward private messages: invalid source peer")
	}
	if len(req.MessageIDs) == 0 || len(req.MessageIDs) != len(req.RandomIDs) {
		return res, domain.ErrMessageIDInvalid
	}
	if len(req.MessageIDs) > domain.MaxForwardMessageIDs {
		return res, fmt.Errorf("forward private messages: too many ids: %d > %d", len(req.MessageIDs), domain.MaxForwardMessageIDs)
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	boxIDs := make([]int32, 0, len(req.MessageIDs))
	for i, id := range req.MessageIDs {
		if id <= 0 || id > domain.MaxMessageBoxID || req.RandomIDs[i] == 0 {
			return res, domain.ErrMessageIDInvalid
		}
		boxIDs = append(boxIDs, int32(id))
	}
	rows, err := s.q.GetMessageBoxesForForward(ctx, sqlcgen.GetMessageBoxesForForwardParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.FromPeer.Type),
		PeerID:      req.FromPeer.ID,
		BoxIds:      boxIDs,
	})
	if err != nil {
		return res, fmt.Errorf("get forward messages: %w", err)
	}
	if len(rows) != len(req.MessageIDs) {
		return res, domain.ErrMessageIDInvalid
	}
	res.SenderMessages = make([]domain.Message, 0, len(rows))
	res.RecipientMessages = make([]domain.Message, 0, len(rows))
	res.SenderEvents = make([]domain.UpdateEvent, 0, len(rows))
	res.RecipientEvents = make([]domain.UpdateEvent, 0, len(rows))
	res.Duplicates = make([]bool, 0, len(rows))
	for i, row := range rows {
		if int(row.BoxID) != req.MessageIDs[i] {
			return res, domain.ErrMessageIDInvalid
		}
		source, err := messageFromForwardRow(row)
		if err != nil {
			return res, err
		}
		if source.NoForwards {
			return res, domain.ErrChatForwardsRestricted
		}
		var forward *domain.MessageForward
		if !req.DropAuthor {
			forward = cloneMessageForward(source.Forward)
			if forward == nil {
				forward = &domain.MessageForward{From: source.From, Date: source.Date}
			}
		}
		sent, err := s.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:     req.OwnerUserID,
			RecipientUserID:  req.ToUserID,
			RandomID:         req.RandomIDs[i],
			Message:          source.Body,
			Entities:         append([]domain.MessageEntity(nil), source.Entities...),
			Media:            source.Media,
			Silent:           req.Silent,
			NoForwards:       req.NoForwards,
			ReplyTo:          req.ReplyTo,
			Forward:          forward,
			Date:             req.Date,
			OriginAuthKeyID:  req.OriginAuthKeyID,
			OriginSessionID:  req.OriginSessionID,
			RecipientBlocked: req.RecipientBlocked,
		})
		if err != nil {
			return res, err
		}
		res.SenderMessages = append(res.SenderMessages, sent.SenderMessage)
		res.RecipientMessages = append(res.RecipientMessages, sent.RecipientMessage)
		res.SenderEvents = append(res.SenderEvents, sent.SenderEvent)
		res.RecipientEvents = append(res.RecipientEvents, sent.RecipientEvent)
		res.Duplicates = append(res.Duplicates, sent.Duplicate)
	}
	return res, nil
}

func (s *MessageStore) GetByIDs(ctx context.Context, userID int64, ids []int) (domain.MessageList, error) {
	if userID == 0 || len(ids) == 0 {
		return domain.MessageList{}, nil
	}
	boxIDs := make([]int32, 0, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		boxIDs = append(boxIDs, int32(id))
	}
	if len(boxIDs) == 0 {
		return domain.MessageList{}, nil
	}
	rows, err := s.q.GetMessageBoxesByIDs(ctx, sqlcgen.GetMessageBoxesByIDsParams{
		OwnerUserID: userID,
		BoxIds:      boxIDs,
	})
	if err != nil {
		return domain.MessageList{}, fmt.Errorf("get messages by ids: %w", err)
	}
	out := domain.MessageList{
		Messages: make([]domain.Message, 0, len(rows)),
		Users:    make([]domain.User, 0, len(rows)*2),
	}
	seenUsers := map[int64]struct{}{}
	for _, row := range rows {
		msg, err := messageFromIDRow(row)
		if err != nil {
			return domain.MessageList{}, err
		}
		out.Messages = append(out.Messages, msg)
		appendUsersFromMessageIDRow(&out, seenUsers, row)
	}
	if err := s.enrichPrivateMessageReactions(ctx, s.db, userID, out.Messages); err != nil {
		return domain.MessageList{}, err
	}
	out.Hash = messageListHash(out.Messages)
	return out, nil
}

func (s *MessageStore) ListByUser(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	addOffset := domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	rows, err := s.q.ListMessagesByUser(ctx, sqlcgen.ListMessagesByUserParams{
		OwnerUserID:    userID,
		HasPeer:        filter.HasPeer,
		PeerType:       string(filter.Peer.Type),
		PeerID:         filter.Peer.ID,
		Query:          filter.Query,
		OffsetID:       pgInt32NonNegative(filter.OffsetID),
		OffsetDate:     pgInt32NonNegative(filter.OffsetDate),
		MaxID:          pgInt32NonNegative(filter.MaxID),
		MinID:          pgInt32NonNegative(filter.MinID),
		AddOffset:      pgInt32Bounded(addOffset),
		LimitCount:     int32(limit),
		NeedTotalCount: filter.NeedTotalCount,
	})
	if err != nil {
		return domain.MessageList{}, fmt.Errorf("list messages: %w", err)
	}
	out := domain.MessageList{
		Messages: make([]domain.Message, 0, len(rows)),
		Users:    make([]domain.User, 0, len(rows)*2),
	}
	seenUsers := map[int64]struct{}{}
	for _, row := range rows {
		entities, err := decodeMessageEntities(row.EntitiesJson)
		if err != nil {
			return domain.MessageList{}, fmt.Errorf("decode message entities: %w", err)
		}
		silent, noforwards, reply, forward, err := messageMetadataFromFields(
			row.Silent,
			row.Noforwards,
			row.ReplyToMsgID,
			row.ReplyToPeerType,
			row.ReplyToPeerID,
			row.ReplyToTopID,
			row.QuoteText,
			row.QuoteEntitiesJson,
			row.QuoteOffset,
			row.FwdFromPeerType,
			row.FwdFromPeerID,
			row.FwdFromName,
			row.FwdDate,
		)
		if err != nil {
			return domain.MessageList{}, fmt.Errorf("decode message metadata: %w", err)
		}
		media, err := decodeMessageMedia(row.MediaJson)
		if err != nil {
			return domain.MessageList{}, fmt.Errorf("decode message media: %w", err)
		}
		out.Messages = append(out.Messages, domain.Message{
			ID:             int(row.BoxID),
			UID:            row.PrivateMessageID,
			OwnerUserID:    row.OwnerUserID,
			Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
			From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
			Date:           int(row.MessageDate),
			EditDate:       int(row.EditDate),
			Out:            row.Outgoing,
			Silent:         silent,
			NoForwards:     noforwards,
			Body:           row.Body,
			Entities:       entities,
			ReplyTo:        reply,
			Forward:        forward,
			Pts:            int(row.Pts),
			Media:          media,
			MediaUnread:    row.MediaUnread,
			ReactionUnread: row.ReactionUnread,
		})
		if out.Count == 0 {
			out.Count = int(row.TotalCount)
		}
		appendUserFromMessageRow(&out, seenUsers, row)
	}
	if err := s.enrichPrivateMessageReactions(ctx, s.db, userID, out.Messages); err != nil {
		return domain.MessageList{}, err
	}
	out.Hash = messageListHash(out.Messages)
	return out, nil
}

func (s *MessageStore) ReadHistory(ctx context.Context, req domain.ReadHistoryRequest) (res domain.ReadHistoryResult, err error) {
	res = domain.ReadHistoryResult{OwnerUserID: req.OwnerUserID, Peer: req.Peer, MaxID: req.MaxID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("read history: missing owner user id")
	}
	if req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 {
		return res, fmt.Errorf("read history: invalid peer")
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("read history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin read history tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	var reserved []reservedPts
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		s.recordPtsGaps(ctx, reserved, req.Date)
	}()

	// advisory lock 串行化与会话对端的并发写（peer 即私聊另一方 / 回执 sender），须在行锁前获取。
	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID, req.Peer.ID); err != nil {
		return res, fmt.Errorf("lock read history users: %w", err)
	}

	state, err := qtx.GetDialogReadStateForUpdate(ctx, sqlcgen.GetDialogReadStateForUpdateParams{
		UserID:   req.OwnerUserID,
		PeerType: string(req.Peer.Type),
		PeerID:   req.Peer.ID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, nil
		}
		return res, fmt.Errorf("get dialog read state: %w", err)
	}
	readMax := req.MaxID
	if readMax <= 0 {
		readMax = int(state.TopMessageID)
	}
	if readMax > domain.MaxMessageBoxID {
		readMax = domain.MaxMessageBoxID
	}
	res.MaxID = readMax
	oldRead := int(state.ReadInboxMaxID)
	changed := int(state.UnreadCount) > 0 || readMax > oldRead
	if !changed {
		return res, nil
	}

	candidate, candidateErr := qtx.LatestIncomingReadReceiptCandidate(ctx, sqlcgen.LatestIncomingReadReceiptCandidateParams{
		OwnerUserID:       req.OwnerUserID,
		PeerType:          string(req.Peer.Type),
		PeerID:            req.Peer.ID,
		OldReadInboxMaxID: int32(oldRead),
		NewReadInboxMaxID: int32(readMax),
	})
	if candidateErr != nil && !errors.Is(candidateErr, pgx.ErrNoRows) {
		return res, fmt.Errorf("load read receipt candidate: %w", candidateErr)
	}

	updated, err := qtx.UpdateDialogReadInbox(ctx, sqlcgen.UpdateDialogReadInboxParams{
		UserID:         req.OwnerUserID,
		PeerType:       string(req.Peer.Type),
		PeerID:         req.Peer.ID,
		ReadInboxMaxID: int32(readMax),
	})
	if err != nil {
		return res, fmt.Errorf("update dialog read inbox: %w", err)
	}
	readerPts, err := s.pts.NextPts(ctx, req.OwnerUserID)
	if err != nil {
		return res, fmt.Errorf("allocate read history pts: %w", err)
	}
	reserved = append(reserved, reservedPts{userID: req.OwnerUserID, pts: readerPts})
	res.Changed = true
	res.MaxID = int(updated.ReadInboxMaxID)
	res.StillUnreadCount = int(updated.UnreadCount)
	res.InboxEvent = domain.UpdateEvent{
		UserID:           req.OwnerUserID,
		Type:             domain.UpdateEventReadHistoryInbox,
		Pts:              readerPts,
		PtsCount:         1,
		Date:             req.Date,
		Peer:             req.Peer,
		MaxID:            res.MaxID,
		StillUnreadCount: res.StillUnreadCount,
	}
	if err := appendUserUpdateEvent(ctx, qtx, req.OwnerUserID, res.InboxEvent); err != nil {
		return res, fmt.Errorf("append read inbox event: %w", err)
	}
	if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
		TargetUserID:     req.OwnerUserID,
		Pts:              int32(readerPts),
		EventType:        string(domain.UpdateEventReadHistoryInbox),
		ExcludeAuthKeyID: authKeyIDToInt64(req.OriginAuthKeyID),
		ExcludeSessionID: req.OriginSessionID,
	}); err != nil {
		return res, fmt.Errorf("enqueue read inbox dispatch: %w", err)
	}

	if candidateErr == nil && candidate.SenderOwnerUserID != 0 && int(candidate.SenderBoxID) > 0 {
		if _, err := qtx.UpdateDialogReadOutbox(ctx, sqlcgen.UpdateDialogReadOutboxParams{
			UserID:          candidate.SenderOwnerUserID,
			PeerType:        string(domain.PeerTypeUser),
			PeerID:          req.OwnerUserID,
			ReadOutboxMaxID: candidate.SenderBoxID,
		}); err == nil {
			senderPts, err := s.pts.NextPts(ctx, candidate.SenderOwnerUserID)
			if err != nil {
				return res, fmt.Errorf("allocate read outbox pts: %w", err)
			}
			reserved = append(reserved, reservedPts{userID: candidate.SenderOwnerUserID, pts: senderPts})
			res.OutboxChanged = true
			res.OutboxUserID = candidate.SenderOwnerUserID
			res.OutboxEvent = domain.UpdateEvent{
				UserID:   candidate.SenderOwnerUserID,
				Type:     domain.UpdateEventReadHistoryOutbox,
				Pts:      senderPts,
				PtsCount: 1,
				Date:     req.Date,
				Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID},
				MaxID:    int(candidate.SenderBoxID),
			}
			if err := appendUserUpdateEvent(ctx, qtx, candidate.SenderOwnerUserID, res.OutboxEvent); err != nil {
				return res, fmt.Errorf("append read outbox event: %w", err)
			}
			if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
				TargetUserID:     candidate.SenderOwnerUserID,
				Pts:              int32(senderPts),
				EventType:        string(domain.UpdateEventReadHistoryOutbox),
				ExcludeAuthKeyID: 0,
				ExcludeSessionID: 0,
			}); err != nil {
				return res, fmt.Errorf("enqueue read outbox dispatch: %w", err)
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return res, fmt.Errorf("update dialog read outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit read history tx: %w", err)
	}
	committed = true
	return res, nil
}

func (s *MessageStore) ReadMessageContents(ctx context.Context, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error) {
	res := domain.ReadMessageContentsResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("read message contents: missing owner user id")
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return res, domain.ErrMessageIDInvalid
	}
	seen := make(map[int]struct{}, len(req.IDs))
	ids := make([]int32, 0, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return res, domain.ErrMessageIDInvalid
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, int32(id))
	}
	if len(ids) == 0 {
		return res, nil
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("read message contents: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin read message contents tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	var reserved []reservedPts
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		s.recordPtsGaps(ctx, reserved, req.Date)
	}()
	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID); err != nil {
		return res, fmt.Errorf("lock read message contents user: %w", err)
	}
	rows, err := tx.Query(ctx, `
WITH target AS (
  SELECT owner_user_id, box_id, peer_type, peer_id, reaction_unread
  FROM message_boxes
  WHERE owner_user_id = $1
    AND box_id = ANY($2::int[])
    AND NOT deleted
    AND (media_unread OR reaction_unread)
  FOR UPDATE
),
updated AS (
  UPDATE message_boxes
  SET media_unread = false,
      reaction_unread = false
  FROM target t
  WHERE message_boxes.owner_user_id = t.owner_user_id
    AND message_boxes.box_id = t.box_id
  RETURNING message_boxes.box_id, t.peer_type, t.peer_id, t.reaction_unread
)
SELECT box_id, peer_type, peer_id, reaction_unread
FROM updated
ORDER BY box_id`, req.OwnerUserID, ids)
	if err != nil {
		return res, fmt.Errorf("read message contents: %w", err)
	}
	defer rows.Close()
	affectedPeers := make(map[domain.Peer]struct{})
	for rows.Next() {
		var id int32
		var peerType string
		var peerID int64
		var reactionUnread bool
		if err := rows.Scan(&id, &peerType, &peerID, &reactionUnread); err != nil {
			return res, fmt.Errorf("scan read message contents: %w", err)
		}
		res.MessageIDs = append(res.MessageIDs, int(id))
		if reactionUnread && peerID != 0 {
			affectedPeers[domain.Peer{Type: domain.PeerType(peerType), ID: peerID}] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("read message contents rows: %w", err)
	}
	if len(res.MessageIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return res, fmt.Errorf("commit read message contents noop: %w", err)
		}
		committed = true
		return res, nil
	}
	for peer := range affectedPeers {
		if peer.Type != domain.PeerTypeUser || peer.ID == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE dialogs d
SET unread_reactions_count = (
    SELECT COUNT(*)::int
    FROM message_boxes m
    WHERE m.owner_user_id = d.user_id
      AND m.peer_type = d.peer_type
      AND m.peer_id = d.peer_id
      AND NOT m.deleted
      AND m.reaction_unread
),
updated_at = now()
WHERE d.user_id = $1
  AND d.peer_type = $2
  AND d.peer_id = $3`, req.OwnerUserID, string(peer.Type), peer.ID); err != nil {
			return res, fmt.Errorf("refresh dialog unread reactions after content read: %w", err)
		}
	}
	pts, err := s.nextPtsN(ctx, req.OwnerUserID, len(res.MessageIDs))
	if err != nil {
		return res, fmt.Errorf("allocate read message contents pts: %w", err)
	}
	reserved = append(reserved, reservedPts{userID: req.OwnerUserID, pts: pts, count: len(res.MessageIDs)})
	res.Event = domain.UpdateEvent{
		UserID:     req.OwnerUserID,
		Type:       domain.UpdateEventReadMessageContents,
		Pts:        pts,
		PtsCount:   len(res.MessageIDs),
		Date:       req.Date,
		MessageIDs: append([]int(nil), res.MessageIDs...),
	}
	if err := appendUserUpdateEvent(ctx, qtx, req.OwnerUserID, res.Event); err != nil {
		return res, fmt.Errorf("append read message contents event: %w", err)
	}
	if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
		TargetUserID:     req.OwnerUserID,
		Pts:              int32(pts),
		EventType:        string(domain.UpdateEventReadMessageContents),
		ExcludeAuthKeyID: authKeyIDToInt64(req.OriginAuthKeyID),
		ExcludeSessionID: req.OriginSessionID,
	}); err != nil {
		return res, fmt.Errorf("enqueue read message contents dispatch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit read message contents tx: %w", err)
	}
	committed = true
	return res, nil
}

func (s *MessageStore) GetOutboxReadDate(ctx context.Context, req domain.OutboxReadDateRequest) (int, error) {
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return 0, domain.ErrMessageIDInvalid
	}
	if _, err := s.q.GetOutboxMessageForReadDate(ctx, sqlcgen.GetOutboxMessageForReadDateParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
		BoxID:       int32(req.ID),
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, domain.ErrMessageIDInvalid
		}
		return 0, fmt.Errorf("get outbox message for read date: %w", err)
	}
	date, err := s.q.GetOutboxReadDate(ctx, sqlcgen.GetOutboxReadDateParams{
		UserID:    req.OwnerUserID,
		PeerType:  string(req.Peer.Type),
		PeerID:    req.Peer.ID,
		MessageID: int32(req.ID),
	})
	if err != nil {
		return 0, fmt.Errorf("get outbox read date: %w", err)
	}
	if date == 0 {
		return 0, domain.ErrMessageNotReadYet
	}
	return int(date), nil
}

func (s *MessageStore) SetMessageReactions(ctx context.Context, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if req.UserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if len(req.Reactions) > domain.MaxChannelMessageReactionsPerUser {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	for _, reaction := range req.Reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("set message reactions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("begin set message reactions tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := lockUsersForUpdate(ctx, tx, req.UserID, req.Peer.ID); err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("lock set message reactions users: %w", err)
	}

	var target struct {
		boxID            int32
		privateMessageID int64
		messageSenderID  int64
	}
	if err := tx.QueryRow(ctx, `
SELECT box_id, private_message_id, message_sender_id
FROM message_boxes
WHERE owner_user_id = $1
  AND box_id = $2
  AND peer_type = $3
  AND peer_id = $4
  AND NOT deleted
LIMIT 1
FOR UPDATE`, req.UserID, int32(req.MessageID), string(req.Peer.Type), req.Peer.ID).Scan(&target.boxID, &target.privateMessageID, &target.messageSenderID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("get message for reactions: %w", err)
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM private_message_reactions
WHERE message_sender_id = $1
  AND private_message_id = $2
  AND user_id = $3`, target.messageSenderID, target.privateMessageID, req.UserID); err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("delete old message reactions: %w", err)
	}
	for i, reaction := range req.Reactions {
		if _, err := tx.Exec(ctx, `
INSERT INTO private_message_reactions (
  message_sender_id,
  private_message_id,
  user_id,
  reaction_type,
  reaction_value,
  big,
  reaction_date,
  chosen_order
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (message_sender_id, private_message_id, user_id, reaction_type, reaction_value)
DO UPDATE SET
  big = EXCLUDED.big,
  reaction_date = EXCLUDED.reaction_date,
  chosen_order = EXCLUDED.chosen_order,
  updated_at = now()`,
			target.messageSenderID,
			target.privateMessageID,
			req.UserID,
			string(reaction.Type),
			reaction.Emoticon,
			req.Big,
			int32(req.Date),
			int32(i+1),
		); err != nil {
			return domain.PrivateMessageReactionsResult{}, fmt.Errorf("insert message reaction: %w", err)
		}
	}
	if target.messageSenderID != 0 && target.messageSenderID != req.UserID {
		if _, err := tx.Exec(ctx, `
UPDATE message_boxes b
SET reaction_unread = EXISTS (
    SELECT 1
    FROM private_message_reactions r
    WHERE r.message_sender_id = b.message_sender_id
      AND r.private_message_id = b.private_message_id
      AND r.user_id <> b.owner_user_id
)
WHERE b.owner_user_id = $1
  AND b.message_sender_id = $2
  AND b.private_message_id = $3`, target.messageSenderID, target.messageSenderID, target.privateMessageID); err != nil {
			return domain.PrivateMessageReactionsResult{}, fmt.Errorf("update private reaction unread: %w", err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE dialogs d
SET unread_reactions_count = (
    SELECT COUNT(*)::int
    FROM message_boxes m
    WHERE m.owner_user_id = d.user_id
      AND m.peer_type = d.peer_type
      AND m.peer_id = d.peer_id
      AND NOT m.deleted
      AND m.reaction_unread
),
updated_at = now()
WHERE d.user_id = $1
  AND d.peer_type = $2
  AND d.peer_id = $3`, target.messageSenderID, string(domain.PeerTypeUser), req.UserID); err != nil {
			return domain.PrivateMessageReactionsResult{}, fmt.Errorf("refresh private reaction unread dialog: %w", err)
		}
	}

	boxes, err := qtx.ListVisibleMessageBoxesByPrivateMessage(ctx, sqlcgen.ListVisibleMessageBoxesByPrivateMessageParams{
		MessageSenderID:  target.messageSenderID,
		PrivateMessageID: target.privateMessageID,
	})
	if err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("list visible reaction boxes: %w", err)
	}
	res := domain.PrivateMessageReactionsResult{Messages: make([]domain.Message, 0, len(boxes))}
	for _, box := range boxes {
		msg, err := messageFromVisibleBoxRow(box)
		if err != nil {
			return domain.PrivateMessageReactionsResult{}, err
		}
		res.Messages = append(res.Messages, msg)
	}
	if err := s.enrichPrivateMessageReactions(ctx, tx, req.UserID, res.Messages); err != nil {
		return domain.PrivateMessageReactionsResult{}, err
	}
	for _, msg := range res.Messages {
		if msg.OwnerUserID == req.UserID && msg.Reactions != nil {
			res.Reactions = *msg.Reactions
			break
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("commit set message reactions tx: %w", err)
	}
	committed = true
	return res, nil
}

func (s *MessageStore) GetMessageReactions(ctx context.Context, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if len(req.IDs) == 0 {
		return domain.PrivateMessageReactionsResult{}, nil
	}
	boxIDs := make([]int32, 0, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
		boxIDs = append(boxIDs, int32(id))
	}
	rows, err := s.q.GetMessageBoxesByIDs(ctx, sqlcgen.GetMessageBoxesByIDsParams{
		OwnerUserID: req.OwnerUserID,
		BoxIds:      boxIDs,
	})
	if err != nil {
		return domain.PrivateMessageReactionsResult{}, fmt.Errorf("get message reactions boxes: %w", err)
	}
	res := domain.PrivateMessageReactionsResult{Messages: make([]domain.Message, 0, len(rows))}
	for _, row := range rows {
		if row.PeerType != string(req.Peer.Type) || row.PeerID != req.Peer.ID {
			continue
		}
		msg, err := messageFromIDRow(row)
		if err != nil {
			return domain.PrivateMessageReactionsResult{}, err
		}
		res.Messages = append(res.Messages, msg)
	}
	if err := s.enrichPrivateMessageReactions(ctx, s.db, req.OwnerUserID, res.Messages); err != nil {
		return domain.PrivateMessageReactionsResult{}, err
	}
	for _, msg := range res.Messages {
		if msg.Reactions != nil {
			res.Reactions = *msg.Reactions
			break
		}
	}
	return res, nil
}

func (s *MessageStore) EditMessage(ctx context.Context, req domain.EditMessageRequest) (res domain.EditMessageResult, err error) {
	res = domain.EditMessageResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("edit message: missing owner user id")
	}
	if req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return res, domain.ErrMessageIDInvalid
	}
	if req.Message == "" {
		return res, fmt.Errorf("edit message: empty message")
	}
	if req.EditDate == 0 {
		req.EditDate = int(time.Now().Unix())
	}
	entities, err := encodeMessageEntities(req.Entities)
	if err != nil {
		return res, err
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("edit message: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin edit message tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	var reserved []reservedPts
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		s.recordPtsGaps(ctx, reserved, req.EditDate)
	}()

	// advisory lock 串行化与会话对端的并发写，须在行锁前获取，消除 AB-BA 死锁。
	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID, req.Peer.ID); err != nil {
		return res, fmt.Errorf("lock edit message users: %w", err)
	}

	target, err := qtx.GetMessageBoxForEdit(ctx, sqlcgen.GetMessageBoxForEditParams{
		OwnerUserID: req.OwnerUserID,
		BoxID:       int32(req.ID),
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, domain.ErrMessageIDInvalid
		}
		return res, fmt.Errorf("get message for edit: %w", err)
	}
	if !target.Outgoing || target.MessageSenderID != req.OwnerUserID || target.FromUserID != req.OwnerUserID {
		return res, domain.ErrMessageAuthorRequired
	}
	oldEntities, err := decodeMessageEntities(target.EntitiesJson)
	if err != nil {
		return res, fmt.Errorf("decode target entities: %w", err)
	}
	if target.Body == req.Message && sameMessageEntities(oldEntities, req.Entities) {
		return res, domain.ErrMessageNotModified
	}
	boxes, err := qtx.ListVisibleMessageBoxesByPrivateMessage(ctx, sqlcgen.ListVisibleMessageBoxesByPrivateMessageParams{
		MessageSenderID:  req.OwnerUserID,
		PrivateMessageID: target.PrivateMessageID,
	})
	if err != nil {
		return res, fmt.Errorf("list visible edit boxes: %w", err)
	}
	if len(boxes) == 0 {
		return res, domain.ErrMessageIDInvalid
	}
	if err := qtx.UpdatePrivateMessageEdit(ctx, sqlcgen.UpdatePrivateMessageEditParams{
		SenderUserID:     req.OwnerUserID,
		PrivateMessageID: target.PrivateMessageID,
		Body:             req.Message,
		EntitiesJson:     entities,
		EditDate:         int32(req.EditDate),
	}); err != nil {
		return res, fmt.Errorf("update private message edit: %w", err)
	}
	res.Edited = make([]domain.EditedMessageForUser, 0, len(boxes))
	for _, box := range boxes {
		pts, err := s.pts.NextPts(ctx, box.OwnerUserID)
		if err != nil {
			return res, fmt.Errorf("allocate edit message pts: %w", err)
		}
		reserved = append(reserved, reservedPts{userID: box.OwnerUserID, pts: pts})
		updated, err := qtx.UpdateMessageBoxEdit(ctx, sqlcgen.UpdateMessageBoxEditParams{
			OwnerUserID:  box.OwnerUserID,
			BoxID:        box.BoxID,
			Body:         req.Message,
			EntitiesJson: entities,
			EditDate:     int32(req.EditDate),
			Pts:          int32(pts),
		})
		if err != nil {
			return res, fmt.Errorf("update message box edit: %w", err)
		}
		msg, err := messageFromUpdateEditRow(updated)
		if err != nil {
			return res, err
		}
		event := domain.UpdateEvent{
			UserID:   msg.OwnerUserID,
			Type:     domain.UpdateEventEditMessage,
			Pts:      msg.Pts,
			PtsCount: 1,
			Date:     req.EditDate,
			Message:  msg,
		}
		if err := appendUserUpdateEvent(ctx, qtx, msg.OwnerUserID, event); err != nil {
			return res, fmt.Errorf("append edit message event: %w", err)
		}
		dispatchAuthKeyID := [8]byte{}
		dispatchSessionID := int64(0)
		if msg.OwnerUserID == req.OwnerUserID {
			dispatchAuthKeyID = req.OriginAuthKeyID
			dispatchSessionID = req.OriginSessionID
		}
		if err := qtx.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
			TargetUserID:     msg.OwnerUserID,
			Pts:              int32(pts),
			EventType:        string(domain.UpdateEventEditMessage),
			ExcludeAuthKeyID: authKeyIDToInt64(dispatchAuthKeyID),
			ExcludeSessionID: dispatchSessionID,
		}); err != nil {
			return res, fmt.Errorf("enqueue edit message dispatch: %w", err)
		}
		res.Edited = append(res.Edited, domain.EditedMessageForUser{UserID: msg.OwnerUserID, Message: msg, Event: event})
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit edit message tx: %w", err)
	}
	committed = true
	return res, nil
}

func (s *MessageStore) DeleteMessages(ctx context.Context, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error) {
	res := domain.DeleteMessagesResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("delete messages: missing owner user id")
	}
	ids := normalizeMessageIDs(req.IDs)
	if len(ids) == 0 {
		return res, nil
	}
	if len(ids) > domain.MaxDeleteMessageIDs {
		return res, fmt.Errorf("delete messages: too many ids: %d > %d", len(ids), domain.MaxDeleteMessageIDs)
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("delete messages: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin delete messages tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	var reserved []reservedPts
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		s.recordPtsGaps(ctx, reserved, req.Date)
	}()

	// advisory lock 串行化本 owner 的并发写（与 send/edit/read/其它 delete 共享 owner 时串行）。
	// 被删消息的对端是动态的（由删除结果推出），未在此锁定；但 finishDeleteMessagesTx 内 watermark
	// 与 dialog rebuild 均按 user_id 升序执行，故两个反向 delete 也不会 AB-BA。
	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID); err != nil {
		return res, fmt.Errorf("lock delete messages user: %w", err)
	}

	rows, err := qtx.DeleteMessageBoxesByIDs(ctx, sqlcgen.DeleteMessageBoxesByIDsParams{
		OwnerUserID: req.OwnerUserID,
		BoxIds:      int32s(ids),
	})
	if err != nil {
		return res, fmt.Errorf("delete message boxes by ids: %w", err)
	}
	deleted := deletedRowsFromIDRows(rows)
	if req.Revoke && len(deleted) > 0 {
		peerRows, err := qtx.DeleteMessageBoxesByPrivateMessages(ctx, privateMessageDeleteParams(deleted))
		if err != nil {
			return res, fmt.Errorf("delete revoked private message boxes: %w", err)
		}
		deleted = append(deleted, deletedRowsFromPrivateRows(peerRows)...)
	}
	res, reserved, err = s.finishDeleteMessagesTx(ctx, qtx, req.OwnerUserID, req.OriginAuthKeyID, req.OriginSessionID, req.Date, deleted, false)
	if err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit delete messages tx: %w", err)
	}
	committed = true
	return res, nil
}

func (s *MessageStore) DeleteHistory(ctx context.Context, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error) {
	res := domain.DeleteMessagesResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("delete history: missing owner user id")
	}
	if req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 {
		return res, fmt.Errorf("delete history: invalid peer")
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("delete history: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin delete history tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	var reserved []reservedPts
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
		s.recordPtsGaps(ctx, reserved, req.Date)
	}()

	// advisory lock 串行化与会话对端的并发写，须在行锁前获取，消除 AB-BA 死锁。
	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID, req.Peer.ID); err != nil {
		return res, fmt.Errorf("lock delete history users: %w", err)
	}

	maxID := pgInt32NonNegative(req.MaxID)
	rows, err := qtx.DeleteMessageBoxesByPeerBatch(ctx, sqlcgen.DeleteMessageBoxesByPeerBatchParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
		MaxID:       maxID,
		LimitCount:  int32(domain.MaxDeleteHistoryBatch),
	})
	if err != nil {
		return res, fmt.Errorf("delete message boxes by peer: %w", err)
	}
	deleted := deletedRowsFromPeerBatchRows(rows)
	if req.Revoke && len(deleted) > 0 {
		peerRows, err := qtx.DeleteMessageBoxesByPrivateMessages(ctx, privateMessageDeleteParams(deleted))
		if err != nil {
			return res, fmt.Errorf("delete revoked private history boxes: %w", err)
		}
		deleted = append(deleted, deletedRowsFromPrivateRows(peerRows)...)
	}
	res, reserved, err = s.finishDeleteMessagesTx(ctx, qtx, req.OwnerUserID, req.OriginAuthKeyID, req.OriginSessionID, req.Date, deleted, req.JustClear)
	if err != nil {
		return res, err
	}
	more, err := qtx.HasDeletableMessageBoxByPeer(ctx, sqlcgen.HasDeletableMessageBoxByPeerParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
		MaxID:       maxID,
	})
	if err != nil {
		return res, fmt.Errorf("check remaining history after delete: %w", err)
	}
	if more {
		res.Offset = 1
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit delete history tx: %w", err)
	}
	committed = true
	return res, nil
}

type deletedBox struct {
	ownerUserID      int64
	boxID            int
	privateMessageID int64
	messageSenderID  int64
	peer             domain.Peer
}

func (s *MessageStore) finishDeleteMessagesTx(ctx context.Context, q *sqlcgen.Queries, ownerUserID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, date int, rows []deletedBox, preserveEmptyDialogs bool) (domain.DeleteMessagesResult, []reservedPts, error) {
	res := domain.DeleteMessagesResult{OwnerUserID: ownerUserID}
	if len(rows) == 0 {
		return res, nil, nil
	}
	peersByOwner := make(map[int64]map[domain.Peer]struct{})
	idsByOwner := make(map[int64][]int)
	for _, row := range rows {
		if row.ownerUserID == 0 || row.boxID == 0 {
			continue
		}
		idsByOwner[row.ownerUserID] = append(idsByOwner[row.ownerUserID], row.boxID)
		if row.peer.ID != 0 {
			if peersByOwner[row.ownerUserID] == nil {
				peersByOwner[row.ownerUserID] = make(map[domain.Peer]struct{})
			}
			peersByOwner[row.ownerUserID][row.peer] = struct{}{}
		}
	}
	// 按 owner 升序重建 dialog，使两个反向 delete（X 删与 Y 的会话 / Y 删与 X 的会话）以一致顺序
	// 获取 dialog 行锁，配合下方 watermark 的升序推进，彻底避免 delete-delete 之间的 AB-BA 死锁。
	rebuildOwners := make([]int64, 0, len(peersByOwner))
	for userID := range peersByOwner {
		rebuildOwners = append(rebuildOwners, userID)
	}
	sort.Slice(rebuildOwners, func(i, j int) bool { return rebuildOwners[i] < rebuildOwners[j] })
	for _, userID := range rebuildOwners {
		for peer := range peersByOwner[userID] {
			if err := rebuildDialogAfterMessageDelete(ctx, q, userID, peer, preserveEmptyDialogs); err != nil {
				return res, nil, err
			}
		}
	}

	ownerIDs := make([]int64, 0, len(idsByOwner))
	for userID := range idsByOwner {
		ownerIDs = append(ownerIDs, userID)
	}
	sort.Slice(ownerIDs, func(i, j int) bool { return ownerIDs[i] < ownerIDs[j] })

	reserved := make([]reservedPts, 0, len(ownerIDs))
	res.Deleted = make([]domain.DeletedMessagesForUser, 0, len(ownerIDs))
	for _, userID := range ownerIDs {
		ids := normalizeMessageIDs(idsByOwner[userID])
		if len(ids) == 0 {
			continue
		}
		pts, err := s.nextPtsN(ctx, userID, len(ids))
		if err != nil {
			return res, reserved, fmt.Errorf("allocate delete messages pts: %w", err)
		}
		reserved = append(reserved, reservedPts{userID: userID, pts: pts, count: len(ids)})
		event := domain.UpdateEvent{
			UserID:     userID,
			Type:       domain.UpdateEventDeleteMessages,
			Pts:        pts,
			PtsCount:   len(ids),
			Date:       date,
			MessageIDs: ids,
		}
		if err := appendDeleteMessagesEvent(ctx, q, event); err != nil {
			return res, reserved, err
		}
		dispatchAuthKeyID := [8]byte{}
		dispatchSessionID := int64(0)
		if userID == ownerUserID {
			dispatchAuthKeyID = excludeAuthKeyID
			dispatchSessionID = excludeSessionID
		}
		if err := q.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
			TargetUserID:     userID,
			Pts:              int32(pts),
			EventType:        string(domain.UpdateEventDeleteMessages),
			ExcludeAuthKeyID: authKeyIDToInt64(dispatchAuthKeyID),
			ExcludeSessionID: dispatchSessionID,
		}); err != nil {
			return res, reserved, fmt.Errorf("enqueue delete messages dispatch: %w", err)
		}
		res.Deleted = append(res.Deleted, domain.DeletedMessagesForUser{
			UserID:     userID,
			MessageIDs: ids,
			Event:      event,
		})
	}
	return res, reserved, nil
}

func rebuildDialogAfterMessageDelete(ctx context.Context, q *sqlcgen.Queries, userID int64, peer domain.Peer, preserveEmpty bool) error {
	top, err := q.TopVisibleMessageBoxByPeer(ctx, sqlcgen.TopVisibleMessageBoxByPeerParams{
		OwnerUserID: userID,
		PeerType:    string(peer.Type),
		PeerID:      peer.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		if preserveEmpty {
			if err := q.ClearDialogAfterHistoryDelete(ctx, sqlcgen.ClearDialogAfterHistoryDeleteParams{
				UserID:   userID,
				PeerType: string(peer.Type),
				PeerID:   peer.ID,
			}); err != nil {
				return fmt.Errorf("clear empty dialog after history delete: %w", err)
			}
			return nil
		}
		if err := q.DeleteDialogByPeer(ctx, sqlcgen.DeleteDialogByPeerParams{
			UserID:   userID,
			PeerType: string(peer.Type),
			PeerID:   peer.ID,
		}); err != nil {
			return fmt.Errorf("delete empty dialog after message delete: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load top message after delete: %w", err)
	}
	if err := q.RefreshDialogAfterMessageDelete(ctx, sqlcgen.RefreshDialogAfterMessageDeleteParams{
		TopMessageID:   top.BoxID,
		TopMessageDate: top.MessageDate,
		UserID:         userID,
		PeerType:       string(peer.Type),
		PeerID:         peer.ID,
	}); err != nil {
		return fmt.Errorf("refresh dialog after message delete: %w", err)
	}
	return nil
}

func appendDeleteMessagesEvent(ctx context.Context, q *sqlcgen.Queries, event domain.UpdateEvent) error {
	messageIDs, err := encodeEventMessageIDs(event.MessageIDs)
	if err != nil {
		return err
	}
	if event.PtsCount == 0 {
		event.PtsCount = len(event.MessageIDs)
	}
	if event.PtsCount == 0 {
		event.PtsCount = 1
	}
	if err := q.AppendUserUpdateEvent(ctx, sqlcgen.AppendUserUpdateEventParams{
		UserID:       event.UserID,
		Pts:          int32(event.Pts),
		PtsCount:     int32(event.PtsCount),
		Date:         int32(event.Date),
		EventType:    string(domain.UpdateEventDeleteMessages),
		EventPeers:   []byte("[]"),
		PeerSettings: []byte("{}"),
		MessageIds:   messageIDs,
		DialogFilter: []byte("{}"),
		FilterOrder:  []byte("[]"),
		FolderPeers:  []byte("[]"),
	}); err != nil {
		return fmt.Errorf("append delete messages event: %w", err)
	}
	if _, err := advanceContiguousPts(ctx, q, event.UserID); err != nil {
		return fmt.Errorf("advance update watermark after delete messages: %w", err)
	}
	return nil
}

func deletedRowsFromIDRows(rows []sqlcgen.DeleteMessageBoxesByIDsRow) []deletedBox {
	out := make([]deletedBox, 0, len(rows))
	for _, row := range rows {
		out = append(out, deletedBox{
			ownerUserID:      row.OwnerUserID,
			boxID:            int(row.BoxID),
			privateMessageID: row.PrivateMessageID,
			messageSenderID:  row.MessageSenderID,
			peer:             domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		})
	}
	return out
}

func deletedRowsFromPeerRows(rows []sqlcgen.DeleteMessageBoxesByPeerRow) []deletedBox {
	out := make([]deletedBox, 0, len(rows))
	for _, row := range rows {
		out = append(out, deletedBox{
			ownerUserID:      row.OwnerUserID,
			boxID:            int(row.BoxID),
			privateMessageID: row.PrivateMessageID,
			messageSenderID:  row.MessageSenderID,
			peer:             domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		})
	}
	return out
}

func deletedRowsFromPeerBatchRows(rows []sqlcgen.DeleteMessageBoxesByPeerBatchRow) []deletedBox {
	out := make([]deletedBox, 0, len(rows))
	for _, row := range rows {
		out = append(out, deletedBox{
			ownerUserID:      row.OwnerUserID,
			boxID:            int(row.BoxID),
			privateMessageID: row.PrivateMessageID,
			messageSenderID:  row.MessageSenderID,
			peer:             domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		})
	}
	return out
}

func deletedRowsFromPrivateRows(rows []sqlcgen.DeleteMessageBoxesByPrivateMessagesRow) []deletedBox {
	out := make([]deletedBox, 0, len(rows))
	for _, row := range rows {
		out = append(out, deletedBox{
			ownerUserID:      row.OwnerUserID,
			boxID:            int(row.BoxID),
			privateMessageID: row.PrivateMessageID,
			messageSenderID:  row.MessageSenderID,
			peer:             domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		})
	}
	return out
}

func privateMessageDeleteParams(rows []deletedBox) sqlcgen.DeleteMessageBoxesByPrivateMessagesParams {
	senderIDs := make([]int64, 0, len(rows))
	privateIDs := make([]int64, 0, len(rows))
	seen := make(map[[2]int64]struct{}, len(rows))
	for _, row := range rows {
		key := [2]int64{row.messageSenderID, row.privateMessageID}
		if row.messageSenderID == 0 || row.privateMessageID == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		senderIDs = append(senderIDs, row.messageSenderID)
		privateIDs = append(privateIDs, row.privateMessageID)
	}
	return sqlcgen.DeleteMessageBoxesByPrivateMessagesParams{
		MessageSenderIds:  senderIDs,
		PrivateMessageIds: privateIDs,
	}
}

func normalizeMessageIDs(ids []int) []int {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int, 0, len(ids))
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func int32s(ids []int) []int32 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int32, 0, len(ids))
	for _, id := range ids {
		out = append(out, pgInt32NonNegative(id))
	}
	return out
}

func pgInt32NonNegative(v int) int32 {
	if v <= 0 {
		return 0
	}
	if v > domain.MaxMessageBoxID {
		return int32(domain.MaxMessageBoxID)
	}
	return int32(v)
}

func pgInt32Bounded(v int) int32 {
	if v > domain.MaxMessageBoxID {
		return int32(domain.MaxMessageBoxID)
	}
	if v < -domain.MaxMessageBoxID {
		return int32(-domain.MaxMessageBoxID)
	}
	return int32(v)
}

func appendNewMessageEvent(ctx context.Context, q *sqlcgen.Queries, msg domain.Message) error {
	boxID := int32(msg.ID)
	peerType := string(msg.Peer.Type)
	peerID := msg.Peer.ID
	if err := q.AppendUserUpdateEvent(ctx, sqlcgen.AppendUserUpdateEventParams{
		UserID:       msg.OwnerUserID,
		Pts:          int32(msg.Pts),
		PtsCount:     1,
		Date:         int32(msg.Date),
		EventType:    string(domain.UpdateEventNewMessage),
		EventPeers:   []byte("[]"),
		PeerSettings: []byte("{}"),
		MessageIds:   []byte("[]"),
		DialogFilter: []byte("{}"),
		FilterOrder:  []byte("[]"),
		FolderPeers:  []byte("[]"),
		MessageBoxID: &boxID,
		PeerType:     &peerType,
		PeerID:       &peerID,
	}); err != nil {
		return fmt.Errorf("append new message event: %w", err)
	}
	if _, err := advanceContiguousPts(ctx, q, msg.OwnerUserID); err != nil {
		return fmt.Errorf("advance update watermark after new message: %w", err)
	}
	return nil
}

// lockUsersForUpdate 在事务开始处用事务级 advisory lock 串行化所有涉及指定用户的并发写事务。
// advisory lock 与行锁处于独立锁空间，且按 user_id 升序获取，因此：① 不会与后续 dialog /
// watermark / box 行锁交叉成跨类型死锁；② 任意两个共享某用户的写事务（send/read/edit/delete 对
// 收发双方的并发操作）被完全串行化，从根上消除它们在 watermark 与 dialog 行上因加锁顺序相反
// 导致的 AB-BA 死锁——既包含本次 watermark 优化新引入的（user_update_watermarks FOR UPDATE），
// 也包含 dialog upsert 既有的反向行锁。advisory xact lock 在事务结束自动释放；同对用户本就竞争
// 这些行（天然串行），不额外降并发，不同用户集合的事务仍并行。**必须在任何行锁之前调用。**
func lockUsersForUpdate(ctx context.Context, tx pgx.Tx, userIDs ...int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	unique := make([]int64, 0, len(userIDs))
	seen := make(map[int64]struct{}, len(userIDs))
	for _, id := range userIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	for _, id := range unique {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", id); err != nil {
			return fmt.Errorf("advisory lock user %d: %w", id, err)
		}
	}
	return nil
}

type reservedPts struct {
	userID int64
	pts    int
	count  int
}

func (s *MessageStore) recordPtsGaps(ctx context.Context, items []reservedPts, date int) {
	for _, item := range items {
		count := item.count
		if count <= 0 {
			count = 1
		}
		_ = s.q.AppendUserUpdateEvent(ctx, sqlcgen.AppendUserUpdateEventParams{
			UserID:       item.userID,
			Pts:          int32(item.pts),
			PtsCount:     int32(count),
			Date:         int32(date),
			EventType:    string(domain.UpdateEventNoop),
			EventPeers:   []byte("[]"),
			PeerSettings: []byte("{}"),
			MessageIds:   []byte("[]"),
			DialogFilter: []byte("{}"),
			FilterOrder:  []byte("[]"),
			FolderPeers:  []byte("[]"),
		})
		_, _ = advanceContiguousPts(ctx, s.q, item.userID)
	}
}

func (s *MessageStore) nextPtsN(ctx context.Context, userID int64, count int) (int, error) {
	if count <= 0 {
		count = 1
	}
	if count == 1 {
		return s.pts.NextPts(ctx, userID)
	}
	if ranges, ok := s.pts.(store.PtsRangeAllocator); ok {
		return ranges.NextPtsN(ctx, userID, count)
	}
	var pts int
	var err error
	for i := 0; i < count; i++ {
		pts, err = s.pts.NextPts(ctx, userID)
		if err != nil {
			return 0, err
		}
	}
	return pts, nil
}

type messageMetadataParams struct {
	Silent            bool
	Noforwards        bool
	ReplyToMsgID      int32
	ReplyToPeerType   string
	ReplyToPeerID     int64
	ReplyToTopID      int32
	QuoteText         string
	QuoteEntitiesJSON []byte
	QuoteOffset       int32
	FwdFromPeerType   string
	FwdFromPeerID     int64
	FwdFromName       string
	FwdDate           int32
}

func messageMetadataParamsFrom(silent, noforwards bool, reply *domain.MessageReply, forward *domain.MessageForward) (messageMetadataParams, error) {
	meta := messageMetadataParams{
		Silent:            silent,
		Noforwards:        noforwards,
		QuoteEntitiesJSON: []byte("[]"),
	}
	if reply != nil {
		if err := domain.ValidateMessageReplyBounds(reply); err != nil {
			return messageMetadataParams{}, err
		}
		quoteEntities, err := encodeMessageEntities(reply.QuoteEntities)
		if err != nil {
			return messageMetadataParams{}, err
		}
		meta.ReplyToMsgID = int32(reply.MessageID)
		meta.ReplyToPeerType = string(reply.Peer.Type)
		meta.ReplyToPeerID = reply.Peer.ID
		meta.ReplyToTopID = int32(reply.TopMessageID)
		meta.QuoteText = reply.QuoteText
		meta.QuoteEntitiesJSON = quoteEntities
		meta.QuoteOffset = int32(reply.QuoteOffset)
	}
	if forward != nil {
		if forward.Date < 0 {
			return messageMetadataParams{}, fmt.Errorf("forward metadata: invalid date")
		}
		meta.FwdFromPeerType = string(forward.From.Type)
		meta.FwdFromPeerID = forward.From.ID
		meta.FwdFromName = forward.FromName
		meta.FwdDate = int32(forward.Date)
	}
	return meta, nil
}

func applyCreatePrivateMessageMetadata(arg *sqlcgen.CreatePrivateMessageParams, meta messageMetadataParams) {
	arg.Silent = meta.Silent
	arg.Noforwards = meta.Noforwards
	arg.ReplyToMsgID = meta.ReplyToMsgID
	arg.ReplyToPeerType = meta.ReplyToPeerType
	arg.ReplyToPeerID = meta.ReplyToPeerID
	arg.ReplyToTopID = meta.ReplyToTopID
	arg.QuoteText = meta.QuoteText
	arg.QuoteEntitiesJson = meta.QuoteEntitiesJSON
	arg.QuoteOffset = meta.QuoteOffset
	arg.FwdFromPeerType = meta.FwdFromPeerType
	arg.FwdFromPeerID = meta.FwdFromPeerID
	arg.FwdFromName = meta.FwdFromName
	arg.FwdDate = meta.FwdDate
}

func applyCreateMessageBoxMetadata(arg *sqlcgen.CreateMessageBoxParams, meta messageMetadataParams) {
	arg.Silent = meta.Silent
	arg.Noforwards = meta.Noforwards
	arg.ReplyToMsgID = meta.ReplyToMsgID
	arg.ReplyToPeerType = meta.ReplyToPeerType
	arg.ReplyToPeerID = meta.ReplyToPeerID
	arg.ReplyToTopID = meta.ReplyToTopID
	arg.QuoteText = meta.QuoteText
	arg.QuoteEntitiesJson = meta.QuoteEntitiesJSON
	arg.QuoteOffset = meta.QuoteOffset
	arg.FwdFromPeerType = meta.FwdFromPeerType
	arg.FwdFromPeerID = meta.FwdFromPeerID
	arg.FwdFromName = meta.FwdFromName
	arg.FwdDate = meta.FwdDate
}

func messageMetadataFromFields(silent, noforwards bool, replyToMsgID int32, replyToPeerType string, replyToPeerID int64, replyToTopID int32, quoteText, quoteEntitiesJSON string, quoteOffset int32, fwdFromPeerType string, fwdFromPeerID int64, fwdFromName string, fwdDate int32) (bool, bool, *domain.MessageReply, *domain.MessageForward, error) {
	var reply *domain.MessageReply
	if replyToMsgID > 0 {
		quoteEntities, err := decodeMessageEntities(quoteEntitiesJSON)
		if err != nil {
			return false, false, nil, nil, err
		}
		reply = &domain.MessageReply{
			MessageID:     int(replyToMsgID),
			Peer:          domain.Peer{Type: domain.PeerType(replyToPeerType), ID: replyToPeerID},
			TopMessageID:  int(replyToTopID),
			QuoteText:     quoteText,
			QuoteEntities: quoteEntities,
			QuoteOffset:   int(quoteOffset),
		}
	}
	var forward *domain.MessageForward
	if fwdDate != 0 || fwdFromPeerID != 0 || fwdFromName != "" {
		forward = &domain.MessageForward{
			From:     domain.Peer{Type: domain.PeerType(fwdFromPeerType), ID: fwdFromPeerID},
			FromName: fwdFromName,
			Date:     int(fwdDate),
		}
	}
	return silent, noforwards, reply, forward, nil
}

func messageFromCreateRow(row sqlcgen.CreateMessageRow) (domain.Message, error) {
	entities, err := decodeMessageEntities(row.EntitiesJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode message entities: %w", err)
	}
	return domain.Message{
		ID:          int(row.BoxID),
		UID:         row.PrivateMessageID,
		OwnerUserID: row.OwnerUserID,
		Peer:        domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:        int(row.MessageDate),
		EditDate:    int(row.EditDate),
		Out:         row.Outgoing,
		Body:        row.Body,
		Entities:    entities,
		Pts:         int(row.Pts),
	}, nil
}

func messageFromBoxRow(row sqlcgen.CreateMessageBoxRow) domain.Message {
	entities, _ := decodeMessageEntities(row.EntitiesJson)
	media, _ := decodeMessageMedia(row.MediaJson)
	silent, noforwards, reply, forward, _ := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}
}

func messageFromGetBoxRow(row sqlcgen.GetMessageBoxByPrivateMessageRow) domain.Message {
	entities, _ := decodeMessageEntities(row.EntitiesJson)
	media, _ := decodeMessageMedia(row.MediaJson)
	silent, noforwards, reply, forward, _ := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}
}

func messageFromVisibleBoxRow(row sqlcgen.ListVisibleMessageBoxesByPrivateMessageRow) (domain.Message, error) {
	entities, err := decodeMessageEntities(row.EntitiesJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode visible message entities: %w", err)
	}
	silent, noforwards, reply, forward, err := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode visible message metadata: %w", err)
	}
	media, err := decodeMessageMedia(row.MediaJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode visible message media: %w", err)
	}
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}, nil
}

func messageFromUpdateEditRow(row sqlcgen.UpdateMessageBoxEditRow) (domain.Message, error) {
	entities, err := decodeMessageEntities(row.EntitiesJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode edited message entities: %w", err)
	}
	silent, noforwards, reply, forward, err := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode edited message metadata: %w", err)
	}
	media, err := decodeMessageMedia(row.MediaJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode edited message media: %w", err)
	}
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}, nil
}

func messageFromForwardRow(row sqlcgen.GetMessageBoxesForForwardRow) (domain.Message, error) {
	entities, err := decodeMessageEntities(row.EntitiesJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode forward message entities: %w", err)
	}
	silent, noforwards, reply, forward, err := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode forward message metadata: %w", err)
	}
	media, err := decodeMessageMedia(row.MediaJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode forward message media: %w", err)
	}
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}, nil
}

func messageFromIDRow(row sqlcgen.GetMessageBoxesByIDsRow) (domain.Message, error) {
	entities, err := decodeMessageEntities(row.EntitiesJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode message entities: %w", err)
	}
	silent, noforwards, reply, forward, err := messageMetadataFromFields(
		row.Silent,
		row.Noforwards,
		row.ReplyToMsgID,
		row.ReplyToPeerType,
		row.ReplyToPeerID,
		row.ReplyToTopID,
		row.QuoteText,
		row.QuoteEntitiesJson,
		row.QuoteOffset,
		row.FwdFromPeerType,
		row.FwdFromPeerID,
		row.FwdFromName,
		row.FwdDate,
	)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode message metadata: %w", err)
	}
	media, err := decodeMessageMedia(row.MediaJson)
	if err != nil {
		return domain.Message{}, fmt.Errorf("decode message media: %w", err)
	}
	return domain.Message{
		Media:          media,
		ID:             int(row.BoxID),
		UID:            row.PrivateMessageID,
		OwnerUserID:    row.OwnerUserID,
		Peer:           domain.Peer{Type: domain.PeerType(row.PeerType), ID: row.PeerID},
		From:           domain.Peer{Type: domain.PeerTypeUser, ID: row.FromUserID},
		Date:           int(row.MessageDate),
		EditDate:       int(row.EditDate),
		Out:            row.Outgoing,
		Silent:         silent,
		NoForwards:     noforwards,
		Body:           row.Body,
		Entities:       entities,
		ReplyTo:        reply,
		Forward:        forward,
		Pts:            int(row.Pts),
		MediaUnread:    row.MediaUnread,
		ReactionUnread: row.ReactionUnread,
	}, nil
}

func eventFromMessage(msg domain.Message) domain.UpdateEvent {
	return domain.UpdateEvent{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  msg,
	}
}

func appendUserFromMessageRow(out *domain.MessageList, seen map[int64]struct{}, row sqlcgen.ListMessagesByUserRow) {
	appendMessageUsers(out, seen,
		domain.User{
			ID:          row.PeerUserID,
			AccessHash:  row.PeerAccessHash,
			Phone:       row.PeerPhone,
			FirstName:   row.PeerFirstName,
			LastName:    row.PeerLastName,
			Username:    row.PeerUsername,
			CountryCode: row.PeerCountryCode,
			Verified:    row.PeerVerified,
			Support:     row.PeerSupport,
			LastSeenAt:  int(row.PeerLastSeenAt),
		},
		domain.User{
			ID:          row.FromUserUserID,
			AccessHash:  row.FromUserAccessHash,
			Phone:       row.FromUserPhone,
			FirstName:   row.FromUserFirstName,
			LastName:    row.FromUserLastName,
			Username:    row.FromUserUsername,
			CountryCode: row.FromUserCountryCode,
			Verified:    row.FromUserVerified,
			Support:     row.FromUserSupport,
			LastSeenAt:  int(row.FromUserLastSeenAt),
		},
	)
}

func appendUsersFromMessageIDRow(out *domain.MessageList, seen map[int64]struct{}, row sqlcgen.GetMessageBoxesByIDsRow) {
	appendMessageUsers(out, seen,
		domain.User{
			ID:          row.PeerUserID,
			AccessHash:  row.PeerAccessHash,
			Phone:       row.PeerPhone,
			FirstName:   row.PeerFirstName,
			LastName:    row.PeerLastName,
			Username:    row.PeerUsername,
			CountryCode: row.PeerCountryCode,
			Verified:    row.PeerVerified,
			Support:     row.PeerSupport,
			LastSeenAt:  int(row.PeerLastSeenAt),
		},
		domain.User{
			ID:          row.FromUserUserID,
			AccessHash:  row.FromUserAccessHash,
			Phone:       row.FromUserPhone,
			FirstName:   row.FromUserFirstName,
			LastName:    row.FromUserLastName,
			Username:    row.FromUserUsername,
			CountryCode: row.FromUserCountryCode,
			Verified:    row.FromUserVerified,
			Support:     row.FromUserSupport,
			LastSeenAt:  int(row.FromUserLastSeenAt),
		},
	)
}

func appendMessageUsers(out *domain.MessageList, seen map[int64]struct{}, users ...domain.User) {
	add := func(u domain.User) {
		if u.ID == 0 {
			return
		}
		if _, ok := seen[u.ID]; ok {
			return
		}
		seen[u.ID] = struct{}{}
		out.Users = append(out.Users, u)
	}
	for _, user := range users {
		add(user)
	}
}

type privateMessageReactionRow struct {
	messageSenderID  int64
	privateMessageID int64
	userID           int64
	reaction         domain.MessageReaction
	big              bool
	date             int
	chosenOrder      int
}

type privateMessageReactionKey struct {
	messageSenderID  int64
	privateMessageID int64
}

func (s *MessageStore) enrichPrivateMessageReactions(ctx context.Context, db sqlcgen.DBTX, viewerUserID int64, messages []domain.Message) error {
	if len(messages) == 0 {
		return nil
	}
	keySet := make(map[privateMessageReactionKey]struct{}, len(messages))
	senderIDs := make([]int64, 0, len(messages))
	privateIDs := make([]int64, 0, len(messages))
	for _, msg := range messages {
		if msg.UID == 0 || msg.From.ID == 0 {
			continue
		}
		key := privateMessageReactionKey{messageSenderID: msg.From.ID, privateMessageID: msg.UID}
		if _, ok := keySet[key]; ok {
			continue
		}
		keySet[key] = struct{}{}
		senderIDs = append(senderIDs, key.messageSenderID)
		privateIDs = append(privateIDs, key.privateMessageID)
	}
	if len(senderIDs) == 0 {
		return nil
	}
	rows, err := db.Query(ctx, `
WITH wanted AS (
  SELECT message_sender_id, private_message_id
  FROM unnest($1::bigint[], $2::bigint[]) AS w(message_sender_id, private_message_id)
)
SELECT r.message_sender_id, r.private_message_id, r.user_id, r.reaction_type, r.reaction_value, r.big, r.reaction_date, r.chosen_order
FROM private_message_reactions r
JOIN wanted w
  ON w.message_sender_id = r.message_sender_id
 AND w.private_message_id = r.private_message_id
ORDER BY r.message_sender_id ASC, r.private_message_id ASC, r.reaction_date DESC, r.user_id DESC, r.reaction_value ASC`, senderIDs, privateIDs)
	if err != nil {
		return fmt.Errorf("load private message reactions: %w", err)
	}
	defer rows.Close()
	byMessage := make(map[privateMessageReactionKey][]privateMessageReactionRow)
	for rows.Next() {
		var (
			messageSenderID int64
			uid             int64
			userID          int64
			reactionType    string
			value           string
			big             bool
			date            int32
			chosenOrder     int32
		)
		if err := rows.Scan(&messageSenderID, &uid, &userID, &reactionType, &value, &big, &date, &chosenOrder); err != nil {
			return fmt.Errorf("scan private message reactions: %w", err)
		}
		if reactionType != string(domain.MessageReactionEmoji) || strings.TrimSpace(value) == "" {
			continue
		}
		key := privateMessageReactionKey{messageSenderID: messageSenderID, privateMessageID: uid}
		byMessage[key] = append(byMessage[key], privateMessageReactionRow{
			messageSenderID:  messageSenderID,
			privateMessageID: uid,
			userID:           userID,
			reaction:         domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: value},
			big:              big,
			date:             int(date),
			chosenOrder:      int(chosenOrder),
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("private message reactions rows: %w", err)
	}
	for i := range messages {
		key := privateMessageReactionKey{messageSenderID: messages[i].From.ID, privateMessageID: messages[i].UID}
		reactions := privateMessageReactionsFromRows(byMessage[key], viewerUserID)
		if len(reactions.Results) == 0 && len(reactions.Recent) == 0 {
			continue
		}
		messages[i].Reactions = &reactions
	}
	return nil
}

func privateMessageReactionsFromRows(rows []privateMessageReactionRow, viewerUserID int64) domain.ChannelMessageReactions {
	out := domain.ChannelMessageReactions{
		CanSeeList: true,
		Results:    []domain.ChannelMessageReactionCount{},
		Recent:     []domain.ChannelMessagePeerReaction{},
	}
	if len(rows) == 0 {
		return out
	}
	type aggregate struct {
		reaction    domain.MessageReaction
		count       int
		chosenOrder int
		latestDate  int
	}
	aggregates := make(map[string]*aggregate)
	recent := make([]domain.ChannelMessagePeerReaction, 0, len(rows))
	for _, row := range rows {
		key := string(row.reaction.Type) + "\x00" + row.reaction.Emoticon
		item := aggregates[key]
		if item == nil {
			item = &aggregate{reaction: row.reaction}
			aggregates[key] = item
		}
		item.count++
		if row.userID == viewerUserID && row.chosenOrder > 0 && (item.chosenOrder == 0 || row.chosenOrder < item.chosenOrder) {
			item.chosenOrder = row.chosenOrder
		}
		if row.date > item.latestDate {
			item.latestDate = row.date
		}
		recent = append(recent, domain.ChannelMessagePeerReaction{
			UserID:      row.userID,
			Reaction:    row.reaction,
			Big:         row.big,
			My:          row.userID == viewerUserID,
			ChosenOrder: row.chosenOrder,
			Date:        row.date,
		})
	}
	items := make([]aggregate, 0, len(aggregates))
	for _, item := range aggregates {
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		if items[i].latestDate != items[j].latestDate {
			return items[i].latestDate > items[j].latestDate
		}
		return items[i].reaction.Emoticon < items[j].reaction.Emoticon
	})
	for _, item := range items {
		out.Results = append(out.Results, domain.ChannelMessageReactionCount{
			Reaction:    item.reaction,
			Count:       item.count,
			ChosenOrder: item.chosenOrder,
		})
	}
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].Date != recent[j].Date {
			return recent[i].Date > recent[j].Date
		}
		if recent[i].UserID != recent[j].UserID {
			return recent[i].UserID > recent[j].UserID
		}
		return recent[i].Reaction.Emoticon < recent[j].Reaction.Emoticon
	})
	if len(recent) > domain.MaxChannelMessageReactionRecent {
		recent = recent[:domain.MaxChannelMessageReactionRecent]
	}
	out.Recent = recent
	return out
}

type pgBoxIDAllocator struct {
	s *MessageStore
}

func (a pgBoxIDAllocator) NextBoxID(ctx context.Context, userID int64) (int, error) {
	cur, err := a.CurrentBoxID(ctx, userID)
	if err != nil {
		return 0, err
	}
	return cur + 1, nil
}

func (a pgBoxIDAllocator) CurrentBoxID(ctx context.Context, userID int64) (int, error) {
	v, err := a.s.q.MaxMessageBoxID(ctx, userID)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

type pgPtsAllocator struct {
	events *UpdateEventStore
}

func (a pgPtsAllocator) NextPts(ctx context.Context, userID int64) (int, error) {
	cur, err := a.CurrentPts(ctx, userID)
	if err != nil {
		return 0, err
	}
	return cur + 1, nil
}

func (a pgPtsAllocator) NextPtsN(ctx context.Context, userID int64, count int) (int, error) {
	if count <= 0 {
		count = 1
	}
	cur, err := a.CurrentPts(ctx, userID)
	if err != nil {
		return 0, err
	}
	return cur + count, nil
}

func (a pgPtsAllocator) CurrentPts(ctx context.Context, userID int64) (int, error) {
	return a.events.Current(ctx, userID)
}

type messageEntityJSON struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

func encodeMessageEntities(entities []domain.MessageEntity) ([]byte, error) {
	if len(entities) == 0 {
		return []byte("[]"), nil
	}
	wire := make([]messageEntityJSON, 0, len(entities))
	for _, entity := range entities {
		wire = append(wire, messageEntityJSON{
			Type:   string(entity.Type),
			Offset: entity.Offset,
			Length: entity.Length,
		})
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal message entities: %w", err)
	}
	return raw, nil
}

func decodeMessageEntities(raw string) ([]domain.MessageEntity, error) {
	if raw == "" {
		return nil, nil
	}
	var wire []messageEntityJSON
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, err
	}
	out := make([]domain.MessageEntity, 0, len(wire))
	for _, entity := range wire {
		out = append(out, domain.MessageEntity{
			Type:   domain.MessageEntityType(entity.Type),
			Offset: entity.Offset,
			Length: entity.Length,
		})
	}
	return out, nil
}

func sameMessageEntities(a, b []domain.MessageEntity) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func messageListHash(messages []domain.Message) int64 {
	if len(messages) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [24]byte
	for _, msg := range messages {
		binary.LittleEndian.PutUint32(buf[:4], uint32(msg.ID))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(msg.Date))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(msg.From.ID))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(msg.UID))
		_, _ = h.Write(buf[:])
		writeMessageReactionsHash(h, msg.Reactions)
	}
	return int64(h.Sum64())
}

func writeMessageReactionsHash(h hash.Hash64, reactions *domain.ChannelMessageReactions) {
	if reactions == nil {
		_, _ = h.Write([]byte{0})
		return
	}
	var buf [16]byte
	for _, item := range reactions.Results {
		_, _ = h.Write([]byte(item.Reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(item.Reaction.Emoticon))
		_, _ = h.Write([]byte{0})
		binary.LittleEndian.PutUint32(buf[:4], uint32(item.Count))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(item.ChosenOrder))
		_, _ = h.Write(buf[:8])
	}
	_, _ = h.Write([]byte{0xfe})
	for _, item := range reactions.Recent {
		_, _ = h.Write([]byte(item.Reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(item.Reaction.Emoticon))
		_, _ = h.Write([]byte{0})
		binary.LittleEndian.PutUint64(buf[:8], uint64(item.UserID))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(item.Date))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(item.ChosenOrder))
		_, _ = h.Write(buf[:])
	}
}
