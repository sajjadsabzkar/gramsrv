package messages

import (
	"context"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service 提供消息历史、搜索与已读业务。
type Service struct {
	messages store.MessageStore
	dialogs  store.DialogStore
}

// NewService 创建 messages 服务。
func NewService(messages store.MessageStore, dialogs store.DialogStore) *Service {
	return &Service{messages: messages, dialogs: dialogs}
}

// SendPrivateText 发送一条私聊文本消息。
func (s *Service) SendPrivateText(ctx context.Context, userID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.SendPrivateTextResult{}, nil
	}
	if req.SenderUserID == 0 {
		req.SenderUserID = userID
	}
	return s.messages.SendPrivateText(ctx, req)
}

// ForwardPrivateMessages 转发当前账号可见的私聊文本消息。
func (s *Service) ForwardPrivateMessages(ctx context.Context, userID int64, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.ForwardPrivateMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.ForwardPrivateMessages(ctx, req)
}

// GetMessages returns exact owner-visible message boxes in request order.
func (s *Service) GetMessages(ctx context.Context, userID int64, ids []int) (domain.MessageList, error) {
	if s == nil || s.messages == nil || userID == 0 || len(ids) == 0 {
		return domain.MessageList{}, nil
	}
	return s.messages.GetByIDs(ctx, userID, ids)
}

// GetHistory 返回当前账号某个 peer 的历史消息。
func (s *Service) GetHistory(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	return s.list(ctx, userID, filter)
}

// Search 返回当前账号消息搜索结果。Query 为空时等价于历史列表过滤。
func (s *Service) Search(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	return s.list(ctx, userID, filter)
}

// ReadHistory 将当前账号某个 peer 的 inbox 标记为已读，并为发送方生成 outbox 已读回执。
func (s *Service) ReadHistory(ctx context.Context, userID int64, req domain.ReadHistoryRequest) (domain.ReadHistoryResult, error) {
	if s == nil || userID == 0 {
		return domain.ReadHistoryResult{Peer: req.Peer, MaxID: req.MaxID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if s.messages != nil {
		return s.messages.ReadHistory(ctx, req)
	}
	if s.dialogs != nil {
		return s.dialogs.MarkRead(ctx, userID, req.Peer, req.MaxID)
	}
	return domain.ReadHistoryResult{OwnerUserID: userID, Peer: req.Peer, MaxID: req.MaxID}, nil
}

// ReadMessageContents checks exact owner-visible private message IDs for content-read sync.
func (s *Service) ReadMessageContents(ctx context.Context, userID int64, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error) {
	res := domain.ReadMessageContentsResult{OwnerUserID: userID}
	if s == nil || s.messages == nil || userID == 0 {
		return res, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID || len(req.IDs) > domain.MaxGetMessageIDs {
		return res, domain.ErrMessageIDInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return res, domain.ErrMessageIDInvalid
		}
	}
	return s.messages.ReadMessageContents(ctx, req)
}

// GetOutboxReadDate 返回当前账号某条 outgoing 私聊消息被对端读到的时间。
func (s *Service) GetOutboxReadDate(ctx context.Context, userID int64, req domain.OutboxReadDateRequest) (int, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return 0, domain.ErrMessageIDInvalid
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.GetOutboxReadDate(ctx, req)
}

// SetMessageReactions replaces the current user's reactions on a visible private message.
func (s *Service) SetMessageReactions(ctx context.Context, userID int64, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if req.UserID == 0 {
		req.UserID = userID
	}
	if req.UserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	return s.messages.SetMessageReactions(ctx, req)
}

// GetMessageReactions returns reaction summaries for visible private messages.
func (s *Service) GetMessageReactions(ctx context.Context, userID int64, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.PrivateMessageReactionsResult{}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	if req.OwnerUserID != userID || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
	}
	return s.messages.GetMessageReactions(ctx, req)
}

// EditMessage 编辑当前账号发出的私聊文本消息。
func (s *Service) EditMessage(ctx context.Context, userID int64, req domain.EditMessageRequest) (domain.EditMessageResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.EditMessageResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.EditMessage(ctx, req)
}

// DeleteMessages 删除当前账号视角下的一组消息；revoke 时同步删除对端私聊盒子。
func (s *Service) DeleteMessages(ctx context.Context, userID int64, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.DeleteMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.DeleteMessages(ctx, req)
}

// DeleteHistory 清空当前账号与某个 peer 的历史；revoke 时同步删除对端私聊盒子。
func (s *Service) DeleteHistory(ctx context.Context, userID int64, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.DeleteMessagesResult{OwnerUserID: userID}, nil
	}
	if req.OwnerUserID == 0 {
		req.OwnerUserID = userID
	}
	return s.messages.DeleteHistory(ctx, req)
}

func (s *Service) list(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	if s == nil || s.messages == nil || userID == 0 {
		return domain.MessageList{}, nil
	}
	return s.messages.ListByUser(ctx, userID, filter)
}
