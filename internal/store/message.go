package store

import (
	"context"

	"telesrv/internal/domain"
)

// MessageStore 持久化账号视角下的消息。
type MessageStore interface {
	Create(ctx context.Context, msg domain.Message) (domain.Message, error)
	SendPrivateText(ctx context.Context, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error)
	ForwardPrivateMessages(ctx context.Context, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error)
	ReadHistory(ctx context.Context, req domain.ReadHistoryRequest) (domain.ReadHistoryResult, error)
	ReadMessageContents(ctx context.Context, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error)
	GetOutboxReadDate(ctx context.Context, req domain.OutboxReadDateRequest) (int, error)
	SetMessageReactions(ctx context.Context, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error)
	GetMessageReactions(ctx context.Context, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error)
	EditMessage(ctx context.Context, req domain.EditMessageRequest) (domain.EditMessageResult, error)
	DeleteMessages(ctx context.Context, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error)
	DeleteHistory(ctx context.Context, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error)
	GetByIDs(ctx context.Context, userID int64, ids []int) (domain.MessageList, error)
	ListByUser(ctx context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error)
}
