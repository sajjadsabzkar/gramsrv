package store

import (
	"context"

	"telesrv/internal/domain"
)

// ContactStore 持久化用户通讯录。
type ContactStore interface {
	ListByUser(ctx context.Context, userID int64) (domain.ContactList, error)
	Get(ctx context.Context, userID, contactUserID int64) (domain.Contact, bool, error)
	Upsert(ctx context.Context, userID int64, input domain.ContactInput) (domain.Contact, error)
	UpsertMany(ctx context.Context, userID int64, inputs []domain.ContactInput) ([]domain.Contact, error)
	UpdateNote(ctx context.Context, userID, contactUserID int64, note string, entities []domain.MessageEntity) (domain.Contact, bool, error)
	Delete(ctx context.Context, userID int64, contactUserIDs []int64) (int, error)
	Block(ctx context.Context, userID, blockedUserID int64, date int) (bool, error)
	Unblock(ctx context.Context, userID, blockedUserID int64) (bool, error)
	IsBlocked(ctx context.Context, userID, blockedUserID int64) (bool, error)
	ListBlocked(ctx context.Context, userID int64, offset, limit int) (domain.BlockedContactList, error)
}
