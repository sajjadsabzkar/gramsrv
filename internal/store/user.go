package store

import (
	"context"

	"telesrv/internal/domain"
)

// UserStore 持久化用户。实现见 store/memory（测试替身）、store/postgres。
type UserStore interface {
	ByID(ctx context.Context, id int64) (domain.User, bool, error)
	ByIDs(ctx context.Context, ids []int64) ([]domain.User, error)
	ByPhone(ctx context.Context, phone string) (domain.User, bool, error)
	ByPhones(ctx context.Context, phones []string) ([]domain.User, error)
	ByUsername(ctx context.Context, username string) (domain.User, bool, error)
	Search(ctx context.Context, currentUserID int64, query, phoneQuery string, limit int) (domain.UserSearchResult, error)
	UpdateProfile(ctx context.Context, userID int64, firstName, lastName, about string) (domain.User, error)
	UpdateUsername(ctx context.Context, userID int64, username string) (domain.User, error)
	UpdateLastSeen(ctx context.Context, userID int64, lastSeenAt int) error
	// Create 创建用户并返回分配了 ID 的副本。
	Create(ctx context.Context, u domain.User) (domain.User, error)
}
