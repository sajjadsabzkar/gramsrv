package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// DispatchOutboxItem 是待投递给在线 session 的 update 任务。
type DispatchOutboxItem struct {
	ID               int64
	TargetUserID     int64
	Pts              int
	EventType        domain.UpdateEventType
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Attempts         int
}

// DispatchOutboxStore 持久化 transactional outbox。
type DispatchOutboxStore interface {
	ClaimPending(ctx context.Context, limit int) ([]DispatchOutboxItem, error)
	MarkDelivered(ctx context.Context, targetUserID, id int64) error
	MarkFailed(ctx context.Context, targetUserID, id int64, lastError string) error
	DeleteFailed(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}
