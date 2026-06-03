package postgres

import (
	"context"
	"fmt"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// defaultDispatchLease 是 'dispatching' 行被判定租约过期、可被重新 claim 的默认时长。
// 与 docs/message-module.md 的 outbox 背压参数对应；生产由 config 注入覆盖。
const defaultDispatchLease = 30 * time.Second

// DispatchOutboxStore 用 PostgreSQL 实现 transactional outbox。
type DispatchOutboxStore struct {
	q            *sqlcgen.Queries
	leaseSeconds int32
}

// DispatchOutboxOption 调整 DispatchOutboxStore 的 claim 行为。
type DispatchOutboxOption func(*DispatchOutboxStore)

// WithLeaseTimeout 设置租约超时；<=0 时保持默认。
func WithLeaseTimeout(d time.Duration) DispatchOutboxOption {
	return func(s *DispatchOutboxStore) {
		if d > 0 {
			s.leaseSeconds = int32(d / time.Second)
			if s.leaseSeconds < 1 {
				s.leaseSeconds = 1
			}
		}
	}
}

// NewDispatchOutboxStore 基于 pgx 连接池（或事务）创建 DispatchOutboxStore。
func NewDispatchOutboxStore(db sqlcgen.DBTX, opts ...DispatchOutboxOption) *DispatchOutboxStore {
	s := &DispatchOutboxStore{
		q:            sqlcgen.New(db),
		leaseSeconds: int32(defaultDispatchLease / time.Second),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *DispatchOutboxStore) ClaimPending(ctx context.Context, limit int) ([]store.DispatchOutboxItem, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.q.ClaimDispatchOutbox(ctx, sqlcgen.ClaimDispatchOutboxParams{
		LeaseSeconds: s.leaseSeconds,
		LimitCount:   int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("claim dispatch outbox: %w", err)
	}
	out := make([]store.DispatchOutboxItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, store.DispatchOutboxItem{
			ID:               row.ID,
			TargetUserID:     row.TargetUserID,
			Pts:              int(row.Pts),
			EventType:        domain.UpdateEventType(row.EventType),
			ExcludeAuthKeyID: authKeyIDFromInt64(row.ExcludeAuthKeyID),
			ExcludeSessionID: row.ExcludeSessionID,
			Attempts:         int(row.Attempts),
		})
	}
	return out, nil
}

// MarkDeliveredBatch 一次性删除一批已投递的 outbox 行（方案 A：投递成功即删），取代逐条 MarkDelivered。
func (s *DispatchOutboxStore) MarkDeliveredBatch(ctx context.Context, items []store.DispatchOutboxItem) error {
	if len(items) == 0 {
		return nil
	}
	targetUserIDs := make([]int64, len(items))
	ids := make([]int64, len(items))
	for i, it := range items {
		targetUserIDs[i] = it.TargetUserID
		ids[i] = it.ID
	}
	if err := s.q.MarkDispatchDeliveredBatch(ctx, sqlcgen.MarkDispatchDeliveredBatchParams{
		TargetUserIds: targetUserIDs,
		Ids:           ids,
	}); err != nil {
		return fmt.Errorf("mark dispatch delivered batch: %w", err)
	}
	return nil
}

func (s *DispatchOutboxStore) MarkDelivered(ctx context.Context, targetUserID, id int64) error {
	if err := s.q.MarkDispatchDelivered(ctx, sqlcgen.MarkDispatchDeliveredParams{
		TargetUserID: targetUserID,
		ID:           id,
	}); err != nil {
		return fmt.Errorf("mark dispatch delivered: %w", err)
	}
	return nil
}

func (s *DispatchOutboxStore) MarkFailed(ctx context.Context, targetUserID, id int64, lastError string) error {
	if err := s.q.MarkDispatchFailed(ctx, sqlcgen.MarkDispatchFailedParams{
		TargetUserID: targetUserID,
		ID:           id,
		LastError:    lastError,
	}); err != nil {
		return fmt.Errorf("mark dispatch failed: %w", err)
	}
	return nil
}

func (s *DispatchOutboxStore) DeleteFailed(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if olderThan <= 0 {
		olderThan = 24 * time.Hour
	}
	if limit <= 0 {
		limit = 10000
	}
	if limit > 100000 {
		limit = 100000
	}
	deleted, err := s.q.DeleteFailedDispatchOutbox(ctx, sqlcgen.DeleteFailedDispatchOutboxParams{
		OlderThanSeconds: int32(olderThan / time.Second),
		LimitCount:       int32(limit),
	})
	if err != nil {
		return 0, fmt.Errorf("delete failed dispatch outbox: %w", err)
	}
	return int(deleted), nil
}
