package store

import (
	"context"

	"telesrv/internal/domain"
)

// UpdateEventStore 持久化 user 维度的增量事件。
type UpdateEventStore interface {
	Append(ctx context.Context, userID int64, event domain.UpdateEvent) error
	ListAfter(ctx context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error)
	Current(ctx context.Context, userID int64) (int, error)
	// MaxContiguousPts 返回从 1 起无空洞的最大已提交 pts。
	// 并发发送在途时（pts 已分配未提交）会暂时小于 Current（最大已提交），空洞提交/补洞后回升。
	// getState/getDifference 据此只暴露连续 pts，避免客户端跳过在途空洞而丢消息。
	MaxContiguousPts(ctx context.Context, userID int64) (int, error)
	// AdvanceContiguousPts 推进并返回账号级连续 pts 水位。实现应在写事件的同一事务内调用。
	AdvanceContiguousPts(ctx context.Context, userID int64) (int, error)
}

// EventCursor 精确定位单条账号事件，用于 outbox worker 批量加载已 claim 事件。
type EventCursor struct {
	UserID int64
	Pts    int
}
