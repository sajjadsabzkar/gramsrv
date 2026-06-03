package postgres

import (
	"context"
	"fmt"

	"telesrv/internal/store/postgres/sqlcgen"
)

// MessageBoxCounterSource 从 message_boxes durable log 恢复某 owner 的当前最大 box_id。
type MessageBoxCounterSource struct {
	q *sqlcgen.Queries
}

// NewMessageBoxCounterSource 创建 Redis BoxIDAllocator 的 PG 恢复源。
func NewMessageBoxCounterSource(db sqlcgen.DBTX) *MessageBoxCounterSource {
	return &MessageBoxCounterSource{q: sqlcgen.New(db)}
}

func (s *MessageBoxCounterSource) Current(ctx context.Context, userID int64) (int, error) {
	v, err := s.q.MaxMessageBoxID(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("max message box id: %w", err)
	}
	return int(v), nil
}

// ChannelIDCounterSource 从 channels durable 表恢复全局 channel id。
type ChannelIDCounterSource struct {
	db sqlcgen.DBTX
}

// NewChannelIDCounterSource 创建 Redis ChannelIDAllocator 的 PG 恢复源。
func NewChannelIDCounterSource(db sqlcgen.DBTX) *ChannelIDCounterSource {
	return &ChannelIDCounterSource{db: db}
}

func (s *ChannelIDCounterSource) Current(ctx context.Context, _ int64) (int, error) {
	var id int
	if err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channels`).Scan(&id); err != nil {
		return 0, fmt.Errorf("max channel id: %w", err)
	}
	return id, nil
}

// ChannelPtsCounterSource 从 channel_update_events 恢复某 channel 的当前最大 pts。
type ChannelPtsCounterSource struct {
	db sqlcgen.DBTX
}

func NewChannelPtsCounterSource(db sqlcgen.DBTX) *ChannelPtsCounterSource {
	return &ChannelPtsCounterSource{db: db}
}

func (s *ChannelPtsCounterSource) Current(ctx context.Context, channelID int64) (int, error) {
	var pts int
	if err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(pts), 0) FROM channel_update_events WHERE channel_id = $1`, channelID).Scan(&pts); err != nil {
		return 0, fmt.Errorf("max channel pts: %w", err)
	}
	return pts, nil
}

// ChannelMessageIDCounterSource 从 channel_messages 恢复某 channel 的当前最大 message id。
type ChannelMessageIDCounterSource struct {
	db sqlcgen.DBTX
}

func NewChannelMessageIDCounterSource(db sqlcgen.DBTX) *ChannelMessageIDCounterSource {
	return &ChannelMessageIDCounterSource{db: db}
}

func (s *ChannelMessageIDCounterSource) Current(ctx context.Context, channelID int64) (int, error) {
	var id int
	if err := s.db.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM channel_messages WHERE channel_id = $1`, channelID).Scan(&id); err != nil {
		return 0, fmt.Errorf("max channel message id: %w", err)
	}
	return id, nil
}
