package store

import "context"

// PtsAllocator 分配用户级 pts。实现必须保证同一 user 内单调递增。
type PtsAllocator interface {
	NextPts(ctx context.Context, userID int64) (int, error)
	CurrentPts(ctx context.Context, userID int64) (int, error)
}

// PtsRangeAllocator 可一次性分配一段连续 pts，返回该段最终 pts。
// 批量事件（例如 updateDeleteMessages）必须用 pts_count 推进多步时使用它。
type PtsRangeAllocator interface {
	NextPtsN(ctx context.Context, userID int64, count int) (int, error)
}

// BoxIDAllocator 分配用户视角 message box id。box_id 允许空洞，但不能回退。
type BoxIDAllocator interface {
	NextBoxID(ctx context.Context, userID int64) (int, error)
	CurrentBoxID(ctx context.Context, userID int64) (int, error)
}

// CounterSource 用于 Redis 计数器冷启动时从 PostgreSQL durable log 恢复当前值。
type CounterSource interface {
	Current(ctx context.Context, userID int64) (int, error)
}
