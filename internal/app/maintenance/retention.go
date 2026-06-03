package maintenance

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// DispatchOutboxRetentionStore 清理彻底失败（已放弃重试）的 outbox 死任务。
type DispatchOutboxRetentionStore interface {
	DeleteFailed(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// RetentionWorker 周期性回收存储中的死数据。
//
// 注意：本 worker 刻意不清理 user_update_events —— pts log 永久保留。原因：TDesktop 不支持
// 账号级 updates.differenceTooLong（api_updates.cpp 收到该响应只打一行日志，且漏掉
// setRequesting(false)，会永久锁死整个 update 引擎），服务端因此无法让"落后超过保留期"的
// 客户端整库重置；一旦裁剪 events，落后客户端的 getDifference 会拿到不完整的事件链而静默
// 丢消息。详见 docs/performance-audit.md 与 docs/compatibility-matrix.md。user_update_events
// 长期膨胀作为已知 todo。
type RetentionWorker struct {
	outbox    DispatchOutboxRetentionStore
	logger    *zap.Logger
	retention time.Duration
	interval  time.Duration
	batch     int
}

func NewRetentionWorker(outbox DispatchOutboxRetentionStore, logger *zap.Logger, retention, interval time.Duration, batch int) *RetentionWorker {
	if logger == nil {
		logger = zap.NewNop()
	}
	if retention <= 0 {
		retention = 168 * time.Hour
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if batch <= 0 {
		batch = 10000
	}
	return &RetentionWorker{
		outbox:    outbox,
		logger:    logger,
		retention: retention,
		interval:  interval,
		batch:     batch,
	}
}

func (w *RetentionWorker) Run(ctx context.Context) {
	w.runOnce(ctx)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *RetentionWorker) runOnce(ctx context.Context) {
	outboxDeleted, err := w.outbox.DeleteFailed(ctx, w.retention, w.batch)
	if err != nil {
		w.logger.Warn("清理 failed dispatch_outbox 失败", zap.Error(err))
	} else if outboxDeleted > 0 {
		w.logger.Info("清理 failed dispatch_outbox 完成", zap.Int("deleted", outboxDeleted))
	}
}
