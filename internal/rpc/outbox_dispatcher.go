package rpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const (
	defaultOutboxBatch    = 100
	defaultOutboxInterval = 200 * time.Millisecond
	defaultOutboxWorkers  = 2
)

var errMissingOutboxEvent = errors.New("missing outbox update event")

// OutboxDispatcher 把 PG transactional outbox 中的 update 批量推给在线 session。
// 多 worker 并发 claim：ClaimPending 用 FOR UPDATE SKIP LOCKED，worker 间认领不重叠。
type OutboxDispatcher struct {
	events      store.UpdateEventStore
	outbox      store.DispatchOutboxStore
	sessions    SessionBinder
	log         *zap.Logger
	metrics     Metrics
	batch       int
	interval    time.Duration
	workers     int
	pushTimeout time.Duration
}

// OutboxOption 调整 OutboxDispatcher 的运行参数。
type OutboxOption func(*OutboxDispatcher)

// WithOutboxBatch 设置每次 claim 的最大条数；<=0 时保持默认。
func WithOutboxBatch(n int) OutboxOption {
	return func(d *OutboxDispatcher) {
		if n > 0 {
			d.batch = n
		}
	}
}

// WithOutboxInterval 设置两次 claim 之间的轮询间隔；<=0 时保持默认。
func WithOutboxInterval(interval time.Duration) OutboxOption {
	return func(d *OutboxDispatcher) {
		if interval > 0 {
			d.interval = interval
		}
	}
}

// WithOutboxWorkers 设置并发 claim worker 数；<=0 时保持默认。
func WithOutboxWorkers(n int) OutboxOption {
	return func(d *OutboxDispatcher) {
		if n > 0 {
			d.workers = n
		}
	}
}

// WithOutboxPushTimeout 设置 updates fanout 入队等待时间；<=0 时使用同步可靠推送。
func WithOutboxPushTimeout(timeout time.Duration) OutboxOption {
	return func(d *OutboxDispatcher) {
		if timeout > 0 {
			d.pushTimeout = timeout
		}
	}
}

// WithOutboxMetrics 注入指标实现；nil 时保持 NopMetrics。
func WithOutboxMetrics(m Metrics) OutboxOption {
	return func(d *OutboxDispatcher) {
		if m != nil {
			d.metrics = m
		}
	}
}

// NewOutboxDispatcher 创建在线 update 推送 worker。batch/interval 默认值见
// defaultOutboxBatch/defaultOutboxInterval，可经 WithOutbox* 选项覆盖（生产由 config 注入）。
func NewOutboxDispatcher(events store.UpdateEventStore, outbox store.DispatchOutboxStore, sessions SessionBinder, log *zap.Logger, opts ...OutboxOption) *OutboxDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	d := &OutboxDispatcher{
		events:   events,
		outbox:   outbox,
		sessions: sessions,
		log:      log,
		metrics:  NopMetrics{},
		batch:    defaultOutboxBatch,
		interval: defaultOutboxInterval,
		workers:  defaultOutboxWorkers,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

// Run 启动 workers 个并发 worker 持续 claim pending outbox；ctx 退出时全部停止并等待退出。
func (d *OutboxDispatcher) Run(ctx context.Context) {
	if d == nil || d.events == nil || d.outbox == nil || d.sessions == nil {
		return
	}
	workers := d.workers
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			d.runWorker(ctx)
		}()
	}
	wg.Wait()
}

// runWorker 是单个 claim 循环；多 worker 靠 ClaimPending 的 SKIP LOCKED 互不重叠。
func (d *OutboxDispatcher) runWorker(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		d.DispatchOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// batchEventLoader 是 UpdateEventStore 的可选批量能力：一次取多条 (user,pts) 事件。
type batchEventLoader interface {
	BatchByCursor(ctx context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error)
}

// batchOutboxMarker 是 DispatchOutboxStore 的可选批量能力：一次标记多行 delivered。
type batchOutboxMarker interface {
	MarkDeliveredBatch(ctx context.Context, items []store.DispatchOutboxItem) error
}

// DispatchOnce claim 一批 outbox 并投递，测试可直接调用。
// store 同时具备批量取事件 + 批量标记能力时走批量路径（每批 ~3 次 PG 往返），否则逐条回退。
func (d *OutboxDispatcher) DispatchOnce(ctx context.Context) {
	items, err := d.outbox.ClaimPending(ctx, d.batch)
	if err != nil {
		d.log.Warn("claim dispatch outbox", zap.Error(err))
		return
	}
	if len(items) == 0 {
		return
	}
	d.metrics.OutboxClaimed(len(items))
	if loader, ok := d.events.(batchEventLoader); ok {
		if marker, ok := d.outbox.(batchOutboxMarker); ok {
			d.dispatchBatch(ctx, items, loader, marker)
			return
		}
	}
	for _, item := range items {
		d.dispatchItem(ctx, item)
	}
}

type outboxEventKey struct {
	userID int64
	pts    int
}

// dispatchBatch 批量加载已 claim 事件、逐条 push、批量标记 delivered；失败项单独退避重试。
func (d *OutboxDispatcher) dispatchBatch(ctx context.Context, items []store.DispatchOutboxItem, loader batchEventLoader, marker batchOutboxMarker) {
	cursors := make([]store.EventCursor, len(items))
	for i, item := range items {
		cursors[i] = store.EventCursor{UserID: item.TargetUserID, Pts: item.Pts}
	}
	events, err := loader.BatchByCursor(ctx, cursors)
	if err != nil {
		// 批量取失败则整批回退逐条路径，让每条各自重试/标失败，不丢进度。
		d.log.Warn("batch load dispatch events", zap.Error(err))
		for _, item := range items {
			d.dispatchItem(ctx, item)
		}
		return
	}
	byKey := make(map[outboxEventKey]domain.UpdateEvent, len(events))
	for _, event := range events {
		byKey[outboxEventKey{event.UserID, event.Pts}] = event
	}
	start := time.Now()
	delivered := make([]store.DispatchOutboxItem, 0, len(items))
	for _, item := range items {
		event, ok := byKey[outboxEventKey{item.TargetUserID, item.Pts}]
		if !ok {
			d.markDispatchFailed(ctx, item, errMissingOutboxEvent)
			continue
		}
		update := tgUpdateForOutboxEvent(event)
		if update == nil {
			delivered = append(delivered, item)
			continue
		}
		if _, retriable, err := d.pushOutboxUpdate(ctx, item, update); err != nil {
			if retriable {
				// 出站队列拥塞：留 dispatching 行靠租约过期重投，不计入 attempts 升级。
				// 不加入 delivered，故不会被 MarkDeliveredBatch 删除。
				continue
			}
			d.markDispatchFailed(ctx, item, err)
			continue
		}
		delivered = append(delivered, item)
	}
	if len(delivered) == 0 {
		return
	}
	if err := marker.MarkDeliveredBatch(ctx, delivered); err != nil {
		// 批量标记失败则逐条标记，避免整批已投递却卡在 dispatching 等租约过期重投。
		d.log.Warn("mark dispatch delivered batch", zap.Error(err))
		for _, item := range delivered {
			if markErr := d.outbox.MarkDelivered(ctx, item.TargetUserID, item.ID); markErr != nil {
				d.log.Warn("mark dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(markErr))
			}
		}
	}
	per := time.Since(start) / time.Duration(len(delivered))
	for range delivered {
		d.metrics.OutboxDelivered(per)
	}
}

func (d *OutboxDispatcher) dispatchItem(ctx context.Context, item store.DispatchOutboxItem) {
	start := time.Now()
	events, err := d.events.ListAfter(ctx, item.TargetUserID, item.Pts-1, 1)
	if err != nil {
		d.markDispatchFailed(ctx, item, err)
		return
	}
	if len(events) == 0 || events[0].Pts != item.Pts {
		d.markDispatchFailed(ctx, item, errMissingOutboxEvent)
		return
	}
	update := tgUpdateForOutboxEvent(events[0])
	if update == nil {
		if err := d.outbox.MarkDelivered(ctx, item.TargetUserID, item.ID); err != nil {
			d.log.Warn("mark noop dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
			return
		}
		d.metrics.OutboxDelivered(time.Since(start))
		return
	}
	sent, retriable, err := d.pushOutboxUpdate(ctx, item, update)
	if err != nil {
		if retriable {
			// 出站队列拥塞：保留 dispatching 行，靠租约过期（defaultDispatchLease）重新 claim 重投，
			// 不计入 attempts 升级，避免正常满 fan-out 负载把可靠 update 误打成 failed。
			d.log.Debug("dispatch outbox deferred (push queue full)",
				zap.Int64("target_user_id", item.TargetUserID),
				zap.Int64("outbox_id", item.ID),
				zap.Int("pts", item.Pts),
			)
			return
		}
		d.markDispatchFailed(ctx, item, err)
		return
	}
	if err := d.outbox.MarkDelivered(ctx, item.TargetUserID, item.ID); err != nil {
		d.log.Warn("mark dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
		return
	}
	d.metrics.OutboxDelivered(time.Since(start))
	d.log.Debug("dispatch outbox delivered",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("pts", item.Pts),
		zap.Int("sessions", sent),
	)
}

// pushOutboxUpdate 投递一条 outbox update，返回 (送达的在线 session 数, 是否可重试, err)。
// best-effort 路径（pushTimeout>0）的失败只可能是出站队列拥塞（慢消费者入队超时），属暂时性、
// 可重试：调用方应保留 dispatching 行靠租约过期重投，而非计入 attempts 升级为 failed。
// 可靠路径的失败是真实投递错误，retriable=false，按原逻辑退避升级。
func (d *OutboxDispatcher) pushOutboxUpdate(ctx context.Context, item store.DispatchOutboxItem, update *tg.Updates) (sent int, retriable bool, err error) {
	var zeroAuthKeyID [8]byte
	if d.pushTimeout > 0 {
		if scoped, ok := d.sessions.(ScopedBestEffortSessionBinder); ok && item.ExcludeAuthKeyID != zeroAuthKeyID {
			sent, err = scoped.PushToUserExceptAuthKeySessionBestEffort(ctx, item.TargetUserID, item.ExcludeAuthKeyID, item.ExcludeSessionID, proto.MessageFromServer, update, d.pushTimeout)
			return sent, err != nil, err
		}
		if bestEffort, ok := d.sessions.(BestEffortSessionBinder); ok {
			sent, err = bestEffort.PushToUserExceptSessionBestEffort(ctx, item.TargetUserID, item.ExcludeSessionID, proto.MessageFromServer, update, d.pushTimeout)
			return sent, err != nil, err
		}
	}
	if scoped, ok := d.sessions.(ScopedSessionBinder); ok && item.ExcludeAuthKeyID != zeroAuthKeyID {
		sent, err = scoped.PushToUserExceptAuthKeySession(ctx, item.TargetUserID, item.ExcludeAuthKeyID, item.ExcludeSessionID, proto.MessageFromServer, update)
		return sent, false, err
	}
	sent, err = d.sessions.PushToUserExceptSession(ctx, item.TargetUserID, item.ExcludeSessionID, proto.MessageFromServer, update)
	return sent, false, err
}

func (d *OutboxDispatcher) markDispatchFailed(ctx context.Context, item store.DispatchOutboxItem, err error) {
	if err == nil {
		err = errMissingOutboxEvent
	}
	if markErr := d.outbox.MarkFailed(ctx, item.TargetUserID, item.ID, err.Error()); markErr != nil {
		d.log.Warn("mark dispatch failed",
			zap.Int64("target_user_id", item.TargetUserID),
			zap.Int64("outbox_id", item.ID),
			zap.Error(markErr),
		)
		return
	}
	d.metrics.OutboxFailed(err)
	d.log.Debug("dispatch outbox failed",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("pts", item.Pts),
		zap.Error(err),
	)
}

func tgUpdateForOutboxEvent(event domain.UpdateEvent) *tg.Updates {
	switch event.Type {
	case domain.UpdateEventNewMessage:
		return tgPrivateMessageUpdates(event, event.Message, 0, false, tgUsers(event.Users), tgChannels(event.UserID, event.Channels))
	case domain.UpdateEventReadHistoryInbox, domain.UpdateEventReadHistoryOutbox:
		var update tg.UpdateClass
		if event.Type == domain.UpdateEventReadHistoryOutbox {
			update = tgReadHistoryOutboxUpdate(event)
		} else {
			update = tgReadHistoryInboxUpdate(event)
		}
		if update == nil {
			return nil
		}
		return &tg.Updates{
			Updates: []tg.UpdateClass{update},
			Date:    event.Date,
			Seq:     0, // 私聊不维护账号级 seq，恒 0
		}
	case domain.UpdateEventNoop:
		return nil
	default:
		update := tgOtherUpdateFromEvent(event)
		if update == nil {
			return nil
		}
		return &tg.Updates{
			Updates: []tg.UpdateClass{update},
			Date:    event.Date,
			Seq:     0,
		}
	}
}
