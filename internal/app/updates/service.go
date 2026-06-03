package updates

import (
	"context"
	"sort"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Service 提供 update 状态查询。
type Service struct {
	states store.UpdateStateStore
	events store.UpdateEventStore
	pts    store.PtsAllocator
}

type dispatchingEventAppender interface {
	AppendWithDispatch(ctx context.Context, userID int64, event domain.UpdateEvent, excludeAuthKeyID [8]byte, excludeSessionID int64) error
}

// ServiceOption 调整 updates 服务的运行时依赖。
type ServiceOption func(*Service)

// WithPtsAllocator 使用外部 pts 分配器推进账号级 pts。
func WithPtsAllocator(pts store.PtsAllocator) ServiceOption {
	return func(s *Service) {
		s.pts = pts
	}
}

// NewService 创建 updates 服务。
func NewService(states store.UpdateStateStore, events store.UpdateEventStore, opts ...ServiceOption) *Service {
	s := &Service{states: states}
	s.events = events
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// UsesReliableDispatch 表示设置类 update 已写入 transactional outbox，由 outbox worker 投递在线 session。
func (s *Service) UsesReliableDispatch() bool {
	if s == nil || s.events == nil {
		return false
	}
	_, ok := s.events.(dispatchingEventAppender)
	return ok
}

// GetState 返回当前 auth_key + user 维度已确认的 update 状态。
// user_update_events 是账号级 durable log；auth_key 维度只保存设备已经通过
// getDifference 确认到的状态，不能在 getState 中直接推进到账号最新水位。
func (s *Service) GetState(ctx context.Context, authKeyID [8]byte, userID int64) (domain.UpdateState, error) {
	now := int(time.Now().Unix())
	// 私聊阶段不维护账号级 seq：对外 UpdateState.Seq 恒为 0，客户端仅靠 pts 同步、
	// 跳过 seq gap 检测（推送信封 seq 同样恒 0）。
	if s.states == nil {
		current, err := s.currentPts(ctx, userID)
		if err != nil {
			return domain.UpdateState{}, err
		}
		return domain.UpdateState{Pts: current, Date: now, Seq: 0}, nil
	}
	st, found, err := s.states.Get(ctx, authKeyID, userID)
	if err != nil {
		return domain.UpdateState{}, err
	}
	if found {
		st.Seq = 0
		if st.Date == 0 {
			st.Date = now
		}
		return st, nil
	}
	current, err := s.currentPts(ctx, userID)
	if err != nil {
		return domain.UpdateState{}, err
	}
	st = domain.UpdateState{Pts: current, Date: now, Seq: 0}
	if err := s.states.Save(ctx, authKeyID, userID, st); err != nil {
		return domain.UpdateState{}, err
	}
	return st, nil
}

// CurrentState 返回账号当前最大连续 update 状态，不修改任何设备已确认水位。
func (s *Service) CurrentState(ctx context.Context, userID int64) (domain.UpdateState, error) {
	return s.currentState(ctx, userID)
}

// getDifferenceLimit 是单次 getDifference 返回的最大连续事件数；超出置 Partial 让客户端翻页。
const getDifferenceLimit = 100

// GetDifference 返回当前 user 从 from 状态之后的增量事件。
//
// 对齐 MTProto：只返回从 from.Pts 起「连续」的事件（遇空洞即截断），State.Pts 取最后连续值，
// 绝不让客户端跳过在途空洞而丢消息——空洞由并发发送的在途事务造成，提交/补洞后客户端下次拉取即可补齐。
// 连续事件填满 limit 时置 Partial（映射 differenceSlice），客户端据返回 State 继续翻页。
func (s *Service) GetDifference(ctx context.Context, authKeyID [8]byte, userID int64, from domain.UpdateState) (domain.UpdateDifference, error) {
	st, err := s.currentState(ctx, userID)
	if err != nil {
		return domain.UpdateDifference{}, err
	}
	if s.events == nil || from.Pts >= st.Pts {
		if from.Date != 0 {
			st.Date = from.Date
		}
		if err := s.saveConfirmedState(ctx, authKeyID, userID, st); err != nil {
			return domain.UpdateDifference{}, err
		}
		return domain.UpdateDifference{State: st}, nil
	}
	events, err := s.events.ListAfter(ctx, userID, from.Pts, getDifferenceLimit)
	if err != nil {
		return domain.UpdateDifference{}, err
	}
	contiguous := contiguousPrefix(events, from.Pts)
	last := from.Pts
	if len(contiguous) > 0 {
		last = contiguous[len(contiguous)-1].Pts
	}
	out := st
	out.Pts = last
	out.Seq = 0 // seq 恒 0，见 GetState 注释
	if len(contiguous) > 0 {
		out.Date = contiguous[len(contiguous)-1].Date
	}
	if err := s.saveConfirmedState(ctx, authKeyID, userID, out); err != nil {
		return domain.UpdateDifference{}, err
	}
	return domain.UpdateDifference{
		State:   out,
		Events:  contiguous,
		Partial: len(contiguous) == getDifferenceLimit,
	}, nil
}

func (s *Service) currentState(ctx context.Context, userID int64) (domain.UpdateState, error) {
	current, err := s.currentPts(ctx, userID)
	if err != nil {
		return domain.UpdateState{}, err
	}
	return domain.UpdateState{
		Pts:  current,
		Date: int(time.Now().Unix()),
		Seq:  0,
	}, nil
}

func (s *Service) saveConfirmedState(ctx context.Context, authKeyID [8]byte, userID int64, st domain.UpdateState) error {
	if s.states == nil {
		return nil
	}
	st.Seq = 0
	return s.states.Save(ctx, authKeyID, userID, st)
}

// contiguousPrefix 返回从 from 起 pts 严格连续（from+1, from+2, ...）的事件前缀。
// 先按 pts 升序排序以兼容存储返回顺序，遇到空洞即停。
func contiguousPrefix(events []domain.UpdateEvent, from int) []domain.UpdateEvent {
	if len(events) == 0 {
		return nil
	}
	sorted := make([]domain.UpdateEvent, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Pts < sorted[j].Pts })
	cursor := from
	out := make([]domain.UpdateEvent, 0, len(sorted))
	for _, event := range sorted {
		ptsCount := event.PtsCount
		if ptsCount <= 0 {
			ptsCount = 1
		}
		if event.Pts != cursor+ptsCount {
			break
		}
		out = append(out, event)
		cursor = event.Pts
	}
	return out
}

// ClearAuthKey 清理某 auth_key 的设备状态。
// user_update_events 是账号级 durable log，不能因设备退出登录被删除。
func (s *Service) ClearAuthKey(ctx context.Context, authKeyID [8]byte) error {
	if s.states != nil {
		if err := s.states.DeleteAuthKey(ctx, authKeyID); err != nil {
			return err
		}
	}
	return nil
}

// RecordNewMessage 推进 update 状态并追加一条 new_message 事件。
func (s *Service) RecordNewMessage(ctx context.Context, authKeyID [8]byte, userID int64, msg domain.Message) (domain.UpdateEvent, domain.UpdateState, error) {
	if userID == 0 {
		userID = msg.OwnerUserID
	}
	date := msg.Date
	if date == 0 {
		date = int(time.Now().Unix())
	}
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventNewMessage,
		Date:     date,
		Message:  msg,
		PtsCount: 1,
	}, false, 0)
}

// RecordMessageReactions records a durable marker for message reaction changes.
//
// updateMessageReactions has no pts fields in Layer 225, but TDesktop still
// needs getDifference to advance account pts and carry the latest reaction
// aggregate for offline devices.
func (s *Service) RecordMessageReactions(ctx context.Context, authKeyID [8]byte, userID int64, msg domain.Message) (domain.UpdateEvent, domain.UpdateState, error) {
	if userID == 0 {
		userID = msg.OwnerUserID
	}
	date := msg.Date
	if date == 0 {
		date = int(time.Now().Unix())
	}
	return s.recordEventWithoutState(ctx, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventMessageReactions,
		Date:     date,
		Message:  msg,
		Peer:     msg.Peer,
		PtsCount: 1,
	})
}

// RecordReadHistory 推进 update 状态并追加一条 read_history_inbox 事件。
func (s *Service) RecordReadHistory(ctx context.Context, authKeyID [8]byte, userID int64, read domain.ReadHistoryResult, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	if userID == 0 {
		userID = read.OwnerUserID
	}
	date := int(time.Now().Unix())
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:             domain.UpdateEventReadHistoryInbox,
		Date:             date,
		Peer:             read.Peer,
		MaxID:            read.MaxID,
		StillUnreadCount: read.StillUnreadCount,
		PtsCount:         1,
	}, true, excludeSessionID)
}

// RecordContactsReset 记录通讯录视角变化，供离线设备通过 updates.getDifference 触发重拉。
func (s *Service) RecordContactsReset(ctx context.Context, authKeyID [8]byte, userID int64, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventContactsReset,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordDialogPinned 记录单个会话置顶状态变化。
func (s *Service) RecordDialogPinned(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, pinned bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventDialogPinned,
		Peer:     peer,
		Bool:     pinned,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordPinnedDialogs 记录置顶会话顺序变化，并把新顺序持久化给 getDifference/outbox。
func (s *Service) RecordPinnedDialogs(ctx context.Context, authKeyID [8]byte, userID int64, order []domain.Peer, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventPinnedDialogs,
		Peers:    append([]domain.Peer(nil), order...),
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordDialogUnreadMark 记录手动未读标记变化。
func (s *Service) RecordDialogUnreadMark(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, unread bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventDialogUnreadMark,
		Peer:     peer,
		Bool:     unread,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordChannelViewForumAsMessages records a per-account forum presentation state change.
func (s *Service) RecordChannelViewForumAsMessages(ctx context.Context, authKeyID [8]byte, userID, channelID int64, enabled bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventChannelViewForum,
		Peer:     domain.Peer{Type: domain.PeerTypeChannel, ID: channelID},
		Bool:     enabled,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordPeerSettings 记录 peer settings 变化。
func (s *Service) RecordPeerSettings(ctx context.Context, authKeyID [8]byte, userID int64, peer domain.Peer, settings domain.PeerSettings, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventPeerSettings,
		Peer:     peer,
		Settings: settings,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordDialogFilter 记录单个 filter 的创建、更新或删除；folder 为 nil 表示删除。
func (s *Service) RecordDialogFilter(ctx context.Context, authKeyID [8]byte, userID int64, folderID int, folder *domain.DialogFolder, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	var copyFolder *domain.DialogFolder
	if folder != nil {
		f := *folder
		copyFolder = &f
	}
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:         domain.UpdateEventDialogFilter,
		FilterID:     folderID,
		DialogFilter: copyFolder,
		PtsCount:     1,
	}, true, excludeSessionID)
}

// RecordDialogFilterOrder 记录 filter 顺序变化。
func (s *Service) RecordDialogFilterOrder(ctx context.Context, authKeyID [8]byte, userID int64, order []int, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:        domain.UpdateEventDialogFilterOrder,
		FilterOrder: append([]int(nil), order...),
		PtsCount:    1,
	}, true, excludeSessionID)
}

// RecordDialogFiltersReload 通知其他设备重新拉取 filter 列表。
func (s *Service) RecordDialogFiltersReload(ctx context.Context, authKeyID [8]byte, userID int64, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventDialogFilters,
		PtsCount: 1,
	}, true, excludeSessionID)
}

// RecordFolderPeers 记录归档/还原会话的 folder_id 变化。
func (s *Service) RecordFolderPeers(ctx context.Context, authKeyID [8]byte, userID int64, peers []domain.FolderPeerUpdate, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:        domain.UpdateEventFolderPeers,
		FolderPeers: append([]domain.FolderPeerUpdate(nil), peers...),
		PtsCount:    1,
	}, true, excludeSessionID)
}

// RecordChannelAvailableMessages records a local channel history clear for multi-device sync.
func (s *Service) RecordChannelAvailableMessages(ctx context.Context, authKeyID [8]byte, userID, channelID int64, availableMinID int, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEvent(ctx, authKeyID, userID, domain.UpdateEvent{
		Type:     domain.UpdateEventChannelAvailable,
		Peer:     domain.Peer{Type: domain.PeerTypeChannel, ID: channelID},
		MaxID:    availableMinID,
		PtsCount: 1,
	}, true, excludeSessionID)
}

func (s *Service) recordEvent(ctx context.Context, authKeyID [8]byte, userID int64, event domain.UpdateEvent, dispatch bool, excludeSessionID int64) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEventCore(ctx, authKeyID, userID, event, dispatch, excludeSessionID, true)
}

func (s *Service) recordEventWithoutState(ctx context.Context, userID int64, event domain.UpdateEvent) (domain.UpdateEvent, domain.UpdateState, error) {
	return s.recordEventCore(ctx, [8]byte{}, userID, event, false, 0, false)
}

func (s *Service) recordEventCore(ctx context.Context, authKeyID [8]byte, userID int64, event domain.UpdateEvent, dispatch bool, excludeSessionID int64, saveState bool) (domain.UpdateEvent, domain.UpdateState, error) {
	date := event.Date
	if date == 0 {
		date = int(time.Now().Unix())
	}
	if event.PtsCount == 0 {
		event.PtsCount = 1
	}
	pts, err := s.nextPtsN(ctx, userID, event.PtsCount)
	if err != nil {
		return domain.UpdateEvent{}, domain.UpdateState{}, err
	}
	st := domain.UpdateState{Pts: pts, Date: date, Seq: 0}
	event.UserID = userID
	event.Pts = st.Pts
	event.Date = date
	if s.events != nil {
		var err error
		if dispatch {
			if appender, ok := s.events.(dispatchingEventAppender); ok {
				err = appender.AppendWithDispatch(ctx, userID, event, authKeyID, excludeSessionID)
			} else {
				err = s.events.Append(ctx, userID, event)
			}
		} else {
			err = s.events.Append(ctx, userID, event)
		}
		if err != nil {
			if !dispatch {
				_ = s.events.Append(ctx, userID, domain.UpdateEvent{
					UserID:   userID,
					Type:     domain.UpdateEventNoop,
					Pts:      pts,
					PtsCount: event.PtsCount,
					Date:     date,
				})
			}
			return domain.UpdateEvent{}, domain.UpdateState{}, err
		}
	}
	if saveState && s.states != nil {
		if err := s.states.Save(ctx, authKeyID, userID, st); err != nil {
			return domain.UpdateEvent{}, domain.UpdateState{}, err
		}
	}
	return event, st, nil
}

// currentPts 供 GetState 报告「当前 pts」。对齐 MTProto：报告最大连续已提交 pts，
// 而非 Redis allocator 的最大已分配值——后者在并发发送在途时会超前于已提交事件，
// 会让首次登录基线越过在途空洞而丢消息。allocator 仅在无 events 存储时兜底。
func (s *Service) currentPts(ctx context.Context, userID int64) (int, error) {
	if s.events != nil {
		return s.events.MaxContiguousPts(ctx, userID)
	}
	if s.pts != nil {
		return s.pts.CurrentPts(ctx, userID)
	}
	return 0, nil
}

func (s *Service) nextPts(ctx context.Context, userID int64) (int, error) {
	if s.pts != nil {
		return s.pts.NextPts(ctx, userID)
	}
	current, err := s.currentPts(ctx, userID)
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

func (s *Service) nextPtsN(ctx context.Context, userID int64, count int) (int, error) {
	if count <= 0 {
		count = 1
	}
	if count == 1 {
		return s.nextPts(ctx, userID)
	}
	if s.pts != nil {
		if ranges, ok := s.pts.(store.PtsRangeAllocator); ok {
			return ranges.NextPtsN(ctx, userID, count)
		}
		var pts int
		var err error
		for i := 0; i < count; i++ {
			pts, err = s.pts.NextPts(ctx, userID)
			if err != nil {
				return 0, err
			}
		}
		return pts, nil
	}
	current, err := s.currentPts(ctx, userID)
	if err != nil {
		return 0, err
	}
	return current + count, nil
}
