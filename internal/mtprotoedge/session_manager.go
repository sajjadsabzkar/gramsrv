package mtprotoedge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
)

// ErrSessionNotFound 表示目标 session 当前无活跃连接。
var ErrSessionNotFound = errors.New("session not found")

// ErrSessionAmbiguous 表示仅用 session_id 无法唯一定位连接。
var ErrSessionAmbiguous = errors.New("session id is shared by multiple auth keys")

const (
	maxPendingPushesPerSession = 32
	// pendingPushMaxAge：session 注册后迟迟不调 updates.getState（receivesUpdates 恒 false）时，
	// 其暂存的主动推送最长保留时长。超过即丢整批并不再囤——正常 TDesktop 登录后秒级就会
	// getState 建立同步基线；长期不 ready 多为异常/对抗连接。丢弃不丢消息：getDifference 以
	// user_update_events durable log 兜底补齐。
	pendingPushMaxAge = 60 * time.Second
)

type queuedPush struct {
	t   proto.MessageType
	msg bin.Encoder
	at  time.Time
}

type sessionKey struct {
	authKeyID [8]byte
	sessionID int64
}

// SessionLifecycleObserver receives active connection lifecycle events.
type SessionLifecycleObserver interface {
	SessionOffline(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool)
}

// SessionManager 是活跃连接注册表，支持按 session / auth-key / user 查找并主动 push。
//
// 它管理运行态的在线连接，与持久化的 store.SessionStore 互补：后者记录 session 数据，
// 前者持有可发送的活跃连接。所有方法并发安全。
type SessionManager struct {
	mu                sync.RWMutex
	bySession         map[sessionKey]*Conn
	bySessionID       map[int64]map[[8]byte]*Conn // sessionID → raw authKeyID → Conn，用于兼容旧 API 的唯一性检查
	byAuthKey         map[[8]byte]map[int64]*Conn // raw authKeyID → sessionID → Conn
	byUser            map[int64]map[sessionKey]*Conn
	byChannel         map[int64]map[sessionKey]int64 // channelID → session → userID，用于频道 active-viewer 临时推送
	bySessionChannels map[sessionKey]map[int64]struct{}
	byMemberChannel   map[int64]map[sessionKey]int64 // channelID → session → userID，用于已上线成员持久 update 推送
	bySessionMembers  map[sessionKey]map[int64]struct{}
	pending           map[sessionKey][]queuedPush // updates-ready 前暂存的主动推送

	lifecycle SessionLifecycleObserver
	log       *zap.Logger
}

// NewSessionManager 创建空的连接注册表。
func NewSessionManager(log *zap.Logger) *SessionManager {
	if log == nil {
		log = zap.NewNop()
	}
	return &SessionManager{
		bySession:         make(map[sessionKey]*Conn),
		bySessionID:       make(map[int64]map[[8]byte]*Conn),
		byAuthKey:         make(map[[8]byte]map[int64]*Conn),
		byUser:            make(map[int64]map[sessionKey]*Conn),
		byChannel:         make(map[int64]map[sessionKey]int64),
		bySessionChannels: make(map[sessionKey]map[int64]struct{}),
		byMemberChannel:   make(map[int64]map[sessionKey]int64),
		bySessionMembers:  make(map[sessionKey]map[int64]struct{}),
		pending:           make(map[sessionKey][]queuedPush),
		log:               log,
	}
}

// SetLifecycleObserver installs a best-effort active session lifecycle observer.
func (m *SessionManager) SetLifecycleObserver(observer SessionLifecycleObserver) {
	m.mu.Lock()
	m.lifecycle = observer
	m.mu.Unlock()
}

// Register 注册一个活跃连接。若同 raw auth_key_id + session_id 已存在（重连），旧连接被替换并移除索引。
func (m *SessionManager) Register(c *Conn) {
	m.mu.Lock()

	key := connSessionKey(c)
	var replaced *Conn
	if old, ok := m.bySession[key]; ok && old != c {
		replaced = old
		m.removeLocked(old, false)
	}
	m.bySession[key] = c
	addSessionIDIndex(m.bySessionID, c.sessionID, c.authKeyID, c)
	addConnIndex(m.byAuthKey, c.authKeyID, c.sessionID, c)
	if uid := c.userID.Load(); uid != 0 {
		c.userIDResolved.Store(true)
		addUserIndex(m.byUser, uid, key, c)
	}
	m.log.Debug("Session registered",
		zap.String("auth_key_id", sessionKeyLog(key.authKeyID)),
		zap.Int64("session_id", c.sessionID),
		zap.Int("online", len(m.bySession)),
	)
	m.mu.Unlock()

	if replaced != nil {
		replaced.Close()
	}
}

// Unregister 注销一个连接（仅当它仍是当前注册的同一对象，避免误删重连后的新连接）。
func (m *SessionManager) Unregister(c *Conn) {
	m.mu.Lock()
	var (
		observer    SessionLifecycleObserver
		offlineUser int64
		lastForUser bool
	)
	if cur, ok := m.bySession[connSessionKey(c)]; ok && cur == c {
		offlineUser = m.removeLocked(c, true)
		if offlineUser != 0 {
			lastForUser = len(m.byUser[offlineUser]) == 0
			observer = m.lifecycle
		}
		m.log.Debug("Session unregistered",
			zap.String("auth_key_id", sessionKeyLog(c.authKeyID)),
			zap.Int64("session_id", c.sessionID),
			zap.Int("online", len(m.bySession)),
		)
	}
	m.mu.Unlock()
	if observer != nil && offlineUser != 0 {
		observer.SessionOffline(c.authKeyID, c.sessionID, offlineUser, lastForUser)
	}
}

// DestroySession 移除指定 session 的运行态索引，供 MTProto destroy_session 使用。
func (m *SessionManager) DestroySession(sessionID int64) bool {
	m.mu.Lock()
	c, key, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	if ambiguous || !ok {
		if !ambiguous {
			m.dropPendingBySessionLocked(sessionID)
		}
		m.mu.Unlock()
		return false
	}
	offlineUser := m.removeLocked(c, true)
	lastForUser := offlineUser != 0 && len(m.byUser[offlineUser]) == 0
	observer := m.lifecycle
	m.log.Debug("Session destroyed",
		zap.String("auth_key_id", sessionKeyLog(key.authKeyID)),
		zap.Int64("session_id", sessionID),
		zap.Int("online", len(m.bySession)),
	)
	m.mu.Unlock()
	c.Close()
	if observer != nil && offlineUser != 0 {
		observer.SessionOffline(key.authKeyID, sessionID, offlineUser, lastForUser)
	}
	return true
}

// DestroySessionForAuthKey 精确移除某个 raw auth_key_id 下的 session。
func (m *SessionManager) DestroySessionForAuthKey(authKeyID [8]byte, sessionID int64) bool {
	m.mu.Lock()
	key := sessionKey{authKeyID: authKeyID, sessionID: sessionID}
	c, ok := m.bySession[key]
	if !ok {
		delete(m.pending, key)
		m.mu.Unlock()
		return false
	}
	offlineUser := m.removeLocked(c, true)
	lastForUser := offlineUser != 0 && len(m.byUser[offlineUser]) == 0
	observer := m.lifecycle
	m.log.Debug("Session destroyed",
		zap.String("auth_key_id", sessionKeyLog(authKeyID)),
		zap.Int64("session_id", sessionID),
		zap.Int("online", len(m.bySession)),
	)
	m.mu.Unlock()
	c.Close()
	if observer != nil && offlineUser != 0 {
		observer.SessionOffline(authKeyID, sessionID, offlineUser, lastForUser)
	}
	return true
}

// BindUser 缓存 session 的授权用户。userID=0 表示当前 auth_key 已确认未登录。
// 登录后绑定非 0 userID，使其可经 PushToUser 收到推送。
func (m *SessionManager) BindUser(sessionID, userID int64) {
	m.mu.Lock()
	c, key, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	if ambiguous || !ok {
		if ambiguous {
			m.log.Warn("Skip BindUser for ambiguous session_id", zap.Int64("session_id", sessionID))
		}
		m.mu.Unlock()
		return
	}
	m.bindUserLocked(c, key, userID)
	m.mu.Unlock()
}

// BindUserForAuthKey 缓存指定 raw auth_key_id + session_id 的授权用户。
func (m *SessionManager) BindUserForAuthKey(authKeyID [8]byte, sessionID, userID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := sessionKey{authKeyID: authKeyID, sessionID: sessionID}
	c, ok := m.bySession[key]
	if !ok {
		return
	}
	m.bindUserLocked(c, key, userID)
}

func (m *SessionManager) bindUserLocked(c *Conn, key sessionKey, userID int64) {
	if old := c.userID.Swap(userID); old != 0 {
		removeUserIndex(m.byUser, old, key)
		if old != userID {
			m.clearChannelInterestsLocked(key)
			m.clearChannelMembershipsLocked(key)
		}
	}
	c.userIDResolved.Store(true)
	if userID != 0 {
		addUserIndex(m.byUser, userID, key, c)
	} else {
		m.clearChannelInterestsLocked(key)
		m.clearChannelMembershipsLocked(key)
	}
}

// UserID 返回 session 当前缓存的登录用户 id。未绑定或离线时 ok=false。
func (m *SessionManager) UserID(sessionID int64) (int64, bool) {
	m.mu.RLock()
	c, _, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	m.mu.RUnlock()
	if ambiguous || !ok {
		return 0, false
	}
	userID := c.userID.Load()
	if userID == 0 {
		return 0, false
	}
	return userID, true
}

// UserIDForAuthKey 返回指定 raw auth_key_id + session_id 当前缓存的登录用户 id。
func (m *SessionManager) UserIDForAuthKey(authKeyID [8]byte, sessionID int64) (int64, bool) {
	m.mu.RLock()
	c, ok := m.bySession[sessionKey{authKeyID: authKeyID, sessionID: sessionID}]
	m.mu.RUnlock()
	if !ok {
		return 0, false
	}
	userID := c.userID.Load()
	if userID == 0 {
		return 0, false
	}
	return userID, true
}

// UserIDResolved 返回 session 的 user_id 授权状态是否已经查过。
// resolved=true 且 userID=0 表示该 session 当前未登录。
func (m *SessionManager) UserIDResolved(sessionID int64) (int64, bool) {
	m.mu.RLock()
	c, _, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	m.mu.RUnlock()
	if ambiguous || !ok {
		return 0, false
	}
	return c.UserIDResolved()
}

// UserIDResolvedForAuthKey 返回指定 raw auth_key_id + session_id 的 user_id 缓存状态。
func (m *SessionManager) UserIDResolvedForAuthKey(authKeyID [8]byte, sessionID int64) (int64, bool) {
	m.mu.RLock()
	c, ok := m.bySession[sessionKey{authKeyID: authKeyID, sessionID: sessionID}]
	m.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return c.UserIDResolved()
}

// BindAuthKey 缓存业务视角 auth_key_id（temp auth_key 解析后的 perm auth_key）。
func (m *SessionManager) BindAuthKey(sessionID int64, authKeyID [8]byte) {
	m.mu.Lock()
	c, key, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	if ambiguous || !ok {
		if ambiguous {
			m.log.Warn("Skip BindAuthKey for ambiguous session_id", zap.Int64("session_id", sessionID))
		}
		m.mu.Unlock()
		return
	}
	m.bindAuthKeyLocked(c, key, authKeyID)
	m.mu.Unlock()
}

// BindAuthKeyForSession 缓存指定 raw auth_key_id + session_id 的业务 auth_key_id。
func (m *SessionManager) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := sessionKey{authKeyID: rawAuthKeyID, sessionID: sessionID}
	c, ok := m.bySession[key]
	if !ok {
		return
	}
	m.bindAuthKeyLocked(c, key, authKeyID)
}

func (m *SessionManager) bindAuthKeyLocked(c *Conn, key sessionKey, authKeyID [8]byte) {
	oldAuthKeyID, resolved := c.BusinessAuthKeyID()
	changed := !resolved || oldAuthKeyID != authKeyID
	oldUserID := c.userID.Load()
	c.SetBusinessAuthKeyID(authKeyID)
	if changed {
		if oldUserID != 0 {
			removeUserIndex(m.byUser, oldUserID, key)
		}
		m.clearChannelInterestsLocked(key)
		m.clearChannelMembershipsLocked(key)
		c.userID.Store(0)
		c.userIDResolved.Store(false)
	}
}

// AuthKeyID 返回 session 缓存的业务视角 auth_key_id。
// ok=false 表示该连接尚未完成 temp→perm 解析。
func (m *SessionManager) AuthKeyID(sessionID int64) ([8]byte, bool) {
	m.mu.RLock()
	c, _, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	m.mu.RUnlock()
	if ambiguous || !ok {
		return [8]byte{}, false
	}
	return c.BusinessAuthKeyID()
}

// AuthKeyIDForSession 返回指定 raw auth_key_id + session_id 缓存的业务 auth_key_id。
func (m *SessionManager) AuthKeyIDForSession(rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool) {
	m.mu.RLock()
	c, ok := m.bySession[sessionKey{authKeyID: rawAuthKeyID, sessionID: sessionID}]
	m.mu.RUnlock()
	if !ok {
		return [8]byte{}, false
	}
	return c.BusinessAuthKeyID()
}

// UnbindAuthKey 清理某业务 auth_key 下所有活跃连接的登录用户缓存。
func (m *SessionManager) UnbindAuthKey(authKeyID [8]byte) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for key, c := range m.bySession {
		if !connUsesBusinessAuthKey(c, authKeyID) {
			continue
		}
		if old := c.userID.Swap(0); old != 0 {
			removeUserIndex(m.byUser, old, key)
		}
		m.clearChannelInterestsLocked(key)
		m.clearChannelMembershipsLocked(key)
		c.userIDResolved.Store(true)
		count++
	}
	return count
}

// SetReceivesUpdates 标记 session 是否已完成 updates 同步入口。
//
// TDesktop 登录后会先调用 updates.getState/getDifference 建立本地同步基线。
// 在此之前收到的主动 updates 先暂存，待 session 可接收后再异步下发。
func (m *SessionManager) SetReceivesUpdates(sessionID int64, receives bool) {
	m.mu.Lock()
	c, key, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	if ambiguous || !ok {
		if ambiguous {
			m.log.Warn("Skip SetReceivesUpdates for ambiguous session_id", zap.Int64("session_id", sessionID))
		}
		m.mu.Unlock()
		return
	}
	c.receivesUpdates.Store(receives)
	if !receives {
		m.clearChannelInterestsLocked(key)
		m.clearChannelMembershipsLocked(key)
	}
	pending := m.takePendingLocked(key, receives)
	m.mu.Unlock()

	if len(pending) > 0 {
		go m.flushPending(key, pending)
	}
}

// SetReceivesUpdatesForAuthKey 标记指定 raw auth_key_id + session_id 是否接收主动 updates。
func (m *SessionManager) SetReceivesUpdatesForAuthKey(authKeyID [8]byte, sessionID int64, receives bool) {
	m.mu.Lock()
	key := sessionKey{authKeyID: authKeyID, sessionID: sessionID}
	c, ok := m.bySession[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	c.receivesUpdates.Store(receives)
	if !receives {
		m.clearChannelInterestsLocked(key)
		m.clearChannelMembershipsLocked(key)
	}
	pending := m.takePendingLocked(key, receives)
	m.mu.Unlock()

	if len(pending) > 0 {
		go m.flushPending(key, pending)
	}
}

// PushToSession 向指定 session 推送一条消息。
func (m *SessionManager) PushToSession(ctx context.Context, sessionID int64, t proto.MessageType, msg bin.Encoder) error {
	m.mu.Lock()
	c, key, ok, ambiguous := m.uniqueSessionLocked(sessionID)
	if ambiguous {
		m.mu.Unlock()
		return ErrSessionAmbiguous
	}
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	if !c.receivesUpdates.Load() {
		m.queueLocked(key, t, msg)
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	return c.Send(ctx, t, msg)
}

// PushToSessionForAuthKey 向指定 raw auth_key_id + session_id 推送一条消息。
func (m *SessionManager) PushToSessionForAuthKey(ctx context.Context, authKeyID [8]byte, sessionID int64, t proto.MessageType, msg bin.Encoder) error {
	m.mu.Lock()
	key := sessionKey{authKeyID: authKeyID, sessionID: sessionID}
	c, ok := m.bySession[key]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	if !c.receivesUpdates.Load() {
		m.queueLocked(key, t, msg)
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	return c.Send(ctx, t, msg)
}

// PushToUser 向某 user 所有活跃连接推送，返回已发送或已暂存的连接数。
// 发送在释放锁后进行，避免持锁阻塞于网络 IO。
func (m *SessionManager) PushToUser(ctx context.Context, userID int64, t proto.MessageType, msg bin.Encoder) (int, error) {
	return m.PushToUserExceptAuthKeySession(ctx, userID, [8]byte{}, 0, t, msg)
}

// PushToUserExceptSession 向某 user 所有活跃连接推送，但跳过指定 session。
// 未完成 updates 同步入口的 session 会先暂存，等 SetReceivesUpdates(true) 后再发。
func (m *SessionManager) PushToUserExceptSession(ctx context.Context, userID, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error) {
	return m.pushToUser(ctx, userID, nil, excludeSessionID, t, msg)
}

// PushToUserExceptAuthKeySession 向某 user 所有活跃连接推送，跳过指定业务 auth_key + session。
func (m *SessionManager) PushToUserExceptAuthKeySession(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error) {
	return m.pushToUser(ctx, userID, &excludeAuthKeyID, excludeSessionID, t, msg)
}

func (m *SessionManager) pushToUser(ctx context.Context, userID int64, excludeAuthKeyID *[8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder) (int, error) {
	getEncoded := onceEncodedOutbound(msg)
	return m.pushToUserWithSender(ctx, userID, excludeAuthKeyID, excludeSessionID, t, msg, func(c *Conn) error {
		if c.outbound == nil || c.outboundControl == nil {
			return ErrConnClosed
		}
		encoded, err := getEncoded()
		if err != nil {
			return err
		}
		return c.SendEncoded(ctx, t, encoded)
	})
}

func (m *SessionManager) PushToUserExceptSessionBestEffort(ctx context.Context, userID, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error) {
	return m.pushToUserBestEffort(ctx, userID, nil, excludeSessionID, t, msg, timeout)
}

func (m *SessionManager) PushToUserExceptAuthKeySessionBestEffort(ctx context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error) {
	return m.pushToUserBestEffort(ctx, userID, &excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (m *SessionManager) pushToUserBestEffort(ctx context.Context, userID int64, excludeAuthKeyID *[8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, timeout time.Duration) (int, error) {
	getEncoded := onceEncodedOutbound(msg)
	return m.pushToUserWithSender(ctx, userID, excludeAuthKeyID, excludeSessionID, t, msg, func(c *Conn) error {
		if c.outbound == nil || c.outboundControl == nil {
			return ErrConnClosed
		}
		encoded, err := getEncoded()
		if err != nil {
			return err
		}
		return c.SendBestEffortEncoded(ctx, t, encoded, timeout)
	})
}

func onceEncodedOutbound(msg bin.Encoder) func() (*encodedOutboundMessage, error) {
	var (
		encoded *encodedOutboundMessage
		err     error
	)
	return func() (*encodedOutboundMessage, error) {
		if encoded == nil && err == nil {
			encoded, err = encodeOutboundMessage(msg)
		}
		return encoded, err
	}
}

func (m *SessionManager) pushToUserWithSender(ctx context.Context, userID int64, excludeAuthKeyID *[8]byte, excludeSessionID int64, t proto.MessageType, msg bin.Encoder, send func(*Conn) error) (int, error) {
	m.mu.Lock()
	conns := make([]*Conn, 0, len(m.byUser[userID]))
	queued := 0
	for key, c := range m.byUser[userID] {
		if shouldExcludeSession(c, excludeAuthKeyID, excludeSessionID) {
			continue
		}
		if !c.receivesUpdates.Load() {
			m.queueLocked(key, t, msg)
			queued++
			continue
		}
		conns = append(conns, c)
	}
	m.mu.Unlock()

	var firstErr error
	sent := 0
	for _, c := range conns {
		if err := send(c); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		sent++
	}
	return sent + queued, firstErr
}

// Online 返回当前活跃连接数。
func (m *SessionManager) Online() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bySession)
}

// OnlineUserIDs returns a bounded snapshot of users that currently have active
// sessions. Callers still need to verify business visibility before pushing.
func (m *SessionManager) OnlineUserIDs(limit int) []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.byUser) == 0 {
		return nil
	}
	capHint := len(m.byUser)
	if limit > 0 && capHint > limit {
		capHint = limit
	}
	ids := make([]int64, 0, capHint)
	for userID, conns := range m.byUser {
		if userID == 0 || len(conns) == 0 {
			continue
		}
		ids = append(ids, userID)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}
	return ids
}

// IsUserOnline returns whether userID has at least one active connection.
func (m *SessionManager) IsUserOnline(userID int64) bool {
	if userID == 0 {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUser[userID]) > 0
}

// OnlineUserIDsForCandidates filters an explicit candidate set against the
// active user index. It avoids exporting or sorting the whole online map.
func (m *SessionManager) OnlineUserIDsForCandidates(candidateUserIDs []int64, limit int) []int64 {
	if len(candidateUserIDs) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]int64, 0, minInt(len(candidateUserIDs), positiveLimitOrLen(limit, len(candidateUserIDs))))
	seen := make(map[int64]struct{}, len(candidateUserIDs))
	for _, userID := range candidateUserIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if len(m.byUser[userID]) == 0 {
			continue
		}
		out = append(out, userID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// TrackChannelInterest replaces the channel viewer set for one live session.
// Realtime transient fan-out uses this as the current active-viewer candidate
// set; durable channel updates use the broader membership index instead.
func (m *SessionManager) TrackChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64) {
	if userID == 0 {
		return
	}
	key := sessionKey{authKeyID: rawAuthKeyID, sessionID: sessionID}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.bySession[key]
	if !ok || c.userID.Load() != userID {
		return
	}
	m.clearChannelInterestsLocked(key)
	if len(channelIDs) == 0 {
		return
	}
	m.trackChannelIndexLocked(m.byChannel, m.bySessionChannels, key, userID, channelIDs)
}

// ClearChannelInterest removes the active-viewer channel set for one live
// session while leaving its joined-channel membership index intact.
func (m *SessionManager) ClearChannelInterest(rawAuthKeyID [8]byte, sessionID, userID int64) {
	if userID == 0 {
		return
	}
	key := sessionKey{authKeyID: rawAuthKeyID, sessionID: sessionID}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.bySession[key]
	if !ok || c.userID.Load() != userID {
		return
	}
	m.clearChannelInterestsLocked(key)
}

// OnlineChannelUserIDs returns users with active sessions that have recently
// proven current interest in channelID. The result is intentionally unsorted and bounded.
func (m *SessionManager) OnlineChannelUserIDs(channelID int64, limit int) []int64 {
	return m.onlineChannelUsers(m.byChannel, channelID, limit)
}

// SetSessionChannelMemberships replaces the joined-channel index for one
// updates-ready session. This index is broader than TrackChannelInterest and is
// used for durable channel updates such as new/edit/delete message.
func (m *SessionManager) SetSessionChannelMemberships(rawAuthKeyID [8]byte, sessionID, userID int64, channelIDs []int64) {
	key := sessionKey{authKeyID: rawAuthKeyID, sessionID: sessionID}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.bySession[key]
	if !ok {
		return
	}
	m.clearChannelMembershipsLocked(key)
	if userID == 0 || c.userID.Load() != userID {
		return
	}
	m.trackChannelIndexLocked(m.byMemberChannel, m.bySessionMembers, key, userID, channelIDs)
}

// AddUserChannelMembership adds channelID to every live session for userID.
// It is called after successful join/invite approval paths.
func (m *SessionManager) AddUserChannelMembership(userID, channelID int64) {
	if userID == 0 || channelID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, c := range m.byUser[userID] {
		if c == nil || c.userID.Load() != userID {
			continue
		}
		m.trackChannelIndexLocked(m.byMemberChannel, m.bySessionMembers, key, userID, []int64{channelID})
	}
}

// RemoveUserChannelMembership removes channelID from every live session for userID.
// It is called after leave/kick/ban/delete paths.
func (m *SessionManager) RemoveUserChannelMembership(userID, channelID int64) {
	if userID == 0 || channelID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.byUser[userID] {
		m.removeChannelIndexLocked(m.byMemberChannel, m.bySessionMembers, key, channelID)
	}
}

// OnlineChannelMemberUserIDs returns users with active sessions that are indexed
// as joined members of channelID. The result is intentionally unsorted; callers
// still verify business membership before pushing.
func (m *SessionManager) OnlineChannelMemberUserIDs(channelID int64, limit int) []int64 {
	return m.onlineChannelUsers(m.byMemberChannel, channelID, limit)
}

func (m *SessionManager) onlineChannelUsers(index map[int64]map[sessionKey]int64, channelID int64, limit int) []int64 {
	if channelID == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessions := index[channelID]
	if len(sessions) == 0 {
		return nil
	}
	out := make([]int64, 0, positiveLimitOrLen(limit, len(sessions)))
	seen := make(map[int64]struct{}, len(sessions))
	for key, userID := range sessions {
		if userID == 0 {
			continue
		}
		if _, ok := m.bySession[key]; !ok {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		out = append(out, userID)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func (m *SessionManager) removeLocked(c *Conn, dropPending bool) int64 {
	key := connSessionKey(c)
	delete(m.bySession, key)
	removeSessionIDIndex(m.bySessionID, c.sessionID, c.authKeyID)
	removeConnIndex(m.byAuthKey, c.authKeyID, c.sessionID)
	uid := c.userID.Load()
	if uid != 0 {
		removeUserIndex(m.byUser, uid, key)
	}
	m.clearChannelInterestsLocked(key)
	m.clearChannelMembershipsLocked(key)
	if dropPending {
		delete(m.pending, key)
	}
	return uid
}

func (m *SessionManager) clearChannelInterestsLocked(key sessionKey) {
	m.clearChannelIndexLocked(m.byChannel, m.bySessionChannels, key)
}

func (m *SessionManager) clearChannelMembershipsLocked(key sessionKey) {
	m.clearChannelIndexLocked(m.byMemberChannel, m.bySessionMembers, key)
}

func (m *SessionManager) trackChannelIndexLocked(index map[int64]map[sessionKey]int64, reverse map[sessionKey]map[int64]struct{}, key sessionKey, userID int64, channelIDs []int64) {
	channels := reverse[key]
	if channels == nil {
		channels = make(map[int64]struct{}, len(channelIDs))
		reverse[key] = channels
	}
	for _, channelID := range channelIDs {
		if channelID == 0 {
			continue
		}
		channels[channelID] = struct{}{}
		sessions := index[channelID]
		if sessions == nil {
			sessions = make(map[sessionKey]int64)
			index[channelID] = sessions
		}
		sessions[key] = userID
	}
}

func (m *SessionManager) clearChannelIndexLocked(index map[int64]map[sessionKey]int64, reverse map[sessionKey]map[int64]struct{}, key sessionKey) {
	channels := reverse[key]
	if len(channels) == 0 {
		delete(reverse, key)
		return
	}
	for channelID := range channels {
		sessions := index[channelID]
		delete(sessions, key)
		if len(sessions) == 0 {
			delete(index, channelID)
		}
	}
	delete(reverse, key)
}

func (m *SessionManager) removeChannelIndexLocked(index map[int64]map[sessionKey]int64, reverse map[sessionKey]map[int64]struct{}, key sessionKey, channelID int64) {
	channels := reverse[key]
	delete(channels, channelID)
	if len(channels) == 0 {
		delete(reverse, key)
	}
	sessions := index[channelID]
	delete(sessions, key)
	if len(sessions) == 0 {
		delete(index, channelID)
	}
}

func positiveLimitOrLen(limit, length int) int {
	if limit > 0 && limit < length {
		return limit
	}
	return length
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (m *SessionManager) takePendingLocked(key sessionKey, ready bool) []queuedPush {
	if !ready || len(m.pending[key]) == 0 {
		return nil
	}
	pending := append([]queuedPush(nil), m.pending[key]...)
	delete(m.pending, key)
	return pending
}

func (m *SessionManager) queueLocked(key sessionKey, t proto.MessageType, msg bin.Encoder) {
	q := m.pending[key]
	// 过期保护：最早一条暂存已超过 pendingPushMaxAge（session 迟迟未 ready）时，丢整批并
	// 不再囤这条，记 trace。避免「登录后从不 getState」的连接长期占用 pending 内存。
	if len(q) > 0 && time.Since(q[0].at) > pendingPushMaxAge {
		m.log.Debug("Drop stale pending pushes (session not ready in time)",
			zap.String("auth_key_id", sessionKeyLog(key.authKeyID)),
			zap.Int64("session_id", key.sessionID),
			zap.Int("dropped", len(q)),
		)
		delete(m.pending, key)
		return
	}
	push := queuedPush{t: t, msg: msg, at: time.Now()}
	if len(q) >= maxPendingPushesPerSession {
		copy(q, q[1:])
		q[len(q)-1] = push
		m.pending[key] = q
		return
	}
	m.pending[key] = append(q, push)
}

func (m *SessionManager) flushPending(key sessionKey, pending []queuedPush) {
	for _, item := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := m.PushToSessionForAuthKey(ctx, key.authKeyID, key.sessionID, item.t, item.msg)
		cancel()
		if err != nil {
			m.log.Debug("Flush pending push failed",
				zap.String("auth_key_id", sessionKeyLog(key.authKeyID)),
				zap.Int64("session_id", key.sessionID),
				zap.Error(err),
			)
		}
	}
}

func (m *SessionManager) uniqueSessionLocked(sessionID int64) (*Conn, sessionKey, bool, bool) {
	set := m.bySessionID[sessionID]
	if len(set) == 0 {
		return nil, sessionKey{}, false, false
	}
	if len(set) > 1 {
		return nil, sessionKey{}, false, true
	}
	for authKeyID, c := range set {
		return c, sessionKey{authKeyID: authKeyID, sessionID: sessionID}, true, false
	}
	return nil, sessionKey{}, false, false
}

func (m *SessionManager) dropPendingBySessionLocked(sessionID int64) {
	for key := range m.pending {
		if key.sessionID == sessionID {
			delete(m.pending, key)
		}
	}
}

func addConnIndex[K comparable](idx map[K]map[int64]*Conn, key K, sessionID int64, c *Conn) {
	set := idx[key]
	if set == nil {
		set = make(map[int64]*Conn)
		idx[key] = set
	}
	set[sessionID] = c
}

func removeConnIndex[K comparable](idx map[K]map[int64]*Conn, key K, sessionID int64) {
	if set := idx[key]; set != nil {
		delete(set, sessionID)
		if len(set) == 0 {
			delete(idx, key)
		}
	}
}

func addSessionIDIndex(idx map[int64]map[[8]byte]*Conn, sessionID int64, authKeyID [8]byte, c *Conn) {
	set := idx[sessionID]
	if set == nil {
		set = make(map[[8]byte]*Conn)
		idx[sessionID] = set
	}
	set[authKeyID] = c
}

func removeSessionIDIndex(idx map[int64]map[[8]byte]*Conn, sessionID int64, authKeyID [8]byte) {
	if set := idx[sessionID]; set != nil {
		delete(set, authKeyID)
		if len(set) == 0 {
			delete(idx, sessionID)
		}
	}
}

func addUserIndex(idx map[int64]map[sessionKey]*Conn, userID int64, key sessionKey, c *Conn) {
	set := idx[userID]
	if set == nil {
		set = make(map[sessionKey]*Conn)
		idx[userID] = set
	}
	set[key] = c
}

func removeUserIndex(idx map[int64]map[sessionKey]*Conn, userID int64, key sessionKey) {
	if set := idx[userID]; set != nil {
		delete(set, key)
		if len(set) == 0 {
			delete(idx, userID)
		}
	}
}

func connSessionKey(c *Conn) sessionKey {
	return sessionKey{authKeyID: c.authKeyID, sessionID: c.sessionID}
}

func connUsesBusinessAuthKey(c *Conn, authKeyID [8]byte) bool {
	id, resolved := c.BusinessAuthKeyID()
	if resolved {
		return id == authKeyID
	}
	return c.authKeyID == authKeyID
}

func shouldExcludeSession(c *Conn, excludeAuthKeyID *[8]byte, excludeSessionID int64) bool {
	if excludeSessionID == 0 {
		return false
	}
	if c.sessionID != excludeSessionID {
		return false
	}
	if excludeAuthKeyID == nil || *excludeAuthKeyID == ([8]byte{}) {
		return true
	}
	return connUsesBusinessAuthKey(c, *excludeAuthKeyID)
}

func sessionKeyLog(id [8]byte) string {
	return fmt.Sprintf("%x", id)
}
