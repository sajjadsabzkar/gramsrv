package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
)

const userOnlineTTL = 5 * time.Minute
const presenceDialogFanoutCandidateLimit = 512

type presenceSessionKey struct {
	rawAuthKeyID [8]byte
	sessionID    int64
}

type presenceSessionState struct {
	userID int64
	status domain.UserStatus
}

type presenceTracker struct {
	mu        sync.RWMutex
	bySession map[presenceSessionKey]presenceSessionState
	byUser    map[int64]map[presenceSessionKey]domain.UserStatus
}

func newPresenceTracker() *presenceTracker {
	return &presenceTracker{
		bySession: make(map[presenceSessionKey]presenceSessionState),
		byUser:    make(map[int64]map[presenceSessionKey]domain.UserStatus),
	}
}

func (p *presenceTracker) setSessionStatus(key presenceSessionKey, userID int64, status domain.UserStatus) {
	if p == nil || userID == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.bySession[key]; ok {
		p.removeSessionLocked(key, old.userID)
	}
	p.bySession[key] = presenceSessionState{userID: userID, status: status}
	sessions := p.byUser[userID]
	if sessions == nil {
		sessions = make(map[presenceSessionKey]domain.UserStatus)
		p.byUser[userID] = sessions
	}
	sessions[key] = status
}

func (p *presenceTracker) clearSession(key presenceSessionKey) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.bySession[key]; ok {
		p.removeSessionLocked(key, old.userID)
	}
}

func (p *presenceTracker) removeSessionLocked(key presenceSessionKey, userID int64) {
	delete(p.bySession, key)
	sessions := p.byUser[userID]
	delete(sessions, key)
	if len(sessions) == 0 {
		delete(p.byUser, userID)
	}
}

func (p *presenceTracker) statusFor(userID int64, now int) (domain.UserStatus, bool) {
	if p == nil || userID == 0 {
		return domain.UserStatus{}, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	sessions := p.byUser[userID]
	if len(sessions) == 0 {
		return domain.UserStatus{}, false
	}
	known := false
	var bestOnline domain.UserStatus
	var bestOffline domain.UserStatus
	for _, status := range sessions {
		status = normalizePresenceStatus(status, now)
		switch status.Kind {
		case domain.UserStatusOnline:
			if bestOnline.Expires == 0 || status.Expires > bestOnline.Expires {
				bestOnline = status
			}
			known = true
		case domain.UserStatusOffline:
			if bestOffline.WasOnline == 0 || status.WasOnline > bestOffline.WasOnline {
				bestOffline = status
			}
			known = true
		case domain.UserStatusRecently, domain.UserStatusLastWeek, domain.UserStatusLastMonth, domain.UserStatusEmpty:
			if !known {
				bestOffline = status
				known = true
			}
		}
	}
	if bestOnline.Kind == domain.UserStatusOnline {
		return bestOnline, true
	}
	if known {
		return bestOffline, true
	}
	return domain.UserStatus{}, false
}

func normalizePresenceStatus(status domain.UserStatus, now int) domain.UserStatus {
	if status.Kind == domain.UserStatusOnline && status.Expires <= now {
		wasOnline := status.WasOnline
		if wasOnline == 0 || wasOnline > status.Expires {
			wasOnline = status.Expires
		}
		return domain.UserStatus{Kind: domain.UserStatusOffline, WasOnline: wasOnline}
	}
	return status
}

func (r *Router) setPresenceFromContext(ctx context.Context, userID int64, offline bool) domain.UserStatus {
	now := int(r.clock.Now().Unix())
	status := domain.UserStatus{Kind: domain.UserStatusOffline, WasOnline: now}
	if !offline {
		status = domain.UserStatus{
			Kind:      domain.UserStatusOnline,
			Expires:   now + int(userOnlineTTL/time.Second),
			WasOnline: now,
		}
	}
	if key, ok := presenceSessionKeyFromContext(ctx); ok {
		r.presence.setSessionStatus(key, userID, status)
	}
	r.persistLastSeen(ctx, userID, now)
	return r.userPresenceStatusForUser(domain.User{ID: userID, LastSeenAt: now})
}

func (r *Router) announceSessionOnline(ctx context.Context, userID int64) {
	if userID == 0 {
		return
	}
	status := r.setPresenceFromContext(ctx, userID, false)
	r.pushUserStatus(ctx, userID, status)
	r.pushOnlinePeerStatusesToCurrentSession(ctx, userID)
}

func (r *Router) userPresenceStatus(userID int64) domain.UserStatus {
	return r.userPresenceStatusForUser(domain.User{ID: userID})
}

func (r *Router) userPresenceStatusForUser(u domain.User) domain.UserStatus {
	userID := u.ID
	if userID == 0 {
		return domain.UserStatus{Kind: domain.UserStatusRecently}
	}
	now := int(r.clock.Now().Unix())
	if status, ok := r.presence.statusFor(userID, now); ok {
		return status
	}
	if provider, ok := r.deps.Sessions.(OnlineUserProvider); ok && provider.IsUserOnline(userID) {
		return domain.UserStatus{
			Kind:      domain.UserStatusOnline,
			Expires:   now + int(userOnlineTTL/time.Second),
			WasOnline: now,
		}
	}
	if u.LastSeenAt > 0 {
		return domain.UserStatus{Kind: domain.UserStatusOffline, WasOnline: u.LastSeenAt}
	}
	return domain.UserStatus{Kind: domain.UserStatusRecently}
}

type userLastSeenUpdater interface {
	UpdateLastSeen(ctx context.Context, userID int64, lastSeenAt int) error
}

func (r *Router) persistLastSeen(ctx context.Context, userID int64, lastSeenAt int) {
	if userID == 0 || lastSeenAt <= 0 || r.deps.Users == nil {
		return
	}
	updater, ok := r.deps.Users.(userLastSeenUpdater)
	if !ok {
		return
	}
	if err := updater.UpdateLastSeen(ctx, userID, lastSeenAt); err != nil {
		r.log.Warn("Update user last seen failed", zap.Int64("user_id", userID), zap.Int("last_seen_at", lastSeenAt), zap.Error(err))
	}
}

// SessionOffline is called by mtprotoedge when an active connection disappears.
// Business-side effects stay here: mtprotoedge only reports lifecycle facts.
func (r *Router) SessionOffline(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool) {
	if userID == 0 {
		return
	}
	ctx := WithSessionID(WithRawAuthKeyID(context.Background(), rawAuthKeyID), sessionID)
	key := presenceSessionKey{rawAuthKeyID: rawAuthKeyID, sessionID: sessionID}
	if !lastForUser {
		r.presence.clearSession(key)
		return
	}
	now := int(r.clock.Now().Unix())
	status := domain.UserStatus{Kind: domain.UserStatusOffline, WasOnline: now}
	r.presence.setSessionStatus(key, userID, status)
	r.persistLastSeen(ctx, userID, now)
	r.pushUserStatus(ctx, userID, status)
}

func presenceSessionKeyFromContext(ctx context.Context) (presenceSessionKey, bool) {
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return presenceSessionKey{}, false
	}
	if rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx); ok {
		return presenceSessionKey{rawAuthKeyID: rawAuthKeyID, sessionID: sessionID}, true
	}
	if authKeyID, ok := AuthKeyIDFrom(ctx); ok {
		return presenceSessionKey{rawAuthKeyID: authKeyID, sessionID: sessionID}, true
	}
	return presenceSessionKey{sessionID: sessionID}, true
}

func (r *Router) withUserPresence(u domain.User) domain.User {
	if u.ID == 0 {
		return u
	}
	u.Status = r.userPresenceStatusForUser(u)
	return u
}

func (r *Router) withUsersPresence(users []domain.User) []domain.User {
	if len(users) == 0 {
		return users
	}
	out := append([]domain.User(nil), users...)
	for i := range out {
		out[i] = r.withUserPresence(out[i])
	}
	return out
}

func (r *Router) withContactListPresence(list domain.ContactList) domain.ContactList {
	if len(list.Contacts) == 0 {
		return list
	}
	out := list
	out.Contacts = append([]domain.Contact(nil), list.Contacts...)
	for i := range out.Contacts {
		out.Contacts[i].User = r.withUserPresence(out.Contacts[i].User)
	}
	return out
}

func (r *Router) withMessageListPresence(list domain.MessageList) domain.MessageList {
	list.Users = r.withUsersPresence(list.Users)
	return list
}

func (r *Router) withDialogListPresence(list domain.DialogList) domain.DialogList {
	list.Users = r.withUsersPresence(list.Users)
	return list
}

func (r *Router) withUserSearchPresence(res domain.UserSearchResult) domain.UserSearchResult {
	res.MyResults = r.withUsersPresence(res.MyResults)
	res.Results = r.withUsersPresence(res.Results)
	return res
}

func (r *Router) tgUser(u domain.User) *tg.User {
	return tgUser(r.withUserPresence(u))
}

func (r *Router) tgSelfUser(u domain.User) *tg.User {
	return tgSelfUser(r.withUserPresence(u))
}

func (r *Router) tgUsers(users []domain.User) []tg.UserClass {
	return tgUsers(r.withUsersPresence(users))
}

func (r *Router) pushUserStatus(ctx context.Context, userID int64, status domain.UserStatus) {
	if userID == 0 {
		return
	}
	update := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateUserStatus{
			UserID: userID,
			Status: tgUserStatus(status),
		}},
		Date: int(r.clock.Now().Unix()),
		Seq:  0,
	}
	r.pushUserUpdates(ctx, userID, update)
	for _, recipientID := range r.onlinePresenceRecipientIDs(ctx, userID) {
		if recipientID == userID {
			continue
		}
		r.pushUserMessage(ctx, recipientID, "push user status", update)
	}
}

func (r *Router) onlinePresenceRecipientIDs(ctx context.Context, userID int64) []int64 {
	if userID == 0 {
		return nil
	}
	seen := map[int64]struct{}{}
	out := make([]int64, 0)
	add := func(ids []int64) {
		for _, id := range ids {
			if id == 0 || id == userID {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	add(r.onlineContactIDs(ctx, userID))
	add(r.onlinePrivateDialogPeerIDs(ctx, userID, seen))
	return out
}

func (r *Router) onlineContactIDs(ctx context.Context, userID int64) []int64 {
	if r.deps.Contacts == nil || userID == 0 {
		return nil
	}
	ids, notModified, err := r.deps.Contacts.ContactIDs(ctx, userID, 0)
	if err != nil || notModified || len(ids) == 0 {
		return nil
	}
	candidates := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		contactID := int64(id)
		if contactID == 0 || contactID == userID {
			continue
		}
		if _, ok := seen[contactID]; ok {
			continue
		}
		seen[contactID] = struct{}{}
		candidates = append(candidates, contactID)
	}
	if len(candidates) == 0 {
		return nil
	}
	if provider, ok := r.deps.Sessions.(OnlineUserProvider); ok {
		return provider.OnlineUserIDsForCandidates(candidates, 0)
	}
	return candidates
}

func (r *Router) onlinePrivateDialogPeerIDs(ctx context.Context, userID int64, already map[int64]struct{}) []int64 {
	if r.deps.Dialogs == nil || userID == 0 {
		return nil
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return nil
	}
	candidates := provider.OnlineUserIDs(presenceDialogFanoutCandidateLimit)
	if len(candidates) == 0 {
		return nil
	}
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: userID}
	out := make([]int64, 0, len(candidates))
	for _, candidateID := range candidates {
		if candidateID == 0 || candidateID == userID {
			continue
		}
		if _, ok := already[candidateID]; ok {
			continue
		}
		list, err := r.deps.Dialogs.GetPeerDialogs(ctx, candidateID, []domain.Peer{peer})
		if err != nil || !dialogListHasPeer(list, peer) {
			continue
		}
		out = append(out, candidateID)
	}
	return out
}

func (r *Router) pushOnlinePeerStatusesToCurrentSession(ctx context.Context, userID int64) {
	peerIDs := r.onlineRelevantPeerIDs(ctx, userID)
	if len(peerIDs) == 0 {
		return
	}
	updates := make([]tg.UpdateClass, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		status := r.userPresenceStatus(peerID)
		if status.Kind != domain.UserStatusOnline {
			continue
		}
		updates = append(updates, &tg.UpdateUserStatus{
			UserID: peerID,
			Status: tgUserStatus(status),
		})
	}
	if len(updates) == 0 {
		return
	}
	r.pushCurrentSessionMessage(ctx, "push online peer statuses", &tg.Updates{
		Updates: updates,
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	})
}

func (r *Router) onlineRelevantPeerIDs(ctx context.Context, userID int64) []int64 {
	if userID == 0 {
		return nil
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return nil
	}
	seen := map[int64]struct{}{}
	candidates := make([]int64, 0)
	add := func(id int64) {
		if id == 0 || id == userID {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		candidates = append(candidates, id)
	}
	if r.deps.Contacts != nil {
		ids, notModified, err := r.deps.Contacts.ContactIDs(ctx, userID, 0)
		if err == nil && !notModified {
			for _, id := range ids {
				add(int64(id))
			}
		}
	}
	if r.deps.Dialogs != nil {
		list, err := r.deps.Dialogs.GetDialogs(ctx, userID, domain.DialogFilter{Limit: presenceDialogFanoutCandidateLimit})
		if err == nil {
			for _, dialog := range list.Dialogs {
				if dialog.Peer.Type == domain.PeerTypeUser {
					add(dialog.Peer.ID)
				}
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return provider.OnlineUserIDsForCandidates(candidates, 0)
}

func dialogListHasPeer(list domain.DialogList, peer domain.Peer) bool {
	for _, dialog := range list.Dialogs {
		if dialog.Peer != peer {
			continue
		}
		if dialog.TopMessage != 0 ||
			dialog.TopMessageDate != 0 ||
			dialog.ReadInboxMaxID != 0 ||
			dialog.ReadOutboxMaxID != 0 ||
			dialog.UnreadCount != 0 ||
			dialog.UnreadMentions != 0 ||
			dialog.UnreadReactions != 0 ||
			dialog.Pinned ||
			dialog.UnreadMark ||
			dialog.Draft != nil {
			return true
		}
		for _, msg := range list.Messages {
			if msg.Peer == peer {
				return true
			}
		}
	}
	return false
}
