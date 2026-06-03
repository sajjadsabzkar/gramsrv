// Package memory 提供 store 各接口的内存实现，用作测试替身与本地开发兜底。
//
// 与 store/postgres、store/redisstore 对称：store 主包只定义接口与 DTO，
// 三种后端实现各自独立成包。
package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// AuthKeyStore 是 store.AuthKeyStore 的内存实现。
type AuthKeyStore struct {
	mu   sync.RWMutex
	keys map[[8]byte]store.AuthKeyData
}

// NewAuthKeyStore 创建内存 AuthKeyStore。
func NewAuthKeyStore() *AuthKeyStore {
	return &AuthKeyStore{keys: make(map[[8]byte]store.AuthKeyData)}
}

func (s *AuthKeyStore) Save(_ context.Context, k store.AuthKeyData) error {
	s.mu.Lock()
	s.keys[k.ID] = k
	s.mu.Unlock()
	return nil
}

func (s *AuthKeyStore) Get(_ context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	s.mu.RLock()
	k, ok := s.keys[id]
	s.mu.RUnlock()
	return k, ok, nil
}

// SessionStore 是 store.SessionStore 的内存实现。
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[int64]store.SessionData
}

// NewSessionStore 创建内存 SessionStore。
func NewSessionStore() *SessionStore {
	return &SessionStore{sessions: make(map[int64]store.SessionData)}
}

func (s *SessionStore) Save(_ context.Context, d store.SessionData) error {
	s.mu.Lock()
	s.sessions[d.ID] = d
	s.mu.Unlock()
	return nil
}

func (s *SessionStore) Get(_ context.Context, id int64) (store.SessionData, bool, error) {
	s.mu.RLock()
	d, ok := s.sessions[id]
	s.mu.RUnlock()
	return d, ok, nil
}

func (s *SessionStore) Delete(_ context.Context, id int64) error {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	return nil
}

// UpdateStateStore 是 store.UpdateStateStore 的内存实现。
type UpdateStateStore struct {
	mu     sync.RWMutex
	states map[updateStateKey]domain.UpdateState
}

// UpdateEventStore 是 store.UpdateEventStore 的内存实现。
type UpdateEventStore struct {
	mu     sync.RWMutex
	events map[int64][]domain.UpdateEvent
}

type updateStateKey struct {
	authKeyID [8]byte
	userID    int64
}

// NewUpdateEventStore 创建内存 UpdateEventStore。
func NewUpdateEventStore() *UpdateEventStore {
	return &UpdateEventStore{events: make(map[int64][]domain.UpdateEvent)}
}

func (s *UpdateEventStore) Append(_ context.Context, userID int64, event domain.UpdateEvent) error {
	event.UserID = userID
	event.Message = cloneMessage(event.Message)
	event.MessageIDs = append([]int(nil), event.MessageIDs...)
	event.Peers = append([]domain.Peer(nil), event.Peers...)
	event.Users = append([]domain.User(nil), event.Users...)
	event.Channels = append([]domain.Channel(nil), event.Channels...)
	s.mu.Lock()
	s.events[userID] = append(s.events[userID], event)
	s.mu.Unlock()
	return nil
}

func (s *UpdateEventStore) ListAfter(_ context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error) {
	s.mu.RLock()
	items := append([]domain.UpdateEvent(nil), s.events[userID]...)
	s.mu.RUnlock()
	out := make([]domain.UpdateEvent, 0, len(items))
	for _, event := range items {
		if event.Pts <= pts {
			continue
		}
		event.Message = cloneMessage(event.Message)
		event.MessageIDs = append([]int(nil), event.MessageIDs...)
		event.Peers = append([]domain.Peer(nil), event.Peers...)
		event.Users = append([]domain.User(nil), event.Users...)
		event.Channels = append([]domain.Channel(nil), event.Channels...)
		out = append(out, event)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *UpdateEventStore) Current(_ context.Context, userID int64) (int, error) {
	s.mu.RLock()
	items := s.events[userID]
	s.mu.RUnlock()
	max := 0
	for _, event := range items {
		if event.Pts > max {
			max = event.Pts
		}
	}
	return max, nil
}

// MaxContiguousPts 返回从 1 起无空洞的最大 pts（内存版按 pts_count 连续扫描）。
func (s *UpdateEventStore) MaxContiguousPts(_ context.Context, userID int64) (int, error) {
	s.mu.RLock()
	nextByStart := make(map[int]int, len(s.events[userID]))
	for _, event := range s.events[userID] {
		count := event.PtsCount
		if count <= 0 {
			count = 1
		}
		nextByStart[event.Pts-count] = event.Pts
	}
	s.mu.RUnlock()
	contiguous := 0
	for {
		next, ok := nextByStart[contiguous]
		if !ok {
			break
		}
		contiguous = next
	}
	return contiguous, nil
}

func (s *UpdateEventStore) AdvanceContiguousPts(ctx context.Context, userID int64) (int, error) {
	return s.MaxContiguousPts(ctx, userID)
}

// NewUpdateStateStore 创建内存 UpdateStateStore。
func NewUpdateStateStore() *UpdateStateStore {
	return &UpdateStateStore{states: make(map[updateStateKey]domain.UpdateState)}
}

func (s *UpdateStateStore) Get(_ context.Context, id [8]byte, userID int64) (domain.UpdateState, bool, error) {
	s.mu.RLock()
	st, ok := s.states[updateStateKey{authKeyID: id, userID: userID}]
	s.mu.RUnlock()
	return st, ok, nil
}

func (s *UpdateStateStore) Save(_ context.Context, id [8]byte, userID int64, st domain.UpdateState) error {
	s.mu.Lock()
	s.states[updateStateKey{authKeyID: id, userID: userID}] = st
	s.mu.Unlock()
	return nil
}

func (s *UpdateStateStore) Delete(_ context.Context, id [8]byte, userID int64) error {
	s.mu.Lock()
	delete(s.states, updateStateKey{authKeyID: id, userID: userID})
	s.mu.Unlock()
	return nil
}

func (s *UpdateStateStore) DeleteAuthKey(_ context.Context, id [8]byte) error {
	s.mu.Lock()
	for k := range s.states {
		if k.authKeyID == id {
			delete(s.states, k)
		}
	}
	s.mu.Unlock()
	return nil
}

// TempAuthKeyBindingStore 是 store.TempAuthKeyBindingStore 的内存实现。
type TempAuthKeyBindingStore struct {
	mu sync.RWMutex
	m  map[[8]byte]domain.TempAuthKeyBinding
}

// NewTempAuthKeyBindingStore 创建内存 TempAuthKeyBindingStore。
func NewTempAuthKeyBindingStore() *TempAuthKeyBindingStore {
	return &TempAuthKeyBindingStore{m: make(map[[8]byte]domain.TempAuthKeyBinding)}
}

func (s *TempAuthKeyBindingStore) Save(_ context.Context, b domain.TempAuthKeyBinding) error {
	b.EncryptedMessage = append([]byte(nil), b.EncryptedMessage...)
	s.mu.Lock()
	s.m[b.TempAuthKeyID] = b
	s.mu.Unlock()
	return nil
}

func (s *TempAuthKeyBindingStore) GetByTemp(_ context.Context, tempAuthKeyID [8]byte) (domain.TempAuthKeyBinding, bool, error) {
	s.mu.RLock()
	b, ok := s.m[tempAuthKeyID]
	s.mu.RUnlock()
	if !ok {
		return domain.TempAuthKeyBinding{}, false, nil
	}
	b.EncryptedMessage = append([]byte(nil), b.EncryptedMessage...)
	return b, true, nil
}

// ContactStore 是 store.ContactStore 的内存实现。
type ContactStore struct {
	mu sync.RWMutex
	m  map[int64]domain.ContactList
}

// NewContactStore 创建内存 ContactStore。
func NewContactStore() *ContactStore {
	return &ContactStore{m: make(map[int64]domain.ContactList)}
}

func (s *ContactStore) ListByUser(_ context.Context, userID int64) (domain.ContactList, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	list.Contacts = cloneContacts(list.Contacts)
	list.Hash = contactListHash(list.Contacts)
	return list, nil
}

func (s *ContactStore) Get(_ context.Context, userID, contactUserID int64) (domain.Contact, bool, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	for _, contact := range list.Contacts {
		if contact.User.ID == contactUserID {
			return cloneContact(contact), true, nil
		}
	}
	return domain.Contact{}, false, nil
}

func (s *ContactStore) Upsert(_ context.Context, userID int64, input domain.ContactInput) (domain.Contact, error) {
	contact := domain.Contact{
		User: domain.User{
			ID:        input.ContactUserID,
			Phone:     input.Phone,
			FirstName: input.FirstName,
			LastName:  input.LastName,
			Contact:   true,
		},
		FirstName:    input.FirstName,
		LastName:     input.LastName,
		Phone:        input.Phone,
		Note:         input.Note,
		NoteEntities: append([]domain.MessageEntity(nil), input.NoteEntities...),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	reverse := s.m[input.ContactUserID]
	for i := range reverse.Contacts {
		if reverse.Contacts[i].User.ID == userID {
			reverse.Contacts[i].Mutual = true
			reverse.Contacts[i].User.Mutual = true
			contact.Mutual = true
			contact.User.Mutual = true
			s.m[input.ContactUserID] = reverse
			break
		}
	}
	for i, existing := range list.Contacts {
		if existing.User.ID != input.ContactUserID {
			continue
		}
		contact.User.AccessHash = existing.User.AccessHash
		contact.User.Username = existing.User.Username
		contact.User.CountryCode = existing.User.CountryCode
		contact.User.Verified = existing.User.Verified
		contact.User.Support = existing.User.Support
		if contact.Phone == "" {
			contact.User.Phone = existing.User.Phone
		}
		if contact.FirstName == "" {
			contact.User.FirstName = existing.User.FirstName
		}
		if contact.LastName == "" {
			contact.User.LastName = existing.User.LastName
		}
		list.Contacts[i] = contact
		list.Hash = contactListHash(list.Contacts)
		s.m[userID] = list
		return cloneContact(contact), nil
	}
	list.Contacts = append(list.Contacts, contact)
	list.Hash = contactListHash(list.Contacts)
	s.m[userID] = list
	return cloneContact(contact), nil
}

func (s *ContactStore) UpsertMany(ctx context.Context, userID int64, inputs []domain.ContactInput) ([]domain.Contact, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	out := make([]domain.Contact, 0, len(inputs))
	for _, input := range inputs {
		contact, err := s.Upsert(ctx, userID, input)
		if err != nil {
			return nil, err
		}
		out = append(out, contact)
	}
	return out, nil
}

func (s *ContactStore) UpdateNote(_ context.Context, userID, contactUserID int64, note string, entities []domain.MessageEntity) (domain.Contact, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	for i := range list.Contacts {
		if list.Contacts[i].User.ID != contactUserID {
			continue
		}
		list.Contacts[i].Note = note
		list.Contacts[i].NoteEntities = append([]domain.MessageEntity(nil), entities...)
		list.Hash = contactListHash(list.Contacts)
		s.m[userID] = list
		return cloneContact(list.Contacts[i]), true, nil
	}
	return domain.Contact{}, false, nil
}

func (s *ContactStore) Delete(_ context.Context, userID int64, contactUserIDs []int64) (int, error) {
	remove := make(map[int64]struct{}, len(contactUserIDs))
	for _, id := range contactUserIDs {
		if id != 0 {
			remove[id] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	out := list.Contacts[:0]
	deleted := 0
	for _, contact := range list.Contacts {
		if _, ok := remove[contact.User.ID]; ok {
			deleted++
			if reverse := s.m[contact.User.ID]; len(reverse.Contacts) > 0 {
				for i := range reverse.Contacts {
					if reverse.Contacts[i].User.ID == userID {
						reverse.Contacts[i].Mutual = false
						reverse.Contacts[i].User.Mutual = false
					}
				}
				reverse.Hash = contactListHash(reverse.Contacts)
				s.m[contact.User.ID] = reverse
			}
			continue
		}
		out = append(out, contact)
	}
	list.Contacts = out
	list.Hash = contactListHash(list.Contacts)
	s.m[userID] = list
	return deleted, nil
}

// SaveList 保存一份用户通讯录，供测试和本地替身使用。
func (s *ContactStore) SaveList(_ context.Context, userID int64, list domain.ContactList) error {
	list.Contacts = cloneContacts(list.Contacts)
	list.Hash = contactListHash(list.Contacts)
	s.mu.Lock()
	s.m[userID] = list
	s.mu.Unlock()
	return nil
}

// DialogStore 是 store.DialogStore 的内存实现。
type DialogStore struct {
	mu          sync.RWMutex
	m           map[int64]domain.DialogList
	drafts      map[int64]map[dialogDraftKey]domain.DialogDraft
	folders     map[int64]map[int]domain.DialogFolder
	folderOrder map[int64][]int
	folderTags  map[int64]bool
}

type dialogDraftKey struct {
	peerType     domain.PeerType
	peerID       int64
	topMessageID int
}

// NewDialogStore 创建内存 DialogStore。
func NewDialogStore() *DialogStore {
	return &DialogStore{
		m:           make(map[int64]domain.DialogList),
		drafts:      make(map[int64]map[dialogDraftKey]domain.DialogDraft),
		folders:     make(map[int64]map[int]domain.DialogFolder),
		folderOrder: make(map[int64][]int),
		folderTags:  make(map[int64]bool),
	}
}

func (s *DialogStore) ListByUser(_ context.Context, userID int64, filter domain.DialogFilter) (domain.DialogList, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	list.Dialogs = cloneDialogs(list.Dialogs)
	list.Messages = cloneMessages(list.Messages)
	list.Users = append([]domain.User(nil), list.Users...)
	return filterDialogList(list, filter), nil
}

func (s *DialogStore) ListByPeers(_ context.Context, userID int64, peers []domain.Peer) (domain.DialogList, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	list.Dialogs = cloneDialogs(list.Dialogs)
	list.Messages = cloneMessages(list.Messages)
	list.Users = append([]domain.User(nil), list.Users...)

	byPeer := make(map[domain.Peer]domain.Dialog, len(list.Dialogs))
	for _, dialog := range list.Dialogs {
		byPeer[dialog.Peer] = dialog
	}
	out := domain.DialogList{
		Dialogs: make([]domain.Dialog, 0, len(peers)),
		Users:   make([]domain.User, 0, len(peers)),
	}
	seenPeers := make(map[domain.Peer]struct{}, len(peers))
	seenUsers := map[int64]struct{}{}
	for _, peer := range peers {
		if _, ok := seenPeers[peer]; ok {
			continue
		}
		seenPeers[peer] = struct{}{}
		dialog := byPeer[peer]
		if dialog.Peer.ID == 0 {
			dialog.Peer = peer
		}
		out.Dialogs = append(out.Dialogs, dialog)
		if peer.Type == domain.PeerTypeUser {
			if user, ok := findDialogUser(list.Users, peer.ID); ok {
				appendDialogUser(&out, seenUsers, user)
			} else if peer.ID == domain.OfficialSystemUserID {
				appendDialogUser(&out, seenUsers, domain.OfficialSystemUser())
			}
		}
	}
	out.Messages = keepDialogMessages(list.Messages, out.Dialogs)
	out.Count = len(out.Dialogs)
	out.Hash = dialogListHash(out.Dialogs)
	return out, nil
}

// SaveList 保存一份用户会话列表，供测试和本地替身使用。
func (s *DialogStore) SaveList(_ context.Context, userID int64, list domain.DialogList) error {
	list.Dialogs = cloneDialogs(list.Dialogs)
	list.Messages = cloneMessages(list.Messages)
	list.Users = append([]domain.User(nil), list.Users...)
	s.mu.Lock()
	s.m[userID] = list
	s.mu.Unlock()
	return nil
}

func (s *DialogStore) Upsert(_ context.Context, userID int64, dialog domain.Dialog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	for i, existing := range list.Dialogs {
		if existing.Peer == dialog.Peer {
			if dialog.FolderID == domain.DialogMainFolderID && existing.FolderID != domain.DialogMainFolderID {
				dialog.FolderID = existing.FolderID
			}
			list.Dialogs[i] = dialog
			s.m[userID] = list
			return nil
		}
	}
	list.Dialogs = append(list.Dialogs, dialog)
	s.m[userID] = list
	return nil
}

func (s *DialogStore) SaveDraft(_ context.Context, userID int64, draft domain.DialogDraft) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drafts[userID] == nil {
		s.drafts[userID] = make(map[dialogDraftKey]domain.DialogDraft)
	}
	s.drafts[userID][draftKey(draft.Peer, draft.TopMessageID)] = cloneDialogDraft(draft)
	return nil
}

func (s *DialogStore) DeleteDraft(_ context.Context, userID int64, peer domain.Peer, topMessageID int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.drafts[userID]
	if len(items) == 0 {
		return false, nil
	}
	key := draftKey(peer, topMessageID)
	if _, ok := items[key]; !ok {
		return false, nil
	}
	delete(items, key)
	return true, nil
}

func (s *DialogStore) ListDrafts(_ context.Context, userID int64, limit int) ([]domain.DialogDraft, error) {
	s.mu.RLock()
	items := s.drafts[userID]
	out := make([]domain.DialogDraft, 0, len(items))
	for _, draft := range items {
		out = append(out, cloneDialogDraft(draft))
	}
	s.mu.RUnlock()
	sortDialogDrafts(out)
	if limit <= 0 || limit > domain.MaxDialogDraftsPerUser {
		limit = domain.MaxDialogDraftsPerUser
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *DialogStore) ClearDrafts(_ context.Context, userID int64, limit int) ([]domain.DialogDraft, error) {
	if limit <= 0 || limit > domain.MaxDialogDraftsPerUser {
		limit = domain.MaxDialogDraftsPerUser
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.drafts[userID]
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]domain.DialogDraft, 0, len(items))
	for _, draft := range items {
		out = append(out, cloneDialogDraft(draft))
	}
	sortDialogDrafts(out)
	if len(out) > limit {
		out = out[:limit]
	}
	for _, draft := range out {
		delete(items, draftKey(draft.Peer, draft.TopMessageID))
	}
	return out, nil
}

func (s *DialogStore) MarkRead(_ context.Context, userID int64, peer domain.Peer, maxID int) (domain.ReadHistoryResult, error) {
	result := domain.ReadHistoryResult{OwnerUserID: userID, Peer: peer, MaxID: maxID}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	for i, dialog := range list.Dialogs {
		if dialog.Peer != peer {
			continue
		}
		readMax := maxID
		if readMax <= 0 {
			readMax = dialog.TopMessage
		}
		result.MaxID = readMax
		result.Changed = dialog.UnreadCount > 0 || readMax > dialog.ReadInboxMaxID
		if readMax > dialog.ReadInboxMaxID {
			dialog.ReadInboxMaxID = readMax
		}
		dialog.UnreadCount = 0
		dialog.UnreadMentions = 0
		dialog.UnreadReactions = 0
		dialog.UnreadMark = false
		result.StillUnreadCount = dialog.UnreadCount
		list.Dialogs[i] = dialog
		s.m[userID] = list
		return result, nil
	}
	return result, nil
}

func (s *DialogStore) SetPinned(_ context.Context, userID int64, peer domain.Peer, pinned bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	nextOrder := 1
	for _, dialog := range list.Dialogs {
		if dialog.Pinned && dialog.PinnedOrder >= nextOrder {
			nextOrder = dialog.PinnedOrder + 1
		}
	}
	for i := range list.Dialogs {
		if list.Dialogs[i].Peer != peer {
			continue
		}
		list.Dialogs[i].Pinned = pinned
		if pinned {
			if list.Dialogs[i].PinnedOrder == 0 {
				list.Dialogs[i].PinnedOrder = nextOrder
			}
		} else {
			list.Dialogs[i].PinnedOrder = 0
		}
		s.m[userID] = list
		return true, nil
	}
	return false, nil
}

func (s *DialogStore) ReorderPinned(_ context.Context, userID int64, order []domain.Peer, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	positions := make(map[domain.Peer]int, len(order))
	for i, peer := range order {
		if peer.Type == "" || peer.ID == 0 {
			continue
		}
		if _, ok := positions[peer]; ok {
			continue
		}
		positions[peer] = len(order) - i
	}
	for i := range list.Dialogs {
		pos, ok := positions[list.Dialogs[i].Peer]
		if ok {
			list.Dialogs[i].Pinned = true
			list.Dialogs[i].PinnedOrder = pos
			continue
		}
		if force && list.Dialogs[i].Pinned {
			list.Dialogs[i].Pinned = false
			list.Dialogs[i].PinnedOrder = 0
		}
	}
	s.m[userID] = list
	return nil
}

func (s *DialogStore) SetUnreadMark(_ context.Context, userID int64, peer domain.Peer, unread bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	for i := range list.Dialogs {
		if list.Dialogs[i].Peer != peer {
			continue
		}
		list.Dialogs[i].UnreadMark = unread
		s.m[userID] = list
		return true, nil
	}
	return false, nil
}

func (s *DialogStore) ListUnreadMarked(_ context.Context, userID int64) ([]domain.Peer, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	out := make([]domain.Peer, 0, len(list.Dialogs))
	for _, dialog := range list.Dialogs {
		if dialog.UnreadMark {
			out = append(out, dialog.Peer)
		}
	}
	return out, nil
}

func (s *DialogStore) SetPeerSettingsBarHidden(_ context.Context, userID int64, peer domain.Peer) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	for i := range list.Dialogs {
		if list.Dialogs[i].Peer != peer {
			continue
		}
		list.Dialogs[i].PeerSettingsBarHidden = true
		s.m[userID] = list
		return true, nil
	}
	return false, nil
}

func (s *DialogStore) PeerSettingsBarHidden(_ context.Context, userID int64, peer domain.Peer) (bool, error) {
	s.mu.RLock()
	list := s.m[userID]
	s.mu.RUnlock()
	for _, dialog := range list.Dialogs {
		if dialog.Peer == peer {
			return dialog.PeerSettingsBarHidden, nil
		}
	}
	return false, nil
}

func (s *DialogStore) ListFolders(_ context.Context, userID int64) (domain.DialogFolderList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byID := s.folders[userID]
	order := append([]int(nil), s.folderOrder[userID]...)
	seen := make(map[int]struct{}, len(byID))
	out := domain.DialogFolderList{
		TagsEnabled: s.folderTags[userID],
		Folders:     make([]domain.DialogFolder, 0, len(byID)),
	}
	for _, id := range order {
		folder, ok := byID[id]
		if !ok {
			continue
		}
		seen[id] = struct{}{}
		out.Folders = append(out.Folders, cloneDialogFolder(folder))
	}
	remaining := make([]int, 0, len(byID))
	for id := range byID {
		if _, ok := seen[id]; !ok {
			remaining = append(remaining, id)
		}
	}
	sort.Ints(remaining)
	for _, id := range remaining {
		out.Folders = append(out.Folders, cloneDialogFolder(byID[id]))
	}
	return out, nil
}

func (s *DialogStore) GetFolder(_ context.Context, userID int64, folderID int) (domain.DialogFolder, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	folder, ok := s.folders[userID][folderID]
	if !ok {
		return domain.DialogFolder{}, false, nil
	}
	return cloneDialogFolder(folder), true, nil
}

func (s *DialogStore) UpsertFolder(_ context.Context, userID int64, folder domain.DialogFolder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.folders[userID] == nil {
		s.folders[userID] = make(map[int]domain.DialogFolder)
	}
	s.folders[userID][folder.ID] = cloneDialogFolder(folder)
	if !containsInt(s.folderOrder[userID], folder.ID) {
		s.folderOrder[userID] = append(s.folderOrder[userID], folder.ID)
	}
	return nil
}

func (s *DialogStore) DeleteFolder(_ context.Context, userID int64, folderID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.folders[userID], folderID)
	s.folderOrder[userID] = removeInt(s.folderOrder[userID], folderID)
	return nil
}

func (s *DialogStore) ReorderFolders(_ context.Context, userID int64, order []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byID := s.folders[userID]
	seen := make(map[int]struct{}, len(order))
	next := make([]int, 0, len(byID))
	for _, id := range order {
		if id < domain.DialogCustomFolderMinID {
			continue
		}
		if _, ok := byID[id]; !ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		next = append(next, id)
	}
	remaining := make([]int, 0, len(byID))
	for id := range byID {
		if _, ok := seen[id]; !ok {
			remaining = append(remaining, id)
		}
	}
	sort.Ints(remaining)
	next = append(next, remaining...)
	s.folderOrder[userID] = next
	return nil
}

func (s *DialogStore) SetFolderTagsEnabled(_ context.Context, userID int64, enabled bool) error {
	s.mu.Lock()
	s.folderTags[userID] = enabled
	s.mu.Unlock()
	return nil
}

func (s *DialogStore) EditPeerFolders(_ context.Context, userID int64, peers []domain.FolderPeerUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.m[userID]
	updates := make(map[domain.Peer]int, len(peers))
	for _, item := range peers {
		if item.Peer.Type == "" || item.Peer.ID == 0 {
			continue
		}
		updates[item.Peer] = item.FolderID
	}
	for i := range list.Dialogs {
		if folderID, ok := updates[list.Dialogs[i].Peer]; ok {
			list.Dialogs[i].FolderID = folderID
		}
	}
	s.m[userID] = list
	return nil
}

// MessageStore 是 store.MessageStore 的内存实现。
type MessageStore struct {
	mu               sync.RWMutex
	m                map[int64][]domain.Message
	nextUID          int64
	nextBox          map[int64]int
	nextPts          map[int64]int
	readOutboxDates  map[readOutboxDateKey]int
	privateReactions map[int64]map[int64][]domain.ChannelMessagePeerReaction
	dialogs          *DialogStore
}

type readOutboxDateKey struct {
	ownerUserID int64
	peerID      int64
	msgID       int
}

// NewMessageStore 创建内存 MessageStore。
func NewMessageStore(dialogs ...*DialogStore) *MessageStore {
	s := &MessageStore{
		m:                make(map[int64][]domain.Message),
		nextUID:          1,
		nextBox:          make(map[int64]int),
		nextPts:          make(map[int64]int),
		readOutboxDates:  make(map[readOutboxDateKey]int),
		privateReactions: make(map[int64]map[int64][]domain.ChannelMessagePeerReaction),
	}
	if len(dialogs) > 0 {
		s.dialogs = dialogs[0]
	}
	return s
}

func (s *MessageStore) Create(_ context.Context, msg domain.Message) (domain.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.ID = s.nextBoxIDLocked(msg.OwnerUserID)
	msg.UID = s.nextUID
	s.nextUID++
	msg.Entities = append([]domain.MessageEntity(nil), msg.Entities...)
	s.m[msg.OwnerUserID] = append(s.m[msg.OwnerUserID], msg)
	if s.dialogs != nil {
		s.dialogs.mu.Lock()
		list := s.dialogs.m[msg.OwnerUserID]
		list.Messages = append(list.Messages, msg)
		if msg.Peer.Type == domain.PeerTypeUser && msg.Peer.ID == domain.OfficialSystemUserID && !hasUser(list.Users, domain.OfficialSystemUserID) {
			list.Users = append(list.Users, domain.OfficialSystemUser())
		}
		s.dialogs.m[msg.OwnerUserID] = list
		s.dialogs.mu.Unlock()
	}
	return msg, nil
}

func (s *MessageStore) SendPrivateText(_ context.Context, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, msg := range s.m[req.SenderUserID] {
		if msg.RandomID != 0 && msg.RandomID == req.RandomID {
			recipient := domain.Message{}
			if req.SenderUserID != req.RecipientUserID {
				for _, peerMsg := range s.m[req.RecipientUserID] {
					if peerMsg.UID == msg.UID {
						recipient = peerMsg
						break
					}
				}
			} else {
				recipient = msg
			}
			return domain.SendPrivateTextResult{
				SenderMessage:    cloneMessage(msg),
				RecipientMessage: cloneMessage(recipient),
				SenderEvent:      newMessageEvent(msg),
				RecipientEvent:   newMessageEvent(recipient),
				Duplicate:        true,
			}, nil
		}
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	senderReply, recipientReply, err := s.resolveMemoryReplyLocked(req)
	if err != nil {
		return domain.SendPrivateTextResult{}, err
	}
	uid := s.nextUID
	s.nextUID++
	sender := domain.Message{
		ID:          s.nextBoxIDLocked(req.SenderUserID),
		UID:         uid,
		RandomID:    req.RandomID,
		OwnerUserID: req.SenderUserID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: req.RecipientUserID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: req.SenderUserID},
		Date:        req.Date,
		Out:         true,
		Silent:      req.Silent,
		NoForwards:  req.NoForwards,
		Body:        req.Message,
		Entities:    append([]domain.MessageEntity(nil), req.Entities...),
		Media:       req.Media,
		ReplyTo:     cloneMessageReply(senderReply),
		Forward:     cloneMessageForward(req.Forward),
		Pts:         s.nextPtsLocked(req.SenderUserID),
	}
	recipient := sender
	if req.SenderUserID != req.RecipientUserID {
		recipient.ID = s.nextBoxIDLocked(req.RecipientUserID)
		recipient.OwnerUserID = req.RecipientUserID
		recipient.Peer = domain.Peer{Type: domain.PeerTypeUser, ID: req.SenderUserID}
		recipient.Out = false
		recipient.ReplyTo = cloneMessageReply(recipientReply)
		recipient.Pts = s.nextPtsLocked(req.RecipientUserID)
	}
	s.m[req.SenderUserID] = append(s.m[req.SenderUserID], sender)
	if req.SenderUserID != req.RecipientUserID {
		s.m[req.RecipientUserID] = append(s.m[req.RecipientUserID], recipient)
	}
	if s.dialogs != nil {
		s.upsertMemoryDialogsLocked(sender, recipient)
	}
	return domain.SendPrivateTextResult{
		SenderMessage:    cloneMessage(sender),
		RecipientMessage: cloneMessage(recipient),
		SenderEvent:      newMessageEvent(sender),
		RecipientEvent:   newMessageEvent(recipient),
	}, nil
}

func (s *MessageStore) resolveMemoryReplyLocked(req domain.SendPrivateTextRequest) (*domain.MessageReply, *domain.MessageReply, error) {
	if req.ReplyTo == nil {
		return nil, nil, nil
	}
	if err := domain.ValidateMessageReplyBounds(req.ReplyTo); err != nil {
		return nil, nil, err
	}
	peer := req.ReplyTo.Peer
	if peer.ID == 0 {
		peer = domain.Peer{Type: domain.PeerTypeUser, ID: req.RecipientUserID}
	}
	if peer.Type != domain.PeerTypeUser || peer.ID != req.RecipientUserID {
		return nil, nil, domain.ErrReplyMessageIDInvalid
	}
	var target domain.Message
	for _, msg := range s.m[req.SenderUserID] {
		if msg.Peer == peer && msg.ID == req.ReplyTo.MessageID {
			target = msg
			break
		}
	}
	if target.ID == 0 {
		return nil, nil, domain.ErrReplyMessageIDInvalid
	}
	senderReply := cloneMessageReply(req.ReplyTo)
	senderReply.MessageID = target.ID
	senderReply.Peer = peer
	if req.SenderUserID == req.RecipientUserID {
		return senderReply, cloneMessageReply(senderReply), nil
	}
	for _, msg := range s.m[req.RecipientUserID] {
		if msg.UID == target.UID {
			recipientReply := cloneMessageReply(senderReply)
			recipientReply.MessageID = msg.ID
			recipientReply.Peer = domain.Peer{Type: domain.PeerTypeUser, ID: req.SenderUserID}
			return senderReply, recipientReply, nil
		}
	}
	return senderReply, nil, nil
}

func (s *MessageStore) ForwardPrivateMessages(ctx context.Context, req domain.ForwardPrivateMessagesRequest) (domain.ForwardPrivateMessagesResult, error) {
	res := domain.ForwardPrivateMessagesResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.ToUserID == 0 || req.FromPeer.Type != domain.PeerTypeUser || req.FromPeer.ID == 0 {
		return res, domain.ErrMessageIDInvalid
	}
	if len(req.MessageIDs) == 0 || len(req.MessageIDs) != len(req.RandomIDs) {
		return res, domain.ErrMessageIDInvalid
	}
	if len(req.MessageIDs) > domain.MaxForwardMessageIDs {
		return res, domain.ErrMessageIDInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	s.mu.RLock()
	sources := make([]domain.Message, 0, len(req.MessageIDs))
	for _, id := range req.MessageIDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			s.mu.RUnlock()
			return res, domain.ErrMessageIDInvalid
		}
		var source domain.Message
		for _, msg := range s.m[req.OwnerUserID] {
			if msg.Peer == req.FromPeer && msg.ID == id {
				source = cloneMessage(msg)
				break
			}
		}
		if source.ID == 0 {
			s.mu.RUnlock()
			return res, domain.ErrMessageIDInvalid
		}
		if source.NoForwards {
			s.mu.RUnlock()
			return res, domain.ErrChatForwardsRestricted
		}
		sources = append(sources, source)
	}
	s.mu.RUnlock()

	res.SenderMessages = make([]domain.Message, 0, len(sources))
	res.RecipientMessages = make([]domain.Message, 0, len(sources))
	res.SenderEvents = make([]domain.UpdateEvent, 0, len(sources))
	res.RecipientEvents = make([]domain.UpdateEvent, 0, len(sources))
	res.Duplicates = make([]bool, 0, len(sources))
	for i, source := range sources {
		if req.RandomIDs[i] == 0 {
			return res, domain.ErrMessageIDInvalid
		}
		var forward *domain.MessageForward
		if !req.DropAuthor {
			forward = cloneMessageForward(source.Forward)
			if forward == nil {
				forward = &domain.MessageForward{From: source.From, Date: source.Date}
			}
		}
		sent, err := s.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:    req.OwnerUserID,
			RecipientUserID: req.ToUserID,
			RandomID:        req.RandomIDs[i],
			Message:         source.Body,
			Entities:        append([]domain.MessageEntity(nil), source.Entities...),
			Silent:          req.Silent,
			NoForwards:      req.NoForwards,
			ReplyTo:         req.ReplyTo,
			Forward:         forward,
			Date:            req.Date,
			OriginAuthKeyID: req.OriginAuthKeyID,
			OriginSessionID: req.OriginSessionID,
		})
		if err != nil {
			return res, err
		}
		res.SenderMessages = append(res.SenderMessages, sent.SenderMessage)
		res.RecipientMessages = append(res.RecipientMessages, sent.RecipientMessage)
		res.SenderEvents = append(res.SenderEvents, sent.SenderEvent)
		res.RecipientEvents = append(res.RecipientEvents, sent.RecipientEvent)
		res.Duplicates = append(res.Duplicates, sent.Duplicate)
	}
	return res, nil
}

func (s *MessageStore) GetByIDs(_ context.Context, userID int64, ids []int) (domain.MessageList, error) {
	if userID == 0 || len(ids) == 0 {
		return domain.MessageList{}, nil
	}
	s.mu.RLock()
	byID := make(map[int]domain.Message, len(s.m[userID]))
	for _, msg := range s.m[userID] {
		item := cloneMessage(msg)
		reactions := s.privateMessageReactionsLocked(item.UID, item.OwnerUserID)
		if len(reactions.Results) > 0 || len(reactions.Recent) > 0 {
			item.Reactions = cloneChannelMessageReactionsPtr(&reactions)
		}
		byID[msg.ID] = item
	}
	s.mu.RUnlock()
	out := domain.MessageList{Messages: make([]domain.Message, 0, len(ids))}
	for _, id := range ids {
		if msg, ok := byID[id]; ok {
			out.Messages = append(out.Messages, msg)
		}
	}
	out.Users = usersForMessages(out.Messages)
	out.Hash = messageListHash(out.Messages)
	return out, nil
}

func (s *MessageStore) ListByUser(_ context.Context, userID int64, filter domain.MessageFilter) (domain.MessageList, error) {
	s.mu.RLock()
	messages := cloneMessages(s.m[userID])
	for i := range messages {
		reactions := s.privateMessageReactionsLocked(messages[i].UID, messages[i].OwnerUserID)
		if len(reactions.Results) > 0 || len(reactions.Recent) > 0 {
			messages[i].Reactions = cloneChannelMessageReactionsPtr(&reactions)
		}
	}
	s.mu.RUnlock()
	return filterMessageList(messages, filter), nil
}

func (s *MessageStore) ReadHistory(_ context.Context, req domain.ReadHistoryRequest) (domain.ReadHistoryResult, error) {
	res := domain.ReadHistoryResult{OwnerUserID: req.OwnerUserID, Peer: req.Peer, MaxID: req.MaxID}
	if req.OwnerUserID == 0 || req.Peer.ID == 0 {
		return res, nil
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dialogs == nil {
		return res, nil
	}
	s.dialogs.mu.Lock()
	defer s.dialogs.mu.Unlock()
	list := s.dialogs.m[req.OwnerUserID]
	for i, dialog := range list.Dialogs {
		if dialog.Peer != req.Peer {
			continue
		}
		readMax := req.MaxID
		if readMax <= 0 {
			readMax = dialog.TopMessage
		}
		if readMax > domain.MaxMessageBoxID {
			readMax = domain.MaxMessageBoxID
		}
		oldRead := dialog.ReadInboxMaxID
		res.MaxID = readMax
		res.Changed = dialog.UnreadCount > 0 || readMax > oldRead
		if !res.Changed {
			return res, nil
		}
		var latestIncoming domain.Message
		unread := 0
		for _, msg := range s.m[req.OwnerUserID] {
			if msg.Peer != req.Peer || msg.Out {
				continue
			}
			if msg.ID > readMax {
				unread++
				continue
			}
			if msg.ID > oldRead && msg.ID > latestIncoming.ID {
				latestIncoming = msg
			}
		}
		if readMax > dialog.ReadInboxMaxID {
			dialog.ReadInboxMaxID = readMax
		}
		dialog.UnreadCount = unread
		dialog.UnreadMentions = 0
		dialog.UnreadReactions = 0
		dialog.UnreadMark = false
		res.StillUnreadCount = unread
		pts := s.nextPtsLocked(req.OwnerUserID)
		res.InboxEvent = domain.UpdateEvent{
			UserID:           req.OwnerUserID,
			Type:             domain.UpdateEventReadHistoryInbox,
			Pts:              pts,
			PtsCount:         1,
			Date:             req.Date,
			Peer:             req.Peer,
			MaxID:            readMax,
			StillUnreadCount: unread,
		}
		list.Dialogs[i] = dialog
		s.dialogs.m[req.OwnerUserID] = list

		if latestIncoming.ID != 0 && latestIncoming.From.ID != 0 && latestIncoming.From.ID != req.OwnerUserID {
			senderUserID := latestIncoming.From.ID
			senderBoxID := 0
			for _, msg := range s.m[senderUserID] {
				if msg.UID == latestIncoming.UID && msg.Out {
					senderBoxID = msg.ID
					break
				}
			}
			if senderBoxID > 0 {
				senderList := s.dialogs.m[senderUserID]
				for j, senderDialog := range senderList.Dialogs {
					if senderDialog.Peer != (domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID}) {
						continue
					}
					if senderBoxID <= senderDialog.ReadOutboxMaxID {
						break
					}
					oldOutbox := senderDialog.ReadOutboxMaxID
					senderDialog.ReadOutboxMaxID = senderBoxID
					senderList.Dialogs[j] = senderDialog
					s.dialogs.m[senderUserID] = senderList
					for _, msg := range s.m[senderUserID] {
						if msg.Peer == (domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID}) && msg.Out && msg.ID > oldOutbox && msg.ID <= senderBoxID {
							s.readOutboxDates[readOutboxDateKey{ownerUserID: senderUserID, peerID: req.OwnerUserID, msgID: msg.ID}] = req.Date
						}
					}
					outPts := s.nextPtsLocked(senderUserID)
					res.OutboxChanged = true
					res.OutboxUserID = senderUserID
					res.OutboxEvent = domain.UpdateEvent{
						UserID:   senderUserID,
						Type:     domain.UpdateEventReadHistoryOutbox,
						Pts:      outPts,
						PtsCount: 1,
						Date:     req.Date,
						Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID},
						MaxID:    senderBoxID,
					}
					break
				}
			}
		}
		return res, nil
	}
	return res, nil
}

func (s *MessageStore) ReadMessageContents(_ context.Context, req domain.ReadMessageContentsRequest) (domain.ReadMessageContentsResult, error) {
	res := domain.ReadMessageContentsResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 {
		return res, fmt.Errorf("read message contents: missing owner user id")
	}
	if len(req.IDs) > domain.MaxGetMessageIDs {
		return res, domain.ErrMessageIDInvalid
	}
	wanted := make(map[int]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return res, domain.ErrMessageIDInvalid
		}
		wanted[id] = struct{}{}
	}
	if len(wanted) == 0 {
		return res, nil
	}
	s.mu.RLock()
	for _, msg := range s.m[req.OwnerUserID] {
		if _, ok := wanted[msg.ID]; ok {
			res.MessageIDs = append(res.MessageIDs, msg.ID)
		}
	}
	s.mu.RUnlock()
	sort.Ints(res.MessageIDs)
	return res, nil
}

func (s *MessageStore) GetOutboxReadDate(_ context.Context, req domain.OutboxReadDateRequest) (int, error) {
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return 0, domain.ErrMessageIDInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	found := false
	for _, msg := range s.m[req.OwnerUserID] {
		if msg.ID == req.ID && msg.Peer == req.Peer && msg.Out {
			found = true
			break
		}
	}
	if !found {
		return 0, domain.ErrMessageIDInvalid
	}
	date := s.readOutboxDates[readOutboxDateKey{ownerUserID: req.OwnerUserID, peerID: req.Peer.ID, msgID: req.ID}]
	if date == 0 {
		return 0, domain.ErrMessageNotReadYet
	}
	return date, nil
}

func (s *MessageStore) SetMessageReactions(_ context.Context, req domain.SetPrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if req.UserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if len(req.Reactions) > domain.MaxChannelMessageReactionsPerUser {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var target domain.Message
	for _, msg := range s.m[req.UserID] {
		if msg.ID == req.MessageID && msg.Peer == req.Peer {
			target = msg
			break
		}
	}
	if target.ID == 0 || target.UID == 0 {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	if _, ok := s.privateReactions[target.UID]; !ok {
		s.privateReactions[target.UID] = make(map[int64][]domain.ChannelMessagePeerReaction)
	}
	rows := make([]domain.ChannelMessagePeerReaction, 0, len(req.Reactions))
	for i, reaction := range req.Reactions {
		if reaction.Type != domain.MessageReactionEmoji || strings.TrimSpace(reaction.Emoticon) == "" {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
		rows = append(rows, domain.ChannelMessagePeerReaction{
			UserID:      req.UserID,
			Reaction:    reaction,
			Big:         req.Big,
			My:          true,
			ChosenOrder: i + 1,
			Date:        req.Date,
		})
	}
	if len(rows) == 0 {
		delete(s.privateReactions[target.UID], req.UserID)
	} else {
		s.privateReactions[target.UID][req.UserID] = rows
	}
	return s.privateReactionResultLocked(target.UID), nil
}

func (s *MessageStore) GetMessageReactions(_ context.Context, req domain.PrivateMessageReactionsRequest) (domain.PrivateMessageReactionsResult, error) {
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 || len(req.IDs) > domain.MaxGetMessageIDs {
		return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
	}
	ids := make(map[int]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id <= 0 || id > domain.MaxMessageBoxID {
			return domain.PrivateMessageReactionsResult{}, domain.ErrMessageIDInvalid
		}
		ids[id] = struct{}{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := domain.PrivateMessageReactionsResult{}
	for _, msg := range s.m[req.OwnerUserID] {
		if msg.Peer != req.Peer {
			continue
		}
		if _, ok := ids[msg.ID]; !ok {
			continue
		}
		item := cloneMessage(msg)
		reactions := s.privateMessageReactionsLocked(msg.UID, msg.OwnerUserID)
		item.Reactions = cloneChannelMessageReactionsPtr(&reactions)
		out.Messages = append(out.Messages, item)
		if len(out.Reactions.Results) == 0 && len(out.Reactions.Recent) == 0 {
			out.Reactions = reactions
		}
	}
	return out, nil
}

func (s *MessageStore) privateReactionResultLocked(uid int64) domain.PrivateMessageReactionsResult {
	out := domain.PrivateMessageReactionsResult{}
	for _, messages := range s.m {
		for _, msg := range messages {
			if msg.UID != uid {
				continue
			}
			item := cloneMessage(msg)
			reactions := s.privateMessageReactionsLocked(uid, msg.OwnerUserID)
			item.Reactions = cloneChannelMessageReactionsPtr(&reactions)
			out.Messages = append(out.Messages, item)
			if len(out.Reactions.Results) == 0 && len(out.Reactions.Recent) == 0 {
				out.Reactions = reactions
			}
		}
	}
	return out
}

func (s *MessageStore) privateMessageReactionsLocked(uid, viewerUserID int64) domain.ChannelMessageReactions {
	byUser := s.privateReactions[uid]
	out := domain.ChannelMessageReactions{CanSeeList: true}
	if len(byUser) == 0 {
		return out
	}
	counts := make(map[string]int)
	recent := make([]domain.ChannelMessagePeerReaction, 0, len(byUser))
	for userID, rows := range byUser {
		for _, row := range rows {
			key := string(row.Reaction.Type) + "\x00" + row.Reaction.Emoticon
			index, ok := counts[key]
			if !ok {
				out.Results = append(out.Results, domain.ChannelMessageReactionCount{Reaction: row.Reaction})
				index = len(out.Results) - 1
				counts[key] = index
			}
			out.Results[index].Count++
			if userID == viewerUserID && (out.Results[index].ChosenOrder == 0 || row.ChosenOrder < out.Results[index].ChosenOrder) {
				out.Results[index].ChosenOrder = row.ChosenOrder
			}
			item := row
			item.UserID = userID
			item.My = userID == viewerUserID
			recent = append(recent, item)
		}
	}
	sort.Slice(out.Results, func(i, j int) bool {
		if out.Results[i].Count != out.Results[j].Count {
			return out.Results[i].Count > out.Results[j].Count
		}
		return out.Results[i].Reaction.Emoticon < out.Results[j].Reaction.Emoticon
	})
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].Date != recent[j].Date {
			return recent[i].Date > recent[j].Date
		}
		return recent[i].UserID < recent[j].UserID
	})
	if len(recent) > domain.MaxChannelMessageReactionRecent {
		recent = recent[:domain.MaxChannelMessageReactionRecent]
	}
	out.Recent = recent
	return out
}

func (s *MessageStore) EditMessage(_ context.Context, req domain.EditMessageRequest) (domain.EditMessageResult, error) {
	res := domain.EditMessageResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.Peer.ID == 0 || req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return res, domain.ErrMessageIDInvalid
	}
	if req.EditDate == 0 {
		req.EditDate = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	targetIndex := -1
	var target domain.Message
	for i, msg := range s.m[req.OwnerUserID] {
		if msg.ID == req.ID && msg.Peer == req.Peer {
			targetIndex = i
			target = msg
			break
		}
	}
	if targetIndex < 0 {
		return res, domain.ErrMessageIDInvalid
	}
	if !target.Out || target.From.ID != req.OwnerUserID {
		return res, domain.ErrMessageAuthorRequired
	}
	if target.Body == req.Message && equalMessageEntities(target.Entities, req.Entities) {
		return res, domain.ErrMessageNotModified
	}
	for userID, messages := range s.m {
		for i, msg := range messages {
			if msg.UID == target.UID && msg.From.ID == req.OwnerUserID {
				msg.Body = req.Message
				msg.Entities = append([]domain.MessageEntity(nil), req.Entities...)
				msg.EditDate = req.EditDate
				msg.Pts = s.nextPtsLocked(userID)
				s.m[userID][i] = msg
				event := editMessageEvent(msg)
				res.Edited = append(res.Edited, domain.EditedMessageForUser{
					UserID:  userID,
					Message: cloneMessage(msg),
					Event:   event,
				})
			}
		}
	}
	if s.dialogs != nil {
		s.dialogs.mu.Lock()
		for userID := range s.dialogs.m {
			list := s.dialogs.m[userID]
			list.Messages = cloneMessages(s.m[userID])
			s.dialogs.m[userID] = list
		}
		s.dialogs.mu.Unlock()
	}
	sort.Slice(res.Edited, func(i, j int) bool { return res.Edited[i].UserID < res.Edited[j].UserID })
	return res, nil
}

func (s *MessageStore) DeleteMessages(_ context.Context, req domain.DeleteMessagesRequest) (domain.DeleteMessagesResult, error) {
	res := domain.DeleteMessagesResult{OwnerUserID: req.OwnerUserID}
	ids := normalizeMemoryMessageIDs(req.IDs)
	if req.OwnerUserID == 0 || len(ids) == 0 {
		return res, nil
	}
	if len(ids) > domain.MaxDeleteMessageIDs {
		return res, fmt.Errorf("delete messages: too many ids: %d > %d", len(ids), domain.MaxDeleteMessageIDs)
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	idSet := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted, revokeUIDs, _ := s.deleteMemoryMessagesLocked(req.OwnerUserID, 0, func(msg domain.Message) bool {
		_, ok := idSet[msg.ID]
		return ok
	})
	if req.Revoke && len(revokeUIDs) > 0 {
		deleted = append(deleted, s.deleteMemoryMessagesByUIDLocked(revokeUIDs, req.OwnerUserID)...)
	}
	return s.finishMemoryDeleteLocked(res, deleted, req.Date, false), nil
}

func (s *MessageStore) DeleteHistory(_ context.Context, req domain.DeleteHistoryRequest) (domain.DeleteMessagesResult, error) {
	res := domain.DeleteMessagesResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.Peer.ID == 0 {
		return res, nil
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted, revokeUIDs, more := s.deleteMemoryMessagesLocked(req.OwnerUserID, domain.MaxDeleteHistoryBatch, func(msg domain.Message) bool {
		return msg.Peer == req.Peer && (req.MaxID <= 0 || msg.ID <= req.MaxID)
	})
	if req.Revoke && len(revokeUIDs) > 0 {
		deleted = append(deleted, s.deleteMemoryMessagesByUIDLocked(revokeUIDs, req.OwnerUserID)...)
	}
	res = s.finishMemoryDeleteLocked(res, deleted, req.Date, req.JustClear)
	if more {
		res.Offset = 1
	}
	return res, nil
}

func (s *MessageStore) nextBoxIDLocked(userID int64) int {
	next := s.nextBox[userID] + 1
	s.nextBox[userID] = next
	return next
}

func (s *MessageStore) nextPtsLocked(userID int64) int {
	next := s.nextPts[userID] + 1
	s.nextPts[userID] = next
	return next
}

func (s *MessageStore) nextPtsNLocked(userID int64, count int) int {
	if count <= 0 {
		count = 1
	}
	next := s.nextPts[userID] + count
	s.nextPts[userID] = next
	return next
}

func (s *MessageStore) upsertMemoryDialogsLocked(sender, recipient domain.Message) {
	s.dialogs.mu.Lock()
	defer s.dialogs.mu.Unlock()
	list := s.dialogs.m[sender.OwnerUserID]
	list = upsertMemoryDialog(list, domain.Dialog{Peer: sender.Peer, TopMessage: sender.ID, TopMessageDate: sender.Date})
	list.Messages = append(list.Messages, sender)
	s.dialogs.m[sender.OwnerUserID] = list
	if recipient.OwnerUserID != sender.OwnerUserID {
		peerList := s.dialogs.m[recipient.OwnerUserID]
		peerList = upsertMemoryDialog(peerList, domain.Dialog{Peer: recipient.Peer, TopMessage: recipient.ID, TopMessageDate: recipient.Date, UnreadCount: 1})
		peerList.Messages = append(peerList.Messages, recipient)
		s.dialogs.m[recipient.OwnerUserID] = peerList
	}
}

func upsertMemoryDialog(list domain.DialogList, dialog domain.Dialog) domain.DialogList {
	for i := range list.Dialogs {
		if list.Dialogs[i].Peer == dialog.Peer {
			if dialog.ReadInboxMaxID == 0 {
				dialog.ReadInboxMaxID = list.Dialogs[i].ReadInboxMaxID
			}
			if dialog.ReadOutboxMaxID == 0 {
				dialog.ReadOutboxMaxID = list.Dialogs[i].ReadOutboxMaxID
			}
			if dialog.FolderID == domain.DialogMainFolderID && list.Dialogs[i].FolderID != domain.DialogMainFolderID {
				dialog.FolderID = list.Dialogs[i].FolderID
			}
			if dialog.UnreadCount != 0 {
				dialog.UnreadCount += list.Dialogs[i].UnreadCount
			}
			list.Dialogs[i] = dialog
			return list
		}
	}
	list.Dialogs = append(list.Dialogs, dialog)
	return list
}

type deletedMemoryMessage struct {
	userID int64
	peer   domain.Peer
	id     int
}

func (s *MessageStore) deleteMemoryMessagesLocked(userID int64, limit int, match func(domain.Message) bool) ([]deletedMemoryMessage, map[int64]struct{}, bool) {
	messages := s.m[userID]
	kept := messages[:0]
	deleted := make([]deletedMemoryMessage, 0)
	revokeUIDs := make(map[int64]struct{})
	more := false
	for _, msg := range messages {
		if match(msg) {
			if limit > 0 && len(deleted) >= limit {
				kept = append(kept, msg)
				more = true
				continue
			}
			deleted = append(deleted, deletedMemoryMessage{userID: userID, peer: msg.Peer, id: msg.ID})
			if msg.UID != 0 {
				revokeUIDs[msg.UID] = struct{}{}
			}
			continue
		}
		kept = append(kept, msg)
	}
	s.m[userID] = kept
	return deleted, revokeUIDs, more
}

func (s *MessageStore) deleteMemoryMessagesByUIDLocked(uids map[int64]struct{}, excludeUserID int64) []deletedMemoryMessage {
	if len(uids) == 0 {
		return nil
	}
	deleted := make([]deletedMemoryMessage, 0)
	for userID, messages := range s.m {
		if userID == excludeUserID {
			continue
		}
		kept := messages[:0]
		for _, msg := range messages {
			if _, ok := uids[msg.UID]; ok {
				deleted = append(deleted, deletedMemoryMessage{userID: userID, peer: msg.Peer, id: msg.ID})
				continue
			}
			kept = append(kept, msg)
		}
		s.m[userID] = kept
	}
	return deleted
}

func (s *MessageStore) finishMemoryDeleteLocked(res domain.DeleteMessagesResult, deleted []deletedMemoryMessage, date int, preserveEmptyDialogs bool) domain.DeleteMessagesResult {
	if len(deleted) == 0 {
		return res
	}
	idsByOwner := make(map[int64][]int)
	peersByOwner := make(map[int64]map[domain.Peer]struct{})
	for _, row := range deleted {
		idsByOwner[row.userID] = append(idsByOwner[row.userID], row.id)
		if peersByOwner[row.userID] == nil {
			peersByOwner[row.userID] = make(map[domain.Peer]struct{})
		}
		peersByOwner[row.userID][row.peer] = struct{}{}
	}
	if s.dialogs != nil {
		s.dialogs.mu.Lock()
		for userID, peers := range peersByOwner {
			for peer := range peers {
				s.rebuildMemoryDialogLocked(userID, peer, preserveEmptyDialogs)
			}
		}
		s.dialogs.mu.Unlock()
	}
	ownerIDs := make([]int64, 0, len(idsByOwner))
	for userID := range idsByOwner {
		ownerIDs = append(ownerIDs, userID)
	}
	sort.Slice(ownerIDs, func(i, j int) bool { return ownerIDs[i] < ownerIDs[j] })
	for _, userID := range ownerIDs {
		ids := normalizeMemoryMessageIDs(idsByOwner[userID])
		if len(ids) == 0 {
			continue
		}
		pts := s.nextPtsNLocked(userID, len(ids))
		event := domain.UpdateEvent{
			UserID:     userID,
			Type:       domain.UpdateEventDeleteMessages,
			Pts:        pts,
			PtsCount:   len(ids),
			Date:       date,
			MessageIDs: ids,
		}
		res.Deleted = append(res.Deleted, domain.DeletedMessagesForUser{
			UserID:     userID,
			MessageIDs: ids,
			Event:      event,
		})
	}
	return res
}

func (s *MessageStore) rebuildMemoryDialogLocked(userID int64, peer domain.Peer, preserveEmpty bool) {
	list := s.dialogs.m[userID]
	topID := 0
	topDate := 0
	unread := 0
	for _, msg := range s.m[userID] {
		if msg.Peer != peer {
			continue
		}
		if msg.ID > topID {
			topID = msg.ID
			topDate = msg.Date
		}
	}
	dialogs := list.Dialogs[:0]
	for _, dialog := range list.Dialogs {
		if dialog.Peer != peer {
			dialogs = append(dialogs, dialog)
			continue
		}
		if topID == 0 {
			if preserveEmpty {
				oldTop := dialog.TopMessage
				dialog.TopMessage = 0
				dialog.TopMessageDate = 0
				if dialog.ReadInboxMaxID < oldTop {
					dialog.ReadInboxMaxID = oldTop
				}
				if dialog.ReadOutboxMaxID < oldTop {
					dialog.ReadOutboxMaxID = oldTop
				}
				dialog.UnreadCount = 0
				dialog.UnreadMark = false
				dialog.UnreadMentions = 0
				dialog.UnreadReactions = 0
				dialogs = append(dialogs, dialog)
			}
			continue
		}
		for _, msg := range s.m[userID] {
			if msg.Peer == peer && !msg.Out && msg.ID > dialog.ReadInboxMaxID {
				unread++
			}
		}
		dialog.TopMessage = topID
		dialog.TopMessageDate = topDate
		dialog.UnreadCount = unread
		dialog.UnreadMentions = 0
		dialog.UnreadReactions = 0
		dialogs = append(dialogs, dialog)
	}
	list.Dialogs = dialogs
	list.Messages = cloneMessages(s.m[userID])
	s.dialogs.m[userID] = list
}

func normalizeMemoryMessageIDs(ids []int) []int {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int, 0, len(ids))
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 || id > domain.MaxMessageBoxID {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func cloneMessage(msg domain.Message) domain.Message {
	msg.Entities = append([]domain.MessageEntity(nil), msg.Entities...)
	msg.ReplyTo = cloneMessageReply(msg.ReplyTo)
	msg.Forward = cloneMessageForward(msg.Forward)
	msg.Reactions = cloneChannelMessageReactionsPtr(msg.Reactions)
	return msg
}

func cloneMessageReply(reply *domain.MessageReply) *domain.MessageReply {
	if reply == nil {
		return nil
	}
	clone := *reply
	clone.QuoteEntities = append([]domain.MessageEntity(nil), reply.QuoteEntities...)
	return &clone
}

func cloneMessageForward(forward *domain.MessageForward) *domain.MessageForward {
	if forward == nil {
		return nil
	}
	clone := *forward
	return &clone
}

func newMessageEvent(msg domain.Message) domain.UpdateEvent {
	if msg.ID == 0 {
		return domain.UpdateEvent{}
	}
	return domain.UpdateEvent{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventNewMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.Date,
		Message:  cloneMessage(msg),
	}
}

func editMessageEvent(msg domain.Message) domain.UpdateEvent {
	if msg.ID == 0 {
		return domain.UpdateEvent{}
	}
	return domain.UpdateEvent{
		UserID:   msg.OwnerUserID,
		Type:     domain.UpdateEventEditMessage,
		Pts:      msg.Pts,
		PtsCount: 1,
		Date:     msg.EditDate,
		Message:  cloneMessage(msg),
	}
}

func equalMessageEntities(a, b []domain.MessageEntity) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasUser(users []domain.User, id int64) bool {
	for _, u := range users {
		if u.ID == id {
			return true
		}
	}
	return false
}

func filterMessageList(messages []domain.Message, filter domain.MessageFilter) domain.MessageList {
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	sort.SliceStable(messages, func(i, j int) bool {
		return messageLess(messages[i], messages[j])
	})

	query := strings.ToLower(filter.Query)
	base := make([]domain.Message, 0, len(messages))
	for _, msg := range messages {
		if filter.HasPeer && msg.Peer != filter.Peer {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(msg.Body), query) {
			continue
		}
		if filter.MaxID > 0 && msg.ID >= filter.MaxID {
			continue
		}
		if filter.MinID > 0 && msg.ID <= filter.MinID {
			continue
		}
		base = append(base, msg)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	page := pageMessageHistory(base, filter, limit)
	return domain.MessageList{
		Messages: page,
		Users:    usersForMessages(page),
		Count:    len(base),
		Hash:     messageListHash(base),
	}
}

func pageMessageHistory(base []domain.Message, filter domain.MessageFilter, limit int) []domain.Message {
	if limit <= 0 || len(base) == 0 {
		return nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return cloneMessages(forwardMessageHistory(base, filter, limit))
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		page := make([]domain.Message, 0, limit)
		page = append(page, forwardMessageHistory(base, filter, forwardLimit)...)
		page = append(page, backwardMessageHistory(base, filter, backwardLimit, true)...)
		sort.SliceStable(page, func(i, j int) bool {
			return messageLess(page[i], page[j])
		})
		return cloneMessages(page)
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		candidates := backwardMessageHistory(base, filter, limit+start, false)
		if start >= len(candidates) {
			return nil
		}
		return cloneMessages(candidates[start:])
	}
}

type messageHistoryLoad int

const (
	messageHistoryLoadBackward messageHistoryLoad = iota
	messageHistoryLoadForward
	messageHistoryLoadAround
)

func messageHistoryLoadType(addOffset, limit int) messageHistoryLoad {
	if addOffset >= 0 {
		return messageHistoryLoadBackward
	}
	if addOffset+limit > 0 {
		return messageHistoryLoadAround
	}
	return messageHistoryLoadForward
}

func backwardMessageHistory(base []domain.Message, filter domain.MessageFilter, limit int, includeOffset bool) []domain.Message {
	if limit <= 0 {
		return nil
	}
	out := make([]domain.Message, 0, limit)
	for _, msg := range base {
		if !messageBeforeHistoryOffset(msg, filter, includeOffset) {
			continue
		}
		out = append(out, msg)
		if len(out) == limit {
			break
		}
	}
	return out
}

func forwardMessageHistory(base []domain.Message, filter domain.MessageFilter, limit int) []domain.Message {
	if limit <= 0 {
		return nil
	}
	out := make([]domain.Message, 0, limit)
	for i := len(base) - 1; i >= 0; i-- {
		msg := base[i]
		if !messageAfterHistoryOffset(msg, filter) {
			continue
		}
		out = append(out, msg)
		if len(out) == limit {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return messageLess(out[i], out[j])
	})
	return out
}

func messageBeforeHistoryOffset(msg domain.Message, filter domain.MessageFilter, includeOffset bool) bool {
	if filter.OffsetDate > 0 {
		if includeOffset {
			return msg.Date <= filter.OffsetDate
		}
		return msg.Date < filter.OffsetDate
	}
	if filter.OffsetID <= 0 {
		return true
	}
	if includeOffset {
		return msg.ID <= filter.OffsetID
	}
	return msg.ID < filter.OffsetID
}

func messageAfterHistoryOffset(msg domain.Message, filter domain.MessageFilter) bool {
	if filter.OffsetDate > 0 {
		return msg.Date >= filter.OffsetDate
	}
	if filter.OffsetID <= 0 {
		return false
	}
	return msg.ID > filter.OffsetID
}

func messageLess(a, b domain.Message) bool {
	if a.Date != b.Date {
		return a.Date > b.Date
	}
	return a.ID > b.ID
}

func usersForMessages(messages []domain.Message) []domain.User {
	seen := map[int64]struct{}{}
	users := make([]domain.User, 0, 1)
	for _, msg := range messages {
		for _, peer := range []domain.Peer{msg.Peer, msg.From} {
			if peer.Type != domain.PeerTypeUser {
				continue
			}
			if _, ok := seen[peer.ID]; ok {
				continue
			}
			seen[peer.ID] = struct{}{}
			if peer.ID == domain.OfficialSystemUserID {
				users = append(users, domain.OfficialSystemUser())
			}
		}
	}
	return users
}

func messageListHash(messages []domain.Message) int64 {
	if len(messages) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [16]byte
	for _, msg := range messages {
		binary.LittleEndian.PutUint32(buf[:4], uint32(msg.ID))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(msg.Date))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(msg.From.ID))
		_, _ = h.Write(buf[:])
		writeMessageReactionsHash(h, msg.Reactions)
	}
	return int64(h.Sum64())
}

func writeMessageReactionsHash(h hash.Hash64, reactions *domain.ChannelMessageReactions) {
	if reactions == nil {
		_, _ = h.Write([]byte{0})
		return
	}
	var buf [16]byte
	for _, item := range reactions.Results {
		_, _ = h.Write([]byte(item.Reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(item.Reaction.Emoticon))
		_, _ = h.Write([]byte{0})
		binary.LittleEndian.PutUint32(buf[:4], uint32(item.Count))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(item.ChosenOrder))
		_, _ = h.Write(buf[:8])
	}
	_, _ = h.Write([]byte{0xfe})
	for _, item := range reactions.Recent {
		_, _ = h.Write([]byte(item.Reaction.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(item.Reaction.Emoticon))
		_, _ = h.Write([]byte{0})
		binary.LittleEndian.PutUint64(buf[:8], uint64(item.UserID))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(item.Date))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(item.ChosenOrder))
		_, _ = h.Write(buf[:])
	}
}

func cloneContacts(contacts []domain.Contact) []domain.Contact {
	out := append([]domain.Contact(nil), contacts...)
	for i := range out {
		out[i] = cloneContact(out[i])
	}
	return out
}

func cloneContact(contact domain.Contact) domain.Contact {
	contact.NoteEntities = append([]domain.MessageEntity(nil), contact.NoteEntities...)
	return contact
}

func contactListHash(contacts []domain.Contact) int64 {
	if len(contacts) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [16]byte
	for _, contact := range contacts {
		binary.LittleEndian.PutUint64(buf[:8], uint64(contact.User.ID))
		if contact.Mutual {
			buf[8] = 1
		} else {
			buf[8] = 0
		}
		_, _ = h.Write(buf[:9])
		_, _ = h.Write([]byte(contact.FirstName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(contact.LastName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(contact.Phone))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(contact.Note))
		_, _ = h.Write([]byte{0})
	}
	return int64(h.Sum64())
}

func filterDialogList(list domain.DialogList, filter domain.DialogFilter) domain.DialogList {
	sort.SliceStable(list.Dialogs, func(i, j int) bool {
		return dialogLess(list.Dialogs[i], list.Dialogs[j])
	})

	base := make([]domain.Dialog, 0, len(list.Dialogs))
	for _, d := range list.Dialogs {
		if !dialogMatchesFolder(d, list.Users, filter) {
			continue
		}
		if filter.PinnedOnly && !d.Pinned {
			continue
		}
		if filter.ExcludePinned && d.Pinned {
			continue
		}
		base = append(base, d)
	}

	list.Count = len(base)
	list.Hash = dialogListHash(base)
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	page := make([]domain.Dialog, 0, len(base))
	for _, d := range base {
		if !afterDialogOffset(d, filter) {
			continue
		}
		page = append(page, d)
		if len(page) >= limit {
			break
		}
	}
	list.Dialogs = page
	list.Messages = keepDialogMessages(list.Messages, page)
	return list
}

func dialogMatchesFolder(d domain.Dialog, users []domain.User, filter domain.DialogFilter) bool {
	if !filter.HasFolderID {
		return true
	}
	if filter.FolderID < domain.DialogCustomFolderMinID {
		return d.FolderID == filter.FolderID
	}
	if filter.Folder == nil {
		return false
	}
	folder := filter.Folder
	if folder.ExcludeArchived && d.FolderID == domain.DialogArchiveFolderID {
		return false
	}
	if folder.ExcludeRead && d.UnreadCount == 0 && !d.UnreadMark {
		return false
	}
	if hasFolderPeer(folder.ExcludePeers, d.Peer) {
		return false
	}
	if hasFolderPeer(folder.IncludePeers, d.Peer) || hasFolderPeer(folder.PinnedPeers, d.Peer) {
		return true
	}
	if d.Peer.Type == domain.PeerTypeUser {
		user, ok := findDialogUser(users, d.Peer.ID)
		if ok && user.Contact && folder.Contacts {
			return true
		}
		if (!ok || !user.Contact) && folder.NonContacts {
			return true
		}
	}
	return false
}

func hasFolderPeer(peers []domain.DialogFolderPeer, peer domain.Peer) bool {
	for _, item := range peers {
		if item.Peer == peer {
			return true
		}
	}
	return false
}

func dialogLess(a, b domain.Dialog) bool {
	if a.Pinned != b.Pinned {
		return a.Pinned && !b.Pinned
	}
	if a.Pinned && b.Pinned && a.PinnedOrder != b.PinnedOrder {
		if a.PinnedOrder == 0 {
			return false
		}
		if b.PinnedOrder == 0 {
			return true
		}
		return a.PinnedOrder < b.PinnedOrder
	}
	if a.TopMessageDate != b.TopMessageDate {
		return a.TopMessageDate > b.TopMessageDate
	}
	if a.TopMessage != b.TopMessage {
		return a.TopMessage > b.TopMessage
	}
	return a.Peer.ID > b.Peer.ID
}

func afterDialogOffset(d domain.Dialog, filter domain.DialogFilter) bool {
	if filter.OffsetDate <= 0 && filter.OffsetID <= 0 {
		return true
	}
	if filter.OffsetDate > 0 {
		if d.TopMessageDate != filter.OffsetDate {
			return d.TopMessageDate < filter.OffsetDate
		}
		if filter.OffsetID <= 0 {
			return false
		}
		if d.TopMessage != filter.OffsetID {
			return d.TopMessage < filter.OffsetID
		}
		if filter.HasOffsetPeer {
			return d.Peer.ID < filter.OffsetPeer.ID
		}
		return false
	}
	return d.TopMessage < filter.OffsetID
}

func keepDialogMessages(messages []domain.Message, dialogs []domain.Dialog) []domain.Message {
	want := make(map[int]struct{}, len(dialogs))
	for _, d := range dialogs {
		if d.TopMessage != 0 {
			want[d.TopMessage] = struct{}{}
		}
	}
	out := make([]domain.Message, 0, len(want))
	for _, msg := range messages {
		if _, ok := want[msg.ID]; ok {
			out = append(out, msg)
		}
	}
	return out
}

func findDialogUser(users []domain.User, id int64) (domain.User, bool) {
	for _, user := range users {
		if user.ID == id {
			return user, true
		}
	}
	return domain.User{}, false
}

func appendDialogUser(list *domain.DialogList, seen map[int64]struct{}, user domain.User) {
	if user.ID == 0 {
		return
	}
	if _, ok := seen[user.ID]; ok {
		return
	}
	seen[user.ID] = struct{}{}
	list.Users = append(list.Users, user)
}

func cloneMessages(messages []domain.Message) []domain.Message {
	out := append([]domain.Message(nil), messages...)
	for i := range out {
		out[i] = cloneMessage(out[i])
	}
	return out
}

func cloneDialogs(dialogs []domain.Dialog) []domain.Dialog {
	out := append([]domain.Dialog(nil), dialogs...)
	for i := range out {
		if out[i].Draft != nil {
			draft := cloneDialogDraft(*out[i].Draft)
			out[i].Draft = &draft
		}
	}
	return out
}

func cloneDialogDraft(draft domain.DialogDraft) domain.DialogDraft {
	draft.Entities = append([]domain.MessageEntity(nil), draft.Entities...)
	draft.ReplyTo = cloneMessageReply(draft.ReplyTo)
	if draft.WebPage != nil {
		webpage := *draft.WebPage
		draft.WebPage = &webpage
	}
	return draft
}

func draftKey(peer domain.Peer, topMessageID int) dialogDraftKey {
	return dialogDraftKey{peerType: peer.Type, peerID: peer.ID, topMessageID: topMessageID}
}

func sortDialogDrafts(drafts []domain.DialogDraft) {
	sort.SliceStable(drafts, func(i, j int) bool {
		if drafts[i].Date != drafts[j].Date {
			return drafts[i].Date > drafts[j].Date
		}
		if drafts[i].Peer.Type != drafts[j].Peer.Type {
			return drafts[i].Peer.Type < drafts[j].Peer.Type
		}
		if drafts[i].Peer.ID != drafts[j].Peer.ID {
			return drafts[i].Peer.ID > drafts[j].Peer.ID
		}
		return drafts[i].TopMessageID > drafts[j].TopMessageID
	})
}

func cloneDialogFolder(folder domain.DialogFolder) domain.DialogFolder {
	folder.TitleEntities = append([]domain.MessageEntity(nil), folder.TitleEntities...)
	folder.PinnedPeers = append([]domain.DialogFolderPeer(nil), folder.PinnedPeers...)
	folder.IncludePeers = append([]domain.DialogFolderPeer(nil), folder.IncludePeers...)
	folder.ExcludePeers = append([]domain.DialogFolderPeer(nil), folder.ExcludePeers...)
	return folder
}

func containsInt(items []int, value int) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func removeInt(items []int, value int) []int {
	out := items[:0]
	for _, item := range items {
		if item != value {
			out = append(out, item)
		}
	}
	return out
}

func dialogListHash(dialogs []domain.Dialog) int64 {
	if len(dialogs) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [47]byte
	for _, d := range dialogs {
		binary.LittleEndian.PutUint64(buf[:8], uint64(d.Peer.ID))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(d.FolderID))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(d.TopMessage))
		binary.LittleEndian.PutUint32(buf[16:20], uint32(d.TopMessageDate))
		binary.LittleEndian.PutUint32(buf[20:24], uint32(d.ReadInboxMaxID))
		binary.LittleEndian.PutUint32(buf[24:28], uint32(d.ReadOutboxMaxID))
		binary.LittleEndian.PutUint32(buf[28:32], uint32(d.UnreadCount))
		binary.LittleEndian.PutUint32(buf[32:36], uint32(d.UnreadMentions))
		binary.LittleEndian.PutUint32(buf[36:40], uint32(d.UnreadReactions))
		if d.Pinned {
			buf[40] = 1
		} else {
			buf[40] = 0
		}
		binary.LittleEndian.PutUint32(buf[41:45], uint32(d.PinnedOrder))
		if d.UnreadMark {
			buf[45] = 1
		} else {
			buf[45] = 0
		}
		if d.PeerSettingsBarHidden {
			buf[46] = 1
		} else {
			buf[46] = 0
		}
		_, _ = h.Write(buf[:])
	}
	return int64(h.Sum64())
}

// LangPackStore 是 store.LangPackStore 的内存实现。
type LangPackStore struct {
	mu sync.RWMutex
	m  map[string]domain.LangPack
}

// NewLangPackStore 创建内存 LangPackStore。
func NewLangPackStore() *LangPackStore {
	return &LangPackStore{m: make(map[string]domain.LangPack)}
}

func (s *LangPackStore) GetPack(_ context.Context, langPack, langCode string, fromVersion int) (domain.LangPack, error) {
	s.mu.RLock()
	pack := s.m[langPackKey(langPack, langCode)]
	s.mu.RUnlock()
	if pack.LangPack == "" {
		return domain.LangPack{LangPack: langPack, LangCode: langCode, FromVersion: fromVersion}, nil
	}
	pack.FromVersion = fromVersion
	if pack.Version <= fromVersion {
		pack.Strings = nil
	} else {
		pack.Strings = append([]domain.LangPackString(nil), pack.Strings...)
	}
	return pack, nil
}

func (s *LangPackStore) GetStrings(_ context.Context, langPack, langCode string, keys []string) (domain.LangPack, error) {
	s.mu.RLock()
	pack := s.m[langPackKey(langPack, langCode)]
	s.mu.RUnlock()
	if pack.LangPack == "" {
		return domain.LangPack{LangPack: langPack, LangCode: langCode}, nil
	}
	if len(keys) == 0 {
		pack.Strings = append([]domain.LangPackString(nil), pack.Strings...)
		return pack, nil
	}
	want := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		want[key] = struct{}{}
	}
	out := domain.LangPack{LangPack: pack.LangPack, LangCode: pack.LangCode, Version: pack.Version}
	for _, item := range pack.Strings {
		if _, ok := want[item.Key]; ok {
			out.Strings = append(out.Strings, item)
		}
	}
	return out, nil
}

func (s *LangPackStore) UpsertPack(_ context.Context, pack domain.LangPack) error {
	pack.Strings = append([]domain.LangPackString(nil), pack.Strings...)
	s.mu.Lock()
	s.m[langPackKey(pack.LangPack, pack.LangCode)] = pack
	s.mu.Unlock()
	return nil
}

func langPackKey(langPack, langCode string) string {
	return langPack + "\x00" + langCode
}

// PasswordStore 是 store.PasswordStore 的内存实现。
type PasswordStore struct {
	mu        sync.RWMutex
	m         map[int64]domain.PasswordSettings
	reactions map[int64]domain.AccountReactionSettings
}

// NewPasswordStore 创建内存 PasswordStore。
func NewPasswordStore() *PasswordStore {
	return &PasswordStore{
		m:         make(map[int64]domain.PasswordSettings),
		reactions: make(map[int64]domain.AccountReactionSettings),
	}
}

func (s *PasswordStore) GetByUser(_ context.Context, userID int64) (domain.PasswordSettings, bool, error) {
	s.mu.RLock()
	settings, ok := s.m[userID]
	s.mu.RUnlock()
	settings.SecureRandom = append([]byte(nil), settings.SecureRandom...)
	return settings, ok, nil
}

func (s *PasswordStore) Save(_ context.Context, userID int64, settings domain.PasswordSettings) error {
	settings.SecureRandom = append([]byte(nil), settings.SecureRandom...)
	s.mu.Lock()
	s.m[userID] = settings
	s.mu.Unlock()
	return nil
}

func (s *PasswordStore) GetReactionSettings(_ context.Context, userID int64) (domain.AccountReactionSettings, bool, error) {
	s.mu.RLock()
	settings, ok := s.reactions[userID]
	s.mu.RUnlock()
	return cloneAccountReactionSettings(settings), ok, nil
}

func (s *PasswordStore) SaveReactionSettings(_ context.Context, userID int64, settings domain.AccountReactionSettings) error {
	s.mu.Lock()
	s.reactions[userID] = cloneAccountReactionSettings(settings)
	s.mu.Unlock()
	return nil
}

func cloneAccountReactionSettings(in domain.AccountReactionSettings) domain.AccountReactionSettings {
	out := in
	if in.PaidPrivacy.Peer != nil {
		peer := *in.PaidPrivacy.Peer
		out.PaidPrivacy.Peer = &peer
	}
	return out
}

// HelpStore 是 store.AppConfigStore 和 store.CountryStore 的内存实现。
type HelpStore struct {
	mu        sync.RWMutex
	appConfig map[string]domain.AppConfig
	countries domain.CountriesList
}

// NewHelpStore 创建内存 HelpStore。
func NewHelpStore() *HelpStore {
	return &HelpStore{appConfig: make(map[string]domain.AppConfig)}
}

func (s *HelpStore) GetAppConfig(_ context.Context, client string) (domain.AppConfig, bool, error) {
	s.mu.RLock()
	cfg, ok := s.appConfig[client]
	s.mu.RUnlock()
	cfg.JSON = append([]byte(nil), cfg.JSON...)
	return cfg, ok, nil
}

func (s *HelpStore) UpsertAppConfig(_ context.Context, cfg domain.AppConfig) error {
	cfg.JSON = append([]byte(nil), cfg.JSON...)
	s.mu.Lock()
	s.appConfig[cfg.Client] = cfg
	s.mu.Unlock()
	return nil
}

func (s *HelpStore) ListCountries(_ context.Context, _ string) (domain.CountriesList, error) {
	s.mu.RLock()
	list := s.countries
	s.mu.RUnlock()
	list.Countries = append([]domain.Country(nil), list.Countries...)
	for i := range list.Countries {
		list.Countries[i].CountryCodes = append([]domain.CountryCode(nil), list.Countries[i].CountryCodes...)
	}
	return list, nil
}

func (s *HelpStore) UpsertCountries(_ context.Context, countries []domain.Country) error {
	list := domain.CountriesList{Hash: 1, Countries: append([]domain.Country(nil), countries...)}
	for i := range list.Countries {
		list.Countries[i].CountryCodes = append([]domain.CountryCode(nil), list.Countries[i].CountryCodes...)
	}
	s.mu.Lock()
	s.countries = list
	s.mu.Unlock()
	return nil
}

// UserStore 是 store.UserStore 的内存实现。ID 与 PG identity 使用同一业务起点。
type UserStore struct {
	mu     sync.RWMutex
	byID   map[int64]domain.User
	nextID int64
}

// NewUserStore 创建内存 UserStore。
func NewUserStore() *UserStore {
	return &UserStore{byID: make(map[int64]domain.User), nextID: domain.UserIDSequenceBase}
}

func (s *UserStore) ByID(_ context.Context, id int64) (domain.User, bool, error) {
	s.mu.RLock()
	u, ok := s.byID[id]
	s.mu.RUnlock()
	return u, ok, nil
}

func (s *UserStore) ByIDs(_ context.Context, ids []int64) ([]domain.User, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.User, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if u, ok := s.byID[id]; ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func (s *UserStore) ByPhone(_ context.Context, phone string) (domain.User, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.byID {
		if u.Phone == phone {
			return u, true, nil
		}
	}
	return domain.User{}, false, nil
}

func (s *UserStore) ByPhones(_ context.Context, phones []string) ([]domain.User, error) {
	if len(phones) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	want := make(map[string]struct{}, len(phones))
	for _, phone := range phones {
		if phone != "" {
			want[phone] = struct{}{}
		}
	}
	out := make([]domain.User, 0, len(want))
	seenIDs := map[int64]struct{}{}
	for _, u := range s.byID {
		if _, ok := want[u.Phone]; !ok {
			continue
		}
		if _, ok := seenIDs[u.ID]; ok {
			continue
		}
		seenIDs[u.ID] = struct{}{}
		out = append(out, u)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *UserStore) ByUsername(_ context.Context, username string) (domain.User, bool, error) {
	username = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(username, "@")))
	if username == "" {
		return domain.User{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.byID {
		if strings.ToLower(u.Username) == username {
			return u, true, nil
		}
	}
	return domain.User{}, false, nil
}

func (s *UserStore) Search(_ context.Context, currentUserID int64, query, phoneQuery string, limit int) (domain.UserSearchResult, error) {
	if limit <= 0 {
		limit = 50
	}
	query = strings.ToLower(strings.TrimSpace(query))
	phoneQuery = strings.TrimSpace(phoneQuery)
	if query == "" {
		return domain.UserSearchResult{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := make([]domain.User, 0)
	for _, u := range s.byID {
		if u.ID == currentUserID {
			continue
		}
		if userMatchesSearch(u, query, phoneQuery) {
			users = append(users, u)
		}
	}
	sort.SliceStable(users, func(i, j int) bool {
		return users[i].ID < users[j].ID
	})
	if len(users) > limit {
		users = users[:limit]
	}
	return domain.UserSearchResult{Results: users}, nil
}

func (s *UserStore) UpdateUsername(_ context.Context, userID int64, username string) (domain.User, error) {
	username = strings.TrimSpace(strings.TrimPrefix(username, "@"))
	usernameLower := strings.ToLower(username)
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[userID]
	if !ok {
		return domain.User{}, domain.ErrUsernameNotOccupied
	}
	if usernameLower != "" {
		for id, existing := range s.byID {
			if id != userID && strings.ToLower(existing.Username) == usernameLower {
				return domain.User{}, domain.ErrUsernameOccupied
			}
		}
	}
	u.Username = username
	s.byID[userID] = u
	return u, nil
}

func (s *UserStore) UpdateProfile(_ context.Context, userID int64, firstName, lastName, about string) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[userID]
	if !ok {
		return domain.User{}, domain.ErrUsernameNotOccupied
	}
	u.FirstName = firstName
	u.LastName = lastName
	u.About = about
	s.byID[userID] = u
	return u, nil
}

func (s *UserStore) UpdateLastSeen(_ context.Context, userID int64, lastSeenAt int) error {
	if lastSeenAt <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[userID]
	if !ok {
		return domain.ErrUsernameNotOccupied
	}
	if lastSeenAt > u.LastSeenAt {
		u.LastSeenAt = lastSeenAt
		s.byID[userID] = u
	}
	return nil
}

func userMatchesSearch(u domain.User, query, phoneQuery string) bool {
	if phoneQuery != "" && strings.HasPrefix(u.Phone, phoneQuery) {
		return true
	}
	first := strings.ToLower(u.FirstName)
	last := strings.ToLower(u.LastName)
	username := strings.ToLower(u.Username)
	fullName := strings.TrimSpace(first + " " + last)
	return strings.Contains(first, query) ||
		strings.Contains(last, query) ||
		strings.Contains(fullName, query) ||
		strings.Contains(username, query)
}

func (s *UserStore) Create(_ context.Context, u domain.User) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	username := strings.ToLower(strings.TrimSpace(u.Username))
	if username != "" {
		for _, existing := range s.byID {
			if strings.ToLower(existing.Username) == username {
				return domain.User{}, domain.ErrUsernameOccupied
			}
		}
	}
	u.ID = s.nextID
	s.nextID++
	s.byID[u.ID] = u
	return u, nil
}

// AuthorizationStore 是 store.AuthorizationStore 的内存实现。
type AuthorizationStore struct {
	mu sync.RWMutex
	m  map[[8]byte]domain.Authorization
}

// NewAuthorizationStore 创建内存 AuthorizationStore。
func NewAuthorizationStore() *AuthorizationStore {
	return &AuthorizationStore{m: make(map[[8]byte]domain.Authorization)}
}

func (s *AuthorizationStore) Bind(_ context.Context, a domain.Authorization) error {
	s.mu.Lock()
	s.m[a.AuthKeyID] = a
	s.mu.Unlock()
	return nil
}

func (s *AuthorizationStore) ByAuthKey(_ context.Context, id [8]byte) (domain.Authorization, bool, error) {
	s.mu.RLock()
	a, ok := s.m[id]
	s.mu.RUnlock()
	return a, ok, nil
}

func (s *AuthorizationStore) ListByUser(_ context.Context, userID int64) ([]domain.Authorization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Authorization, 0)
	for _, a := range s.m {
		if a.UserID == userID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *AuthorizationStore) Delete(_ context.Context, id [8]byte) error {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
	return nil
}

// CodeStore 是 store.CodeStore 的内存实现（带 TTL）。
type CodeStore struct {
	mu sync.Mutex
	m  map[string]codeEntry
}

type codeEntry struct {
	code    store.PhoneCode
	expires time.Time
}

// NewCodeStore 创建内存 CodeStore。
func NewCodeStore() *CodeStore {
	return &CodeStore{m: make(map[string]codeEntry)}
}

func (s *CodeStore) Set(_ context.Context, hash string, code store.PhoneCode, ttl time.Duration) error {
	s.mu.Lock()
	s.m[hash] = codeEntry{code: code, expires: time.Now().Add(ttl)}
	s.mu.Unlock()
	return nil
}

func (s *CodeStore) Get(_ context.Context, hash string) (store.PhoneCode, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[hash]
	if !ok || time.Now().After(e.expires) {
		return store.PhoneCode{}, false, nil
	}
	return e.code, true, nil
}

func (s *CodeStore) Del(_ context.Context, hash string) error {
	s.mu.Lock()
	delete(s.m, hash)
	s.mu.Unlock()
	return nil
}
