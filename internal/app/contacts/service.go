package contacts

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

var (
	ErrContactIDInvalid = errors.New("contact id invalid")
	ErrContactNameEmpty = errors.New("contact name empty")
)

const maxSearchLimit = 50

// Service 提供通讯录查询。
type Service struct {
	contacts store.ContactStore
	users    store.UserStore
}

// NewService 创建 contacts 服务。
func NewService(contacts store.ContactStore, users ...store.UserStore) *Service {
	s := &Service{contacts: contacts}
	if len(users) > 0 {
		s.users = users[0]
	}
	return s
}

// GetContacts 返回当前登录账号的通讯录。未登录或无持久化实现时按空账号处理。
func (s *Service) GetContacts(ctx context.Context, userID int64, hash int64) (domain.ContactList, bool, error) {
	if s == nil || s.contacts == nil || userID == 0 {
		return domain.ContactList{}, false, nil
	}
	list, err := s.contacts.ListByUser(ctx, userID)
	if err != nil {
		return domain.ContactList{}, false, err
	}
	if s.users != nil && len(list.Contacts) > 0 {
		if err := s.attachCurrentLastSeen(ctx, &list); err != nil {
			return domain.ContactList{}, false, err
		}
	}
	if hash != 0 && hash == list.Hash {
		return list, true, nil
	}
	return list, false, nil
}

func (s *Service) attachCurrentLastSeen(ctx context.Context, list *domain.ContactList) error {
	ids := make([]int64, 0, len(list.Contacts))
	seen := make(map[int64]struct{}, len(list.Contacts))
	for _, contact := range list.Contacts {
		id := contact.User.ID
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	users, err := s.users.ByIDs(ctx, ids)
	if err != nil {
		return err
	}
	current := make(map[int64]domain.User, len(users))
	for _, u := range users {
		current[u.ID] = u
	}
	for i := range list.Contacts {
		if u, ok := current[list.Contacts[i].User.ID]; ok {
			list.Contacts[i].User.LastSeenAt = u.LastSeenAt
			list.Contacts[i].User.Status = u.Status
		}
	}
	return nil
}

func (s *Service) AddContact(ctx context.Context, userID int64, input domain.ContactInput) (domain.Contact, error) {
	if s == nil || s.contacts == nil || userID == 0 || input.ContactUserID == 0 || input.ContactUserID == userID {
		return domain.Contact{}, ErrContactIDInvalid
	}
	if input.FirstName == "" && input.LastName == "" {
		return domain.Contact{}, ErrContactNameEmpty
	}
	if s.users != nil {
		target, found, err := s.users.ByID(ctx, input.ContactUserID)
		if err != nil {
			return domain.Contact{}, err
		}
		if !found {
			return domain.Contact{}, ErrContactIDInvalid
		}
		if input.Phone == "" {
			input.Phone = target.Phone
		}
	}
	contact, err := s.contacts.Upsert(ctx, userID, input)
	if err != nil {
		return domain.Contact{}, err
	}
	return contact, nil
}

func (s *Service) ImportContacts(ctx context.Context, userID int64, inputs []domain.ContactInput) (domain.ImportContactsResult, error) {
	if s == nil || s.contacts == nil || s.users == nil || userID == 0 || len(inputs) == 0 {
		return domain.ImportContactsResult{}, nil
	}
	out := domain.ImportContactsResult{
		Imported: make([]domain.ImportedContact, 0, len(inputs)),
		Contacts: make([]domain.Contact, 0, len(inputs)),
	}
	normalized := make([]domain.ContactInput, 0, len(inputs))
	phones := make([]string, 0, len(inputs))
	seenPhones := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		phone := normalizePhone(input.Phone)
		if phone == "" {
			continue
		}
		input.Phone = phone
		normalized = append(normalized, input)
		if _, ok := seenPhones[phone]; ok {
			continue
		}
		seenPhones[phone] = struct{}{}
		phones = append(phones, phone)
	}
	if len(phones) == 0 {
		return out, nil
	}
	targets, err := s.users.ByPhones(ctx, phones)
	if err != nil {
		return domain.ImportContactsResult{}, err
	}
	byPhone := make(map[string]domain.User, len(targets))
	for _, target := range targets {
		if target.Phone != "" {
			byPhone[target.Phone] = target
		}
	}
	upsertsByTarget := make(map[int64]domain.ContactInput, len(targets))
	order := make([]int64, 0, len(targets))
	seenTargets := map[int64]struct{}{}
	for _, input := range normalized {
		target, found := byPhone[input.Phone]
		if !found || target.ID == userID || target.ID == 0 {
			continue
		}
		input.ContactUserID = target.ID
		if input.FirstName == "" && input.LastName == "" {
			input.FirstName = target.FirstName
			input.LastName = target.LastName
		}
		if _, ok := seenTargets[target.ID]; !ok {
			seenTargets[target.ID] = struct{}{}
			order = append(order, target.ID)
		}
		out.Imported = append(out.Imported, domain.ImportedContact{UserID: target.ID, ClientID: input.ClientID})
		upsertsByTarget[target.ID] = input
	}
	if len(order) == 0 {
		return out, nil
	}
	upserts := make([]domain.ContactInput, 0, len(order))
	for _, targetID := range order {
		upserts = append(upserts, upsertsByTarget[targetID])
	}
	contacts, err := s.contacts.UpsertMany(ctx, userID, upserts)
	if err != nil {
		return domain.ImportContactsResult{}, err
	}
	out.Contacts = append(out.Contacts, contacts...)
	return out, nil
}

func (s *Service) Search(ctx context.Context, userID int64, query string, limit int) (domain.UserSearchResult, error) {
	if s == nil || s.users == nil || userID == 0 {
		return domain.UserSearchResult{}, nil
	}
	query = strings.TrimSpace(query)
	query = strings.TrimPrefix(query, "@")
	query = strings.TrimSpace(query)
	if query == "" {
		return domain.UserSearchResult{}, nil
	}
	if limit <= 0 || limit > maxSearchLimit {
		limit = maxSearchLimit
	}
	return s.users.Search(ctx, userID, query, normalizePhone(query), limit)
}

func (s *Service) DeleteContacts(ctx context.Context, userID int64, contactUserIDs []int64) (int, error) {
	if s == nil || s.contacts == nil || userID == 0 {
		return 0, nil
	}
	return s.contacts.Delete(ctx, userID, contactUserIDs)
}

func (s *Service) UpdateContactNote(ctx context.Context, userID, contactUserID int64, note string, entities []domain.MessageEntity) (domain.Contact, error) {
	if s == nil || s.contacts == nil || userID == 0 || contactUserID == 0 || contactUserID == userID {
		return domain.Contact{}, ErrContactIDInvalid
	}
	contact, found, err := s.contacts.UpdateNote(ctx, userID, contactUserID, note, entities)
	if err != nil {
		return domain.Contact{}, err
	}
	if !found {
		return domain.Contact{}, ErrContactIDInvalid
	}
	return contact, nil
}

func (s *Service) GetPeerSettings(ctx context.Context, userID int64, peer domain.Peer) (domain.PeerSettings, error) {
	if s == nil || s.contacts == nil || userID == 0 || peer.Type != domain.PeerTypeUser || peer.ID == 0 || peer.ID == userID {
		return domain.PeerSettings{}, nil
	}
	_, found, err := s.contacts.Get(ctx, userID, peer.ID)
	if err != nil {
		return domain.PeerSettings{}, err
	}
	return domain.PeerSettings{
		AddContact:   !found,
		BlockContact: !found,
		ShareContact: found,
	}, nil
}

func (s *Service) ContactIDs(ctx context.Context, userID int64, hash int64) ([]int, bool, error) {
	list, notModified, err := s.GetContacts(ctx, userID, hash)
	if err != nil || notModified {
		return nil, notModified, err
	}
	ids := make([]int, 0, len(list.Contacts))
	for _, contact := range list.Contacts {
		ids = append(ids, int(contact.User.ID))
	}
	return ids, false, nil
}

func normalizePhone(phone string) string {
	if !utf8.ValidString(phone) {
		return ""
	}
	var b strings.Builder
	b.Grow(len(phone))
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return phone
	}
	return b.String()
}
