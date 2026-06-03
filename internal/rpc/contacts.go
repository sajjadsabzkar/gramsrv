package rpc

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/gotd/td/tg"

	"telesrv/internal/app/contacts"
	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

const (
	maxContactImportBatch = 500
	maxContactDeleteBatch = 500
	maxContactNameLength  = 128
	maxContactPhoneLength = 64
	maxContactNoteLength  = 4096
	maxContactSearchQLen  = 256
	maxContactSearchLimit = 50
)

// registerContacts 注册 contacts.* RPC handler。
func (r *Router) registerContacts(d *tg.ServerDispatcher) {
	d.OnContactsGetContacts(r.onContactsGetContacts)
	d.OnContactsGetContactIDs(r.onContactsGetContactIDs)
	d.OnContactsGetStatuses(r.onContactsGetStatuses)
	d.OnContactsImportContacts(r.onContactsImportContacts)
	d.OnContactsAddContact(r.onContactsAddContact)
	d.OnContactsDeleteContacts(r.onContactsDeleteContacts)
	d.OnContactsUpdateContactNote(r.onContactsUpdateContactNote)
	d.OnContactsSearch(r.onContactsSearch)
	d.OnContactsResolveUsername(r.onContactsResolveUsername)
	d.OnContactsResolvePhone(r.onContactsResolvePhone)
	d.OnContactsGetTopPeers(func(ctx context.Context, req *tg.ContactsGetTopPeersRequest) (tg.ContactsTopPeersClass, error) {
		return tdesktop.TopPeers(), nil
	})
	d.OnContactsGetBlocked(func(ctx context.Context, req *tg.ContactsGetBlockedRequest) (tg.ContactsBlockedClass, error) {
		if req.Limit > 50 {
			return nil, limitInvalidErr()
		}
		return tdesktop.BlockedContacts(), nil
	})
	d.OnContactsGetSponsoredPeers(func(ctx context.Context, q string) (tg.ContactsSponsoredPeersClass, error) {
		if utf8.RuneCountInString(q) > maxContactSearchQLen {
			return nil, limitInvalidErr()
		}
		return &tg.ContactsSponsoredPeersEmpty{}, nil
	})
}

func (r *Router) onContactsGetContacts(ctx context.Context, hash int64) (tg.ContactsContactsClass, error) {
	if r.deps.Contacts == nil {
		return &tg.ContactsContacts{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	list, notModified, err := r.deps.Contacts.GetContacts(ctx, userID, hash)
	if err != nil {
		return nil, internalErr()
	}
	if notModified {
		return &tg.ContactsContactsNotModified{}, nil
	}
	return tgContacts(r.withContactListPresence(list)), nil
}

func (r *Router) onContactsGetStatuses(ctx context.Context) ([]tg.ContactStatus, error) {
	if r.deps.Contacts == nil {
		return []tg.ContactStatus{}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	list, _, err := r.deps.Contacts.GetContacts(ctx, userID, 0)
	if err != nil {
		return nil, internalErr()
	}
	contactUserIDs := make([]int64, 0, len(list.Contacts))
	out := make([]tg.ContactStatus, 0, len(list.Contacts))
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
		contactUserIDs = append(contactUserIDs, id)
	}
	usersByID := make(map[int64]domain.User, len(contactUserIDs))
	if len(contactUserIDs) > 0 && r.deps.Users != nil {
		users, err := r.deps.Users.ByIDs(ctx, userID, contactUserIDs)
		if err != nil {
			return nil, internalErr()
		}
		for _, u := range users {
			if u.ID != 0 {
				usersByID[u.ID] = u
			}
		}
	}
	seen = make(map[int64]struct{}, len(list.Contacts))
	for _, contact := range list.Contacts {
		id := contact.User.ID
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		u := contact.User
		if current, ok := usersByID[id]; ok {
			u.LastSeenAt = current.LastSeenAt
			u.Status = current.Status
		}
		out = append(out, tg.ContactStatus{
			UserID: id,
			Status: tgUserStatus(r.userPresenceStatusForUser(u)),
		})
	}
	return out, nil
}

func (r *Router) onContactsGetContactIDs(ctx context.Context, hash int64) ([]int, error) {
	if r.deps.Contacts == nil {
		return nil, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	ids, notModified, err := r.deps.Contacts.ContactIDs(ctx, userID, hash)
	if err != nil {
		return nil, internalErr()
	}
	if notModified {
		return nil, nil
	}
	return ids, nil
}

func (r *Router) onContactsImportContacts(ctx context.Context, input []tg.InputPhoneContact) (*tg.ContactsImportedContacts, error) {
	if r.deps.Contacts == nil {
		return &tg.ContactsImportedContacts{}, nil
	}
	if len(input) > maxContactImportBatch {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	items := make([]domain.ContactInput, 0, len(input))
	for _, item := range input {
		note, entities := contactNote(item.GetNote())
		if !validContactInput(item.Phone, item.FirstName, item.LastName, note, len(entities)) {
			return nil, limitInvalidErr()
		}
		items = append(items, domain.ContactInput{
			ClientID:     item.ClientID,
			Phone:        item.Phone,
			FirstName:    item.FirstName,
			LastName:     item.LastName,
			Note:         note,
			NoteEntities: entities,
		})
	}
	res, err := r.deps.Contacts.ImportContacts(ctx, userID, items)
	if err != nil {
		return nil, internalErr()
	}
	out := &tg.ContactsImportedContacts{
		Imported: make([]tg.ImportedContact, 0, len(res.Imported)),
		Users:    make([]tg.UserClass, 0, len(res.Contacts)),
	}
	for _, imported := range res.Imported {
		out.Imported = append(out.Imported, tg.ImportedContact{UserID: imported.UserID, ClientID: imported.ClientID})
	}
	for _, contact := range res.Contacts {
		out.Users = append(out.Users, r.tgUser(contact.User))
	}
	out.RetryContacts = append(out.RetryContacts, res.RetryContacts...)
	for _, contact := range res.Contacts {
		if err := r.recordPeerSettings(ctx, userID, domain.Peer{Type: domain.PeerTypeUser, ID: contact.User.ID}, domain.PeerSettings{ShareContact: true}); err != nil {
			return nil, internalErr()
		}
	}
	if err := r.recordContactsReset(ctx, userID); err != nil {
		return nil, internalErr()
	}
	r.pushContactsReset(ctx, userID)
	return out, nil
}

func (r *Router) onContactsAddContact(ctx context.Context, req *tg.ContactsAddContactRequest) (tg.UpdatesClass, error) {
	if r.deps.Contacts == nil {
		return &tg.Updates{Date: int(r.clock.Now().Unix())}, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	target, found, err := r.userFromInput(ctx, userID, req.ID)
	if err != nil {
		return nil, contactErr(err)
	}
	if !found {
		return nil, contactIDInvalidErr()
	}
	note, entities := contactNote(req.GetNote())
	if !validContactInput(req.Phone, req.FirstName, req.LastName, note, len(entities)) {
		return nil, limitInvalidErr()
	}
	contact, err := r.deps.Contacts.AddContact(ctx, userID, domain.ContactInput{
		ContactUserID:            target.ID,
		Phone:                    req.Phone,
		FirstName:                req.FirstName,
		LastName:                 req.LastName,
		Note:                     note,
		NoteEntities:             entities,
		AddPhonePrivacyException: req.AddPhonePrivacyException,
	})
	if err != nil {
		return nil, contactErr(err)
	}
	updates := r.contactPeerSettingsUpdates(ctx, userID, contact.User, domain.PeerSettings{ShareContact: true}, true)
	updates.Updates = append(updates.Updates, &tg.UpdateContactsReset{})
	if err := r.recordPeerSettings(ctx, userID, domain.Peer{Type: domain.PeerTypeUser, ID: contact.User.ID}, domain.PeerSettings{ShareContact: true}); err != nil {
		return nil, internalErr()
	}
	if err := r.recordContactsReset(ctx, userID); err != nil {
		return nil, internalErr()
	}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, updates)
	return updates, nil
}

func (r *Router) onContactsDeleteContacts(ctx context.Context, ids []tg.InputUserClass) (tg.UpdatesClass, error) {
	if r.deps.Contacts == nil {
		return &tg.Updates{Date: int(r.clock.Now().Unix())}, nil
	}
	if len(ids) > maxContactDeleteBatch {
		return nil, limitInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	contactIDs := make([]int64, 0, len(ids))
	users := make([]tg.UserClass, 0, len(ids)+1)
	if r.deps.Users != nil {
		if u, err := r.deps.Users.Self(ctx, userID); err == nil {
			users = append(users, r.tgSelfUser(u))
		}
	}
	seen := map[int64]struct{}{userID: {}}
	for _, id := range ids {
		u, found, err := r.userFromInput(ctx, userID, id)
		if err != nil {
			return nil, contactErr(err)
		}
		if !found || u.ID == userID {
			continue
		}
		contactIDs = append(contactIDs, u.ID)
		u.Contact = false
		u.Mutual = false
		if _, ok := seen[u.ID]; !ok {
			users = append(users, r.tgUser(u))
			seen[u.ID] = struct{}{}
		}
	}
	if _, err := r.deps.Contacts.DeleteContacts(ctx, userID, contactIDs); err != nil {
		return nil, internalErr()
	}
	updates := make([]tg.UpdateClass, 0, len(contactIDs))
	for _, id := range contactIDs {
		updates = append(updates, &tg.UpdatePeerSettings{
			Peer:     &tg.PeerUser{UserID: id},
			Settings: tgPeerSettings(domain.PeerSettings{AddContact: true, BlockContact: true}),
		})
	}
	if len(contactIDs) > 0 {
		for _, id := range contactIDs {
			if err := r.recordPeerSettings(ctx, userID, domain.Peer{Type: domain.PeerTypeUser, ID: id}, domain.PeerSettings{AddContact: true, BlockContact: true}); err != nil {
				return nil, internalErr()
			}
		}
		updates = append(updates, &tg.UpdateContactsReset{})
		if err := r.recordContactsReset(ctx, userID); err != nil {
			return nil, internalErr()
		}
	}
	out := &tg.Updates{Updates: updates, Users: users, Date: int(r.clock.Now().Unix())}
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, out)
	return out, nil
}

func (r *Router) onContactsUpdateContactNote(ctx context.Context, req *tg.ContactsUpdateContactNoteRequest) (bool, error) {
	if r.deps.Contacts == nil {
		return true, nil
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	target, found, err := r.userFromInput(ctx, userID, req.ID)
	if err != nil {
		return false, contactErr(err)
	}
	if !found {
		return false, contactIDInvalidErr()
	}
	if utf8.RuneCountInString(req.Note.Text) > maxContactNoteLength || len(req.Note.Entities) > maxMessageEntityCount {
		return false, limitInvalidErr()
	}
	if _, err := r.deps.Contacts.UpdateContactNote(ctx, userID, target.ID, req.Note.Text, domainMessageEntities(req.Note.Entities)); err != nil {
		return false, contactErr(err)
	}
	if err := r.recordContactsReset(ctx, userID); err != nil {
		return false, internalErr()
	}
	r.pushContactsReset(ctx, userID)
	return true, nil
}

func (r *Router) onContactsSearch(ctx context.Context, req *tg.ContactsSearchRequest) (*tg.ContactsFound, error) {
	if r.deps.Contacts == nil && r.deps.Channels == nil {
		return &tg.ContactsFound{}, nil
	}
	query := normalizeSearchQuery(req.Q)
	if query == "" {
		return nil, searchQueryEmptyErr()
	}
	if utf8.RuneCountInString(query) < 3 {
		return nil, queryTooShortErr()
	}
	if utf8.RuneCountInString(query) > maxContactSearchQLen {
		return nil, limitInvalidErr()
	}
	limit := req.Limit
	if limit <= 0 || limit > maxContactSearchLimit {
		limit = maxContactSearchLimit
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	res := domain.UserSearchResult{}
	if r.deps.Contacts != nil {
		userRes, err := r.deps.Contacts.Search(ctx, userID, query, limit)
		if err != nil {
			return nil, internalErr()
		}
		res = userRes
	}
	if r.deps.Channels != nil {
		channelRes, err := r.deps.Channels.SearchPublicChannels(ctx, userID, query, limit)
		if err != nil {
			return nil, channelInvalidErr(err)
		}
		res.MyChannelResults = channelRes.MyResults
		res.ChannelResults = channelRes.Results
	}
	return tgContactsFound(userID, r.withUserSearchPresence(res)), nil
}

func (r *Router) onContactsResolveUsername(ctx context.Context, req *tg.ContactsResolveUsernameRequest) (*tg.ContactsResolvedPeer, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if svc, ok := r.deps.Users.(UserIdentityService); ok {
		u, found, err := svc.ResolveUsername(ctx, userID, req.Username)
		if err != nil {
			return nil, usernameErr(err)
		}
		if found {
			return r.tgResolvedUserPeer(userID, u), nil
		}
	}
	if r.deps.Channels != nil {
		ch, found, err := r.deps.Channels.ResolvePublicUsername(ctx, userID, req.Username)
		if err != nil {
			return nil, usernameErr(err)
		}
		if found {
			return tgResolvedChannelPeer(userID, ch), nil
		}
	}
	return nil, usernameNotOccupiedErr()
}

func (r *Router) onContactsResolvePhone(ctx context.Context, phone string) (*tg.ContactsResolvedPeer, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	svc, ok := r.deps.Users.(UserIdentityService)
	if !ok {
		return nil, phoneNotOccupiedErr()
	}
	u, found, err := svc.ResolvePhone(ctx, userID, phone)
	if err != nil {
		if errors.Is(err, domain.ErrPhoneNotOccupied) {
			return nil, phoneNotOccupiedErr()
		}
		return nil, internalErr()
	}
	if !found {
		return nil, phoneNotOccupiedErr()
	}
	return r.tgResolvedUserPeer(userID, u), nil
}

func (r *Router) tgResolvedUserPeer(currentUserID int64, u domain.User) *tg.ContactsResolvedPeer {
	var user tg.UserClass
	if u.ID == currentUserID {
		user = r.tgSelfUser(u)
	} else {
		user = r.tgUser(u)
	}
	return &tg.ContactsResolvedPeer{
		Peer:  &tg.PeerUser{UserID: u.ID},
		Users: []tg.UserClass{user},
	}
}

func tgResolvedChannelPeer(currentUserID int64, ch domain.Channel) *tg.ContactsResolvedPeer {
	return &tg.ContactsResolvedPeer{
		Peer:  &tg.PeerChannel{ChannelID: ch.ID},
		Chats: []tg.ChatClass{tgChannelChat(currentUserID, ch, nil)},
	}
}

func normalizeSearchQuery(query string) string {
	query = strings.TrimSpace(query)
	query = strings.TrimPrefix(query, "@")
	return strings.TrimSpace(query)
}

func validContactInput(phone, firstName, lastName, note string, entities int) bool {
	if utf8.RuneCountInString(phone) > maxContactPhoneLength {
		return false
	}
	if utf8.RuneCountInString(firstName) > maxContactNameLength || utf8.RuneCountInString(lastName) > maxContactNameLength {
		return false
	}
	if utf8.RuneCountInString(note) > maxContactNoteLength || entities > maxMessageEntityCount {
		return false
	}
	return true
}

func contactNote(note tg.TextWithEntities, ok bool) (string, []domain.MessageEntity) {
	if !ok {
		return "", nil
	}
	return note.Text, domainMessageEntities(note.Entities)
}

func (r *Router) contactPeerSettingsUpdates(ctx context.Context, userID int64, peerUser domain.User, settings domain.PeerSettings, includeSelf bool) *tg.Updates {
	users := make([]tg.UserClass, 0, 2)
	if includeSelf && r.deps.Users != nil {
		if self, err := r.deps.Users.Self(ctx, userID); err == nil && self.ID != 0 {
			users = append(users, r.tgSelfUser(self))
		}
	}
	users = append(users, r.tgUser(peerUser))
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdatePeerSettings{
				Peer:     &tg.PeerUser{UserID: peerUser.ID},
				Settings: tgPeerSettings(settings),
			},
		},
		Users: users,
		Date:  int(r.clock.Now().Unix()),
		Seq:   0,
	}
}

func (r *Router) pushContactsReset(ctx context.Context, userID int64) {
	r.pushUserUpdatesIfNoReliableDispatch(ctx, userID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateContactsReset{}},
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	})
}

func (r *Router) recordContactsReset(ctx context.Context, userID int64) error {
	if r.deps.Updates == nil || userID == 0 {
		return nil
	}
	authKeyID, _ := AuthKeyIDFrom(ctx)
	sessionID, _ := SessionIDFrom(ctx)
	_, _, err := r.deps.Updates.RecordContactsReset(ctx, authKeyID, userID, sessionID)
	return err
}

func (r *Router) recordPeerSettings(ctx context.Context, userID int64, peer domain.Peer, settings domain.PeerSettings) error {
	if r.deps.Updates == nil || userID == 0 {
		return nil
	}
	authKeyID, _ := AuthKeyIDFrom(ctx)
	sessionID, _ := SessionIDFrom(ctx)
	_, _, err := r.deps.Updates.RecordPeerSettings(ctx, authKeyID, userID, peer, settings, sessionID)
	return err
}

type reliableUpdateDispatchReporter interface {
	UsesReliableDispatch() bool
}

func (r *Router) hasReliableUpdateDispatch() bool {
	reporter, ok := r.deps.Updates.(reliableUpdateDispatchReporter)
	return ok && reporter.UsesReliableDispatch()
}

func (r *Router) pushUserUpdatesIfNoReliableDispatch(ctx context.Context, userID int64, updates *tg.Updates) {
	if r.hasReliableUpdateDispatch() {
		return
	}
	r.pushUserUpdates(ctx, userID, updates)
}

func (r *Router) pushUserUpdates(ctx context.Context, userID int64, updates *tg.Updates) int {
	return r.pushUserMessage(ctx, userID, "push user updates", updates)
}

func tgPeerSettings(settings domain.PeerSettings) tg.PeerSettings {
	if settings.HiddenPeerSettingsBar {
		return tg.PeerSettings{}
	}
	return tg.PeerSettings{
		AddContact:            settings.AddContact,
		BlockContact:          settings.BlockContact,
		ShareContact:          settings.ShareContact,
		NeedContactsException: settings.NeedContactsException,
	}
}

func contactErr(err error) error {
	switch {
	case errors.Is(err, contacts.ErrContactNameEmpty):
		return contactNameEmptyErr()
	case errors.Is(err, contacts.ErrContactIDInvalid):
		return contactIDInvalidErr()
	default:
		return internalErr()
	}
}
