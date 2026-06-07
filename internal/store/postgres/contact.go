package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// ContactStore 用 PostgreSQL 实现 store.ContactStore。
type ContactStore struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

// NewContactStore 基于 pgx 连接池（或事务）创建 ContactStore。
func NewContactStore(db sqlcgen.DBTX) *ContactStore {
	return &ContactStore{db: db, q: sqlcgen.New(db)}
}

func (s *ContactStore) ListByUser(ctx context.Context, userID int64) (domain.ContactList, error) {
	rows, err := s.q.ListContactsByUser(ctx, userID)
	if err != nil {
		return domain.ContactList{}, fmt.Errorf("list contacts: %w", err)
	}
	out := domain.ContactList{Contacts: make([]domain.Contact, 0, len(rows))}
	for _, row := range rows {
		contact, err := contactFromListRow(row)
		if err != nil {
			return domain.ContactList{}, err
		}
		out.Contacts = append(out.Contacts, contact)
	}
	out.Hash = contactListHash(out.Contacts)
	return out, nil
}

func (s *ContactStore) Get(ctx context.Context, userID, contactUserID int64) (domain.Contact, bool, error) {
	row, err := s.q.GetContact(ctx, sqlcgen.GetContactParams{
		UserID:        userID,
		ContactUserID: contactUserID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Contact{}, false, nil
		}
		return domain.Contact{}, false, fmt.Errorf("get contact: %w", err)
	}
	contact, err := contactFromGetRow(row)
	if err != nil {
		return domain.Contact{}, false, err
	}
	return contact, true, nil
}

func (s *ContactStore) Upsert(ctx context.Context, userID int64, input domain.ContactInput) (domain.Contact, error) {
	entities, err := encodeMessageEntities(input.NoteEntities)
	if err != nil {
		return domain.Contact{}, err
	}
	row, err := s.q.UpsertContact(ctx, sqlcgen.UpsertContactParams{
		UserID:           userID,
		ContactUserID:    input.ContactUserID,
		ContactPhone:     input.Phone,
		ContactFirstName: input.FirstName,
		ContactLastName:  input.LastName,
		Note:             input.Note,
		NoteEntities:     entities,
	})
	if err != nil {
		return domain.Contact{}, fmt.Errorf("upsert contact: %w", err)
	}
	contact, err := contactFromUpsertRow(row)
	if err != nil {
		return domain.Contact{}, err
	}
	return contact, nil
}

const upsertContactsManySQL = `
WITH input AS (
  SELECT
    $1::bigint AS user_id,
    i.contact_user_id,
    i.contact_phone,
    i.contact_first_name,
    i.contact_last_name,
    i.note,
    i.note_entities_json::jsonb AS note_entities,
    i.ord
  FROM unnest(
    $2::bigint[],
    $3::text[],
    $4::text[],
    $5::text[],
    $6::text[],
    $7::text[]
  ) WITH ORDINALITY AS i(contact_user_id, contact_phone, contact_first_name, contact_last_name, note, note_entities_json, ord)
),
reverse AS (
  SELECT
    i.contact_user_id,
    EXISTS (
      SELECT 1
      FROM contacts c
      WHERE c.user_id = i.contact_user_id
        AND c.contact_user_id = i.user_id
    )::boolean AS mutual
  FROM input i
),
upserted AS (
  INSERT INTO contacts (
    user_id,
    contact_user_id,
    contact_phone,
    contact_first_name,
    contact_last_name,
    note,
    note_entities,
    mutual
  )
  SELECT
    i.user_id,
    i.contact_user_id,
    i.contact_phone,
    i.contact_first_name,
    i.contact_last_name,
    i.note,
    i.note_entities,
    r.mutual
  FROM input i
  JOIN reverse r ON r.contact_user_id = i.contact_user_id
  ON CONFLICT (user_id, contact_user_id) DO UPDATE SET
    contact_phone = EXCLUDED.contact_phone,
    contact_first_name = EXCLUDED.contact_first_name,
    contact_last_name = EXCLUDED.contact_last_name,
    note = EXCLUDED.note,
    note_entities = EXCLUDED.note_entities,
    mutual = contacts.mutual OR EXCLUDED.mutual,
    updated_at = now()
  RETURNING *
),
reverse_updated AS (
  UPDATE contacts c
  SET mutual = true,
      updated_at = now()
  FROM upserted u
  WHERE c.user_id = u.contact_user_id
    AND c.contact_user_id = $1::bigint
    AND NOT c.mutual
  RETURNING c.user_id
)
SELECT
  c.contact_user_id,
  c.mutual,
  c.contact_phone,
  c.contact_first_name,
  c.contact_last_name,
  c.note,
  COALESCE(c.note_entities::text, '[]')::text AS note_entities_json,
  u.id,
  u.access_hash,
  COALESCE(NULLIF(c.contact_phone, ''), u.phone)::text AS phone,
  COALESCE(NULLIF(c.contact_first_name, ''), u.first_name)::text AS first_name,
  COALESCE(c.contact_last_name, u.last_name)::text AS last_name,
  u.username,
  u.country_code,
  u.verified,
  u.support,
  u.last_seen_at,
  EXISTS (SELECT 1 FROM reverse_updated ru WHERE ru.user_id = c.contact_user_id)::boolean AS reverse_mutual_changed
FROM upserted c
JOIN users u ON u.id = c.contact_user_id
JOIN input i ON i.contact_user_id = c.contact_user_id
ORDER BY i.ord
`

func (s *ContactStore) UpsertMany(ctx context.Context, userID int64, inputs []domain.ContactInput) ([]domain.Contact, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	contactUserIDs := make([]int64, 0, len(inputs))
	phones := make([]string, 0, len(inputs))
	firstNames := make([]string, 0, len(inputs))
	lastNames := make([]string, 0, len(inputs))
	notes := make([]string, 0, len(inputs))
	noteEntities := make([]string, 0, len(inputs))
	for _, input := range inputs {
		if input.ContactUserID == 0 {
			continue
		}
		raw, err := encodeMessageEntities(input.NoteEntities)
		if err != nil {
			return nil, err
		}
		contactUserIDs = append(contactUserIDs, input.ContactUserID)
		phones = append(phones, input.Phone)
		firstNames = append(firstNames, input.FirstName)
		lastNames = append(lastNames, input.LastName)
		notes = append(notes, input.Note)
		noteEntities = append(noteEntities, string(raw))
	}
	if len(contactUserIDs) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, upsertContactsManySQL, userID, contactUserIDs, phones, firstNames, lastNames, notes, noteEntities)
	if err != nil {
		return nil, fmt.Errorf("upsert contacts many: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Contact, 0, len(contactUserIDs))
	for rows.Next() {
		var (
			contactUserID        int64
			mutual               bool
			contactPhone         string
			contactFirstName     string
			contactLastName      string
			note                 string
			noteEntitiesJSON     string
			id                   int64
			accessHash           int64
			phone                string
			firstName            string
			lastName             string
			username             string
			countryCode          string
			verified             bool
			support              bool
			lastSeenAt           int64
			reverseMutualChanged bool
		)
		if err := rows.Scan(
			&contactUserID,
			&mutual,
			&contactPhone,
			&contactFirstName,
			&contactLastName,
			&note,
			&noteEntitiesJSON,
			&id,
			&accessHash,
			&phone,
			&firstName,
			&lastName,
			&username,
			&countryCode,
			&verified,
			&support,
			&lastSeenAt,
			&reverseMutualChanged,
		); err != nil {
			return nil, fmt.Errorf("scan upsert contacts many: %w", err)
		}
		_ = reverseMutualChanged
		entities, err := decodeMessageEntities(noteEntitiesJSON)
		if err != nil {
			return nil, fmt.Errorf("decode contact note entities: %w", err)
		}
		out = append(out, contactFromFields(id, accessHash, phone, firstName, lastName, username, countryCode, verified, support, int(lastSeenAt), contactFirstName, contactLastName, contactPhone, note, entities, mutual))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate upsert contacts many: %w", err)
	}
	return out, nil
}

func (s *ContactStore) UpdateNote(ctx context.Context, userID, contactUserID int64, note string, entities []domain.MessageEntity) (domain.Contact, bool, error) {
	raw, err := encodeMessageEntities(entities)
	if err != nil {
		return domain.Contact{}, false, err
	}
	row, err := s.q.UpdateContactNote(ctx, sqlcgen.UpdateContactNoteParams{
		UserID:        userID,
		ContactUserID: contactUserID,
		Note:          note,
		NoteEntities:  raw,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Contact{}, false, nil
		}
		return domain.Contact{}, false, fmt.Errorf("update contact note: %w", err)
	}
	contact, err := contactFromUpdateNoteRow(row)
	if err != nil {
		return domain.Contact{}, false, err
	}
	return contact, true, nil
}

func (s *ContactStore) Delete(ctx context.Context, userID int64, contactUserIDs []int64) (int, error) {
	if len(contactUserIDs) == 0 {
		return 0, nil
	}
	count, err := s.q.DeleteContacts(ctx, sqlcgen.DeleteContactsParams{
		UserID:         userID,
		ContactUserIds: contactUserIDs,
	})
	if err != nil {
		return 0, fmt.Errorf("delete contacts: %w", err)
	}
	return int(count), nil
}

func contactFromListRow(row sqlcgen.ListContactsByUserRow) (domain.Contact, error) {
	entities, err := decodeMessageEntities(row.NoteEntitiesJson)
	if err != nil {
		return domain.Contact{}, fmt.Errorf("decode contact note entities: %w", err)
	}
	return contactFromFields(row.ID, row.AccessHash, row.Phone, row.FirstName, row.LastName, row.Username, row.CountryCode, row.Verified, row.Support, int(row.LastSeenAt), row.ContactFirstName, row.ContactLastName, row.ContactPhone, row.Note, entities, row.Mutual), nil
}

func contactFromGetRow(row sqlcgen.GetContactRow) (domain.Contact, error) {
	entities, err := decodeMessageEntities(row.NoteEntitiesJson)
	if err != nil {
		return domain.Contact{}, fmt.Errorf("decode contact note entities: %w", err)
	}
	return contactFromFields(row.ID, row.AccessHash, row.Phone, row.FirstName, row.LastName, row.Username, row.CountryCode, row.Verified, row.Support, int(row.LastSeenAt), row.ContactFirstName, row.ContactLastName, row.ContactPhone, row.Note, entities, row.Mutual), nil
}

func contactFromUpsertRow(row sqlcgen.UpsertContactRow) (domain.Contact, error) {
	entities, err := decodeMessageEntities(row.NoteEntitiesJson)
	if err != nil {
		return domain.Contact{}, fmt.Errorf("decode contact note entities: %w", err)
	}
	return contactFromFields(row.ID, row.AccessHash, row.Phone, row.FirstName, row.LastName, row.Username, row.CountryCode, row.Verified, row.Support, int(row.LastSeenAt), row.ContactFirstName, row.ContactLastName, row.ContactPhone, row.Note, entities, row.Mutual), nil
}

func contactFromUpdateNoteRow(row sqlcgen.UpdateContactNoteRow) (domain.Contact, error) {
	entities, err := decodeMessageEntities(row.NoteEntitiesJson)
	if err != nil {
		return domain.Contact{}, fmt.Errorf("decode contact note entities: %w", err)
	}
	return contactFromFields(row.ID, row.AccessHash, row.Phone, row.FirstName, row.LastName, row.Username, row.CountryCode, row.Verified, row.Support, int(row.LastSeenAt), row.ContactFirstName, row.ContactLastName, row.ContactPhone, row.Note, entities, row.Mutual), nil
}

func contactFromFields(id, accessHash int64, phone, firstName, lastName, username, countryCode string, verified, support bool, lastSeenAt int, contactFirstName, contactLastName, contactPhone, note string, noteEntities []domain.MessageEntity, mutual bool) domain.Contact {
	return domain.Contact{
		User: domain.User{
			ID:          id,
			AccessHash:  accessHash,
			Phone:       phone,
			FirstName:   firstName,
			LastName:    lastName,
			Username:    username,
			CountryCode: countryCode,
			Verified:    verified,
			Support:     support,
			LastSeenAt:  lastSeenAt,
			Contact:     true,
			Mutual:      mutual,
		},
		FirstName:    contactFirstName,
		LastName:     contactLastName,
		Phone:        contactPhone,
		Note:         note,
		NoteEntities: noteEntities,
		Mutual:       mutual,
	}
}

func (s *ContactStore) Block(ctx context.Context, userID, blockedUserID int64, date int) (bool, error) {
	if userID == 0 || blockedUserID == 0 || userID == blockedUserID {
		return false, nil
	}
	tag, err := s.db.Exec(ctx, `
INSERT INTO contact_blocks (owner_user_id, blocked_user_id, date)
VALUES ($1, $2, $3)
ON CONFLICT (owner_user_id, blocked_user_id) DO UPDATE SET
  date = EXCLUDED.date,
  created_at = contact_blocks.created_at`, userID, blockedUserID, date)
	if err != nil {
		return false, fmt.Errorf("block contact: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *ContactStore) Unblock(ctx context.Context, userID, blockedUserID int64) (bool, error) {
	if userID == 0 || blockedUserID == 0 {
		return false, nil
	}
	tag, err := s.db.Exec(ctx, `
DELETE FROM contact_blocks
WHERE owner_user_id = $1
  AND blocked_user_id = $2`, userID, blockedUserID)
	if err != nil {
		return false, fmt.Errorf("unblock contact: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *ContactStore) IsBlocked(ctx context.Context, userID, blockedUserID int64) (bool, error) {
	if userID == 0 || blockedUserID == 0 {
		return false, nil
	}
	var blocked bool
	if err := s.db.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM contact_blocks
  WHERE owner_user_id = $1
    AND blocked_user_id = $2
)`, userID, blockedUserID).Scan(&blocked); err != nil {
		return false, fmt.Errorf("check contact block: %w", err)
	}
	return blocked, nil
}

func (s *ContactStore) ListBlocked(ctx context.Context, userID int64, offset, limit int) (domain.BlockedContactList, error) {
	if userID == 0 {
		return domain.BlockedContactList{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM contact_blocks
WHERE owner_user_id = $1`, userID).Scan(&count); err != nil {
		return domain.BlockedContactList{}, fmt.Errorf("count blocked contacts: %w", err)
	}
	rows, err := s.db.Query(ctx, `
SELECT
  b.blocked_user_id,
  b.date,
  u.access_hash,
  u.phone,
  u.first_name,
  u.last_name,
  u.username,
  u.country_code,
  u.verified,
  u.support,
  u.last_seen_at
FROM contact_blocks b
JOIN users u ON u.id = b.blocked_user_id
WHERE b.owner_user_id = $1
ORDER BY b.date DESC, b.blocked_user_id DESC
OFFSET $2
LIMIT $3`, userID, offset, limit)
	if err != nil {
		return domain.BlockedContactList{}, fmt.Errorf("list blocked contacts: %w", err)
	}
	defer rows.Close()
	out := domain.BlockedContactList{Count: count, Blocked: make([]domain.BlockedContact, 0, limit)}
	for rows.Next() {
		var item domain.BlockedContact
		var lastSeen int64
		if err := rows.Scan(
			&item.User.ID,
			&item.Date,
			&item.User.AccessHash,
			&item.User.Phone,
			&item.User.FirstName,
			&item.User.LastName,
			&item.User.Username,
			&item.User.CountryCode,
			&item.User.Verified,
			&item.User.Support,
			&lastSeen,
		); err != nil {
			return domain.BlockedContactList{}, err
		}
		item.User.LastSeenAt = int(lastSeen)
		out.Blocked = append(out.Blocked, item)
	}
	return out, rows.Err()
}

func contactListHash(contacts []domain.Contact) int64 {
	if len(contacts) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [16]byte
	for _, c := range contacts {
		binary.LittleEndian.PutUint64(buf[:8], uint64(c.User.ID))
		if c.Mutual {
			buf[8] = 1
		} else {
			buf[8] = 0
		}
		_, _ = h.Write(buf[:9])
		_, _ = h.Write([]byte(c.FirstName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(c.LastName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(c.Phone))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(c.Note))
		_, _ = h.Write([]byte{0})
	}
	return int64(h.Sum64())
}
