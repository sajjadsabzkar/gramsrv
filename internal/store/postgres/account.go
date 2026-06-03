package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// PasswordStore 用 PostgreSQL 实现 store.PasswordStore。
type PasswordStore struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

// NewPasswordStore 基于 pgx 连接池（或事务）创建 PasswordStore。
func NewPasswordStore(db sqlcgen.DBTX) *PasswordStore {
	return &PasswordStore{db: db, q: sqlcgen.New(db)}
}

func (s *PasswordStore) GetByUser(ctx context.Context, userID int64) (domain.PasswordSettings, bool, error) {
	row, err := s.q.GetPasswordByUser(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.PasswordSettings{}, false, nil
		}
		return domain.PasswordSettings{}, false, fmt.Errorf("get account password: %w", err)
	}
	return domain.PasswordSettings{
		HasRecovery:             row.HasRecovery,
		HasSecureValues:         row.HasSecureValues,
		HasPassword:             row.HasPassword,
		Hint:                    row.Hint,
		EmailUnconfirmedPattern: row.EmailUnconfirmedPattern,
		LoginEmailPattern:       row.LoginEmailPattern,
		SecureRandom:            append([]byte(nil), row.SecureRandom...),
	}, true, nil
}

func (s *PasswordStore) Save(ctx context.Context, userID int64, settings domain.PasswordSettings) error {
	if err := s.q.UpsertPassword(ctx, sqlcgen.UpsertPasswordParams{
		UserID:                  userID,
		HasRecovery:             settings.HasRecovery,
		HasSecureValues:         settings.HasSecureValues,
		HasPassword:             settings.HasPassword,
		Hint:                    settings.Hint,
		EmailUnconfirmedPattern: settings.EmailUnconfirmedPattern,
		LoginEmailPattern:       settings.LoginEmailPattern,
		SecureRandom:            settings.SecureRandom,
	}); err != nil {
		return fmt.Errorf("upsert account password: %w", err)
	}
	return nil
}

func (s *PasswordStore) GetReactionSettings(ctx context.Context, userID int64) (domain.AccountReactionSettings, bool, error) {
	row := s.db.QueryRow(ctx, `
SELECT messages_notify_from, stories_notify_from, poll_votes_notify_from, show_previews,
       default_reaction_type, default_reaction_value,
       paid_privacy_kind, paid_privacy_peer_type, paid_privacy_peer_id
FROM account_reaction_settings
WHERE user_id = $1`, userID)
	var messagesFrom, storiesFrom, pollVotesFrom string
	var defaultType, defaultValue string
	var paidKind string
	var paidPeerType sql.NullString
	var paidPeerID sql.NullInt64
	settings := domain.DefaultAccountReactionSettings()
	if err := row.Scan(
		&messagesFrom, &storiesFrom, &pollVotesFrom, &settings.Notify.ShowPreviews,
		&defaultType, &defaultValue, &paidKind, &paidPeerType, &paidPeerID,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.AccountReactionSettings{}, false, nil
		}
		return domain.AccountReactionSettings{}, false, fmt.Errorf("get account reaction settings: %w", err)
	}
	settings.Notify.MessagesFrom = domain.ReactionNotifyFrom(messagesFrom)
	settings.Notify.StoriesFrom = domain.ReactionNotifyFrom(storiesFrom)
	settings.Notify.PollVotesFrom = domain.ReactionNotifyFrom(pollVotesFrom)
	settings.DefaultReaction = domain.MessageReaction{Type: domain.MessageReactionType(defaultType), Emoticon: defaultValue}
	settings.PaidPrivacy = domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyKind(paidKind)}
	if settings.PaidPrivacy.Kind == domain.PaidReactionPrivacyPeer && paidPeerType.Valid && paidPeerID.Valid {
		peer := domain.Peer{Type: domain.PeerType(paidPeerType.String), ID: paidPeerID.Int64}
		settings.PaidPrivacy.Peer = &peer
	}
	return settings, true, nil
}

func (s *PasswordStore) SaveReactionSettings(ctx context.Context, userID int64, settings domain.AccountReactionSettings) error {
	var paidPeerType any
	var paidPeerID any
	if settings.PaidPrivacy.Kind == domain.PaidReactionPrivacyPeer && settings.PaidPrivacy.Peer != nil {
		paidPeerType = string(settings.PaidPrivacy.Peer.Type)
		paidPeerID = settings.PaidPrivacy.Peer.ID
	}
	if _, err := s.db.Exec(ctx, `
INSERT INTO account_reaction_settings (
    user_id, messages_notify_from, stories_notify_from, poll_votes_notify_from, show_previews,
    default_reaction_type, default_reaction_value, paid_privacy_kind, paid_privacy_peer_type, paid_privacy_peer_id
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (user_id) DO UPDATE SET
    messages_notify_from = EXCLUDED.messages_notify_from,
    stories_notify_from = EXCLUDED.stories_notify_from,
    poll_votes_notify_from = EXCLUDED.poll_votes_notify_from,
    show_previews = EXCLUDED.show_previews,
    default_reaction_type = EXCLUDED.default_reaction_type,
    default_reaction_value = EXCLUDED.default_reaction_value,
    paid_privacy_kind = EXCLUDED.paid_privacy_kind,
    paid_privacy_peer_type = EXCLUDED.paid_privacy_peer_type,
    paid_privacy_peer_id = EXCLUDED.paid_privacy_peer_id,
    updated_at = now()`,
		userID,
		string(settings.Notify.MessagesFrom), string(settings.Notify.StoriesFrom), string(settings.Notify.PollVotesFrom), settings.Notify.ShowPreviews,
		string(settings.DefaultReaction.Type), settings.DefaultReaction.Emoticon,
		string(settings.PaidPrivacy.Kind), paidPeerType, paidPeerID,
	); err != nil {
		return fmt.Errorf("save account reaction settings: %w", err)
	}
	return nil
}
