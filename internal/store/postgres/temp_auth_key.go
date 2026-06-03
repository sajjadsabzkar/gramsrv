package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// TempAuthKeyBindingStore 用 PostgreSQL 实现 store.TempAuthKeyBindingStore。
type TempAuthKeyBindingStore struct {
	q *sqlcgen.Queries
}

// NewTempAuthKeyBindingStore 基于 pgx 连接池（或事务）创建 TempAuthKeyBindingStore。
func NewTempAuthKeyBindingStore(db sqlcgen.DBTX) *TempAuthKeyBindingStore {
	return &TempAuthKeyBindingStore{q: sqlcgen.New(db)}
}

func (s *TempAuthKeyBindingStore) Save(ctx context.Context, b domain.TempAuthKeyBinding) error {
	if err := s.q.UpsertTempAuthKeyBinding(ctx, sqlcgen.UpsertTempAuthKeyBindingParams{
		TempAuthKeyID:    authKeyIDToInt64(b.TempAuthKeyID),
		PermAuthKeyID:    b.PermAuthKeyID,
		Nonce:            b.Nonce,
		TempSessionID:    b.TempSessionID,
		ExpiresAt:        int32(b.ExpiresAt),
		EncryptedMessage: b.EncryptedMessage,
	}); err != nil {
		return fmt.Errorf("upsert temp auth key binding: %w", err)
	}
	return nil
}

func (s *TempAuthKeyBindingStore) GetByTemp(ctx context.Context, tempAuthKeyID [8]byte) (domain.TempAuthKeyBinding, bool, error) {
	row, err := s.q.GetTempAuthKeyBinding(ctx, authKeyIDToInt64(tempAuthKeyID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.TempAuthKeyBinding{}, false, nil
		}
		return domain.TempAuthKeyBinding{}, false, fmt.Errorf("get temp auth key binding: %w", err)
	}
	return domain.TempAuthKeyBinding{
		TempAuthKeyID:    authKeyIDFromInt64(row.TempAuthKeyID),
		PermAuthKeyID:    row.PermAuthKeyID,
		Nonce:            row.Nonce,
		TempSessionID:    row.TempSessionID,
		ExpiresAt:        int(row.ExpiresAt),
		EncryptedMessage: append([]byte(nil), row.EncryptedMessage...),
	}, true, nil
}
