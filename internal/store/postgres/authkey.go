package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// AuthKeyStore 用 PostgreSQL 实现 store.AuthKeyStore。
type AuthKeyStore struct {
	q *sqlcgen.Queries
}

// NewAuthKeyStore 基于 pgx 连接池（或事务）创建 AuthKeyStore。
func NewAuthKeyStore(db sqlcgen.DBTX) *AuthKeyStore {
	return &AuthKeyStore{q: sqlcgen.New(db)}
}

// Save 实现 store.AuthKeyStore。auth_key_id 以小端解释为 int64 存入 BIGINT；
// created_at 交由 DB 默认值（now()），故传入的 CreatedAt 不落库。
func (s *AuthKeyStore) Save(ctx context.Context, k store.AuthKeyData) error {
	if err := s.q.UpsertAuthKey(ctx, sqlcgen.UpsertAuthKeyParams{
		AuthKeyID:  authKeyIDToInt64(k.ID),
		Body:       k.Value[:],
		ServerSalt: k.ServerSalt,
	}); err != nil {
		return fmt.Errorf("upsert auth key: %w", err)
	}
	return nil
}

// Get 实现 store.AuthKeyStore。不存在时 found=false。
func (s *AuthKeyStore) Get(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	row, err := s.q.GetAuthKey(ctx, authKeyIDToInt64(id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.AuthKeyData{}, false, nil
		}
		return store.AuthKeyData{}, false, fmt.Errorf("get auth key: %w", err)
	}
	if len(row.Body) != len(store.AuthKeyData{}.Value) {
		return store.AuthKeyData{}, false, fmt.Errorf("auth key body length = %d, want 256", len(row.Body))
	}
	data := store.AuthKeyData{ID: id, ServerSalt: row.ServerSalt}
	copy(data.Value[:], row.Body)
	if row.CreatedAt.Valid {
		data.CreatedAt = row.CreatedAt.Time.Unix()
	}
	return data, true, nil
}

// authKeyIDToInt64 把 [8]byte 的 auth_key_id 按小端解释为 int64（MTProto 定义即 SHA1 低 64 位）。
func authKeyIDToInt64(id [8]byte) int64 {
	return int64(binary.LittleEndian.Uint64(id[:]))
}

func authKeyIDFromInt64(v int64) [8]byte {
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], uint64(v))
	return id
}
