package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// AuthorizationStore 用 PostgreSQL 实现 store.AuthorizationStore。
type AuthorizationStore struct {
	q *sqlcgen.Queries
}

// NewAuthorizationStore 基于 pgx 连接池（或事务）创建 AuthorizationStore。
func NewAuthorizationStore(db sqlcgen.DBTX) *AuthorizationStore {
	return &AuthorizationStore{q: sqlcgen.New(db)}
}

func (s *AuthorizationStore) Bind(ctx context.Context, a domain.Authorization) error {
	if err := s.q.UpsertAuthorization(ctx, sqlcgen.UpsertAuthorizationParams{
		AuthKeyID:     authKeyIDToInt64(a.AuthKeyID),
		UserID:        a.UserID,
		Layer:         int32(a.Layer),
		DeviceModel:   a.DeviceModel,
		Platform:      a.Platform,
		SystemVersion: a.SystemVersion,
		ApiID:         int32(a.APIID),
		AppVersion:    a.AppVersion,
		Ip:            a.IP,
	}); err != nil {
		return fmt.Errorf("upsert authorization: %w", err)
	}
	return nil
}

func (s *AuthorizationStore) ByAuthKey(ctx context.Context, id [8]byte) (domain.Authorization, bool, error) {
	row, err := s.q.GetAuthorizationByAuthKey(ctx, authKeyIDToInt64(id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Authorization{}, false, nil
		}
		return domain.Authorization{}, false, fmt.Errorf("get authorization: %w", err)
	}
	return domain.Authorization{
		AuthKeyID:     id,
		UserID:        row.UserID,
		Layer:         int(row.Layer),
		DeviceModel:   row.DeviceModel,
		Platform:      row.Platform,
		SystemVersion: row.SystemVersion,
		APIID:         int(row.ApiID),
		AppVersion:    row.AppVersion,
		IP:            row.Ip,
	}, true, nil
}

func (s *AuthorizationStore) ListByUser(ctx context.Context, userID int64) ([]domain.Authorization, error) {
	rows, err := s.q.ListAuthorizationsByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list authorizations by user: %w", err)
	}
	out := make([]domain.Authorization, 0, len(rows))
	for _, row := range rows {
		out = append(out, authorizationFromRow(row))
	}
	return out, nil
}

func (s *AuthorizationStore) Delete(ctx context.Context, id [8]byte) error {
	if err := s.q.DeleteAuthorization(ctx, authKeyIDToInt64(id)); err != nil {
		return fmt.Errorf("delete authorization: %w", err)
	}
	return nil
}

func authorizationFromRow(row sqlcgen.Authorization) domain.Authorization {
	return domain.Authorization{
		AuthKeyID:     authKeyIDFromInt64(row.AuthKeyID),
		UserID:        row.UserID,
		Layer:         int(row.Layer),
		DeviceModel:   row.DeviceModel,
		Platform:      row.Platform,
		SystemVersion: row.SystemVersion,
		APIID:         int(row.ApiID),
		AppVersion:    row.AppVersion,
		IP:            row.Ip,
	}
}
