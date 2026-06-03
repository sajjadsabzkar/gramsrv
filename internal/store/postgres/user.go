package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// UserStore 用 PostgreSQL 实现 store.UserStore。
type UserStore struct {
	q *sqlcgen.Queries
}

// NewUserStore 基于 pgx 连接池（或事务）创建 UserStore。
func NewUserStore(db sqlcgen.DBTX) *UserStore {
	return &UserStore{q: sqlcgen.New(db)}
}

func (s *UserStore) ByID(ctx context.Context, id int64) (domain.User, bool, error) {
	row, err := s.q.GetUserByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, false, nil
		}
		return domain.User{}, false, fmt.Errorf("get user by id: %w", err)
	}
	return userFromModel(row), true, nil
}

func (s *UserStore) ByIDs(ctx context.Context, ids []int64) ([]domain.User, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.GetUsersByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("get users by ids: %w", err)
	}
	out := make([]domain.User, 0, len(rows))
	for _, row := range rows {
		out = append(out, userFromModel(row))
	}
	return out, nil
}

func (s *UserStore) ByPhone(ctx context.Context, phone string) (domain.User, bool, error) {
	row, err := s.q.GetUserByPhone(ctx, phone)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, false, nil
		}
		return domain.User{}, false, fmt.Errorf("get user by phone: %w", err)
	}
	return userFromModel(row), true, nil
}

func (s *UserStore) ByPhones(ctx context.Context, phones []string) ([]domain.User, error) {
	if len(phones) == 0 {
		return nil, nil
	}
	rows, err := s.q.GetUsersByPhones(ctx, phones)
	if err != nil {
		return nil, fmt.Errorf("get users by phones: %w", err)
	}
	out := make([]domain.User, 0, len(rows))
	for _, row := range rows {
		out = append(out, userFromModel(row))
	}
	return out, nil
}

func (s *UserStore) ByUsername(ctx context.Context, username string) (domain.User, bool, error) {
	username = strings.TrimSpace(strings.TrimPrefix(username, "@"))
	if username == "" {
		return domain.User{}, false, nil
	}
	row, err := s.q.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, false, nil
		}
		return domain.User{}, false, fmt.Errorf("get user by username: %w", err)
	}
	return userFromModel(row), true, nil
}

func (s *UserStore) Search(ctx context.Context, currentUserID int64, query, phoneQuery string, limit int) (domain.UserSearchResult, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if currentUserID == 0 || query == "" {
		return domain.UserSearchResult{}, nil
	}
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	rows, err := s.q.SearchUsers(ctx, sqlcgen.SearchUsersParams{
		CurrentUserID: currentUserID,
		QueryLower:    query,
		QueryLike:     escapeLike(query),
		PhoneQuery:    phoneQuery,
		LimitCount:    int32(limit),
	})
	if err != nil {
		return domain.UserSearchResult{}, fmt.Errorf("search users: %w", err)
	}
	out := domain.UserSearchResult{
		MyResults: make([]domain.User, 0, len(rows)),
		Results:   make([]domain.User, 0, len(rows)),
	}
	for _, row := range rows {
		u := domain.User{
			ID:          row.ID,
			AccessHash:  row.AccessHash,
			Phone:       row.Phone,
			FirstName:   row.FirstName,
			LastName:    row.LastName,
			About:       row.About,
			Username:    row.Username,
			CountryCode: row.CountryCode,
			Verified:    row.Verified,
			Support:     row.Support,
			LastSeenAt:  int(row.LastSeenAt),
			Contact:     row.Contact,
			Mutual:      row.Mutual,
		}
		if row.Contact {
			out.MyResults = append(out.MyResults, u)
		} else {
			out.Results = append(out.Results, u)
		}
	}
	return out, nil
}

func (s *UserStore) UpdateProfile(ctx context.Context, userID int64, firstName, lastName, about string) (domain.User, error) {
	row, err := s.q.UpdateUserProfile(ctx, sqlcgen.UpdateUserProfileParams{
		ID:        userID,
		FirstName: firstName,
		LastName:  lastName,
		About:     about,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, domain.ErrFirstNameInvalid
		}
		return domain.User{}, fmt.Errorf("update user profile: %w", err)
	}
	return userFromModel(row), nil
}

func (s *UserStore) UpdateUsername(ctx context.Context, userID int64, username string) (domain.User, error) {
	username = strings.TrimSpace(strings.TrimPrefix(username, "@"))
	row, err := s.q.UpdateUserUsername(ctx, sqlcgen.UpdateUserUsernameParams{
		ID:       userID,
		Username: username,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "users_username_lower_unique_idx" {
			return domain.User{}, domain.ErrUsernameOccupied
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.User{}, domain.ErrUsernameNotOccupied
		}
		return domain.User{}, fmt.Errorf("update user username: %w", err)
	}
	return userFromModel(row), nil
}

func (s *UserStore) UpdateLastSeen(ctx context.Context, userID int64, lastSeenAt int) error {
	if lastSeenAt <= 0 {
		return nil
	}
	if err := s.q.UpdateUserLastSeen(ctx, sqlcgen.UpdateUserLastSeenParams{
		ID:         userID,
		LastSeenAt: int64(lastSeenAt),
	}); err != nil {
		return fmt.Errorf("update user last seen: %w", err)
	}
	return nil
}

func (s *UserStore) Create(ctx context.Context, u domain.User) (domain.User, error) {
	row, err := s.q.CreateUser(ctx, sqlcgen.CreateUserParams{
		AccessHash:  u.AccessHash,
		Phone:       u.Phone,
		FirstName:   u.FirstName,
		LastName:    u.LastName,
		Username:    u.Username,
		CountryCode: u.CountryCode,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "users_username_lower_unique_idx" {
			return domain.User{}, domain.ErrUsernameOccupied
		}
		return domain.User{}, fmt.Errorf("create user: %w", err)
	}
	return userFromModel(row), nil
}

func escapeLike(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '%' || r == '_' || r == '\\' {
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func userFromModel(r sqlcgen.User) domain.User {
	return domain.User{
		ID:          r.ID,
		AccessHash:  r.AccessHash,
		Phone:       r.Phone,
		FirstName:   r.FirstName,
		LastName:    r.LastName,
		About:       r.About,
		Username:    r.Username,
		CountryCode: r.CountryCode,
		Verified:    r.Verified,
		Support:     r.Support,
		LastSeenAt:  int(r.LastSeenAt),
	}
}
