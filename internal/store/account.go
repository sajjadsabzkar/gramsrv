package store

import (
	"context"

	"telesrv/internal/domain"
)

// PasswordStore 持久化账号 2FA/SRP 配置。
type PasswordStore interface {
	GetByUser(ctx context.Context, userID int64) (domain.PasswordSettings, bool, error)
	Save(ctx context.Context, userID int64, settings domain.PasswordSettings) error
}

// AccountReactionSettingsStore persists account-level reaction preferences.
type AccountReactionSettingsStore interface {
	GetReactionSettings(ctx context.Context, userID int64) (domain.AccountReactionSettings, bool, error)
	SaveReactionSettings(ctx context.Context, userID int64, settings domain.AccountReactionSettings) error
}
