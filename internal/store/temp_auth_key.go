package store

import (
	"context"

	"telesrv/internal/domain"
)

// TempAuthKeyBindingStore 持久化 auth.bindTempAuthKey 的 temp→perm 绑定。
type TempAuthKeyBindingStore interface {
	Save(ctx context.Context, binding domain.TempAuthKeyBinding) error
	GetByTemp(ctx context.Context, tempAuthKeyID [8]byte) (domain.TempAuthKeyBinding, bool, error)
}
