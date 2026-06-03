package rpc

import (
	"context"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
)

// registerAiCompose 注册第一阶段 TDesktop 启动所需 aicompose.* RPC 兼容响应。
func (r *Router) registerAiCompose(d *tg.ServerDispatcher) {
	d.OnAicomposeGetTones(func(ctx context.Context, hash int64) (tg.AicomposeTonesClass, error) {
		return tdesktop.AiComposeTones(), nil
	})
}
