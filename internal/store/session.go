package store

import "context"

// SessionData 是一条 MTProto session 记录（client 生成的 session_id）。
//
// 后续里程碑会扩展 device / layer 等字段。
type SessionData struct {
	ID        int64   // session_id（客户端生成）
	AuthKeyID [8]byte // 绑定的 auth key
	Salt      int64   // 当前 server salt
	LastSeen  int64   // unix 秒
}

// SessionStore 记录在线 MTProto session。实现见 store/memory（测试替身）、store/redisstore。
type SessionStore interface {
	// Save 保存或更新一条 session 记录。
	Save(ctx context.Context, s SessionData) error
	// Get 按 session_id 查询；不存在时 found=false。
	Get(ctx context.Context, id int64) (data SessionData, found bool, err error)
	// Delete 删除一条 session 记录；不存在时不报错。
	Delete(ctx context.Context, id int64) error
}
