package mtprotoedge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/transport"
)

// Conn 是一个已识别 session 的客户端连接，持有向其加密发送消息所需的全部上下文。
// 由 SessionManager 管理，供请求响应与主动 push 共用。
//
// Send 并发安全：所有出站消息先进 per-Conn outbound actor，由它串行分配 msg_id/seq_no、
// 加密并写 transport，避免高并发 RPC 响应与 push 交错造成 MTProto 顺序错误。
type outboundWriter interface {
	Send(context.Context, *bin.Buffer) error
}

type Conn struct {
	transport    transport.Conn
	writer       outboundWriter
	cipher       crypto.Cipher
	msgID        *proto.MessageIDGen
	writeTimeout time.Duration
	metrics      Metrics

	authKeyID [8]byte
	sessionID int64
	salt      int64
	key       crypto.AuthKey

	outbound        chan outboundOp
	outboundControl chan outboundOp
	outboundStop    chan struct{}
	outboundDone    chan struct{}
	outboundClose   sync.Once

	rpcQueue   chan inboundRPC
	rpcStop    chan struct{}
	rpcCancel  context.CancelFunc
	rpcClose   sync.Once
	rpcWG      sync.WaitGroup
	rpcTimeout time.Duration
	// inflightRPCBytes 跟踪已入队未完成的 inbound RPC body 总字节，配合 maxInflightRPCBytes
	// 给 RPC 队列设字节预算（不止限条数），防对抗客户端发大请求撑内存。
	inflightRPCBytes atomic.Int64
	// RPC worker 懒启动：首个 RPC 入队时才起 worker（ensureInboundRPCWorkers），
	// 避免握手后静默 / 纯推送目标连接白白钉住 rpcMaxInflight 个 goroutine。
	rpcRootCtx     context.Context
	rpcMaxInflight int
	rpcWorkersOnce sync.Once

	// sentContentMessages 只由 outbound actor 访问，用于生成 MTProto seq_no。
	sentContentMessages int32
	// outboundPlain/outboundWire 只由 outbound actor 访问，用于复用出站加密缓冲。
	outboundPlain bin.Buffer
	outboundWire  bin.Buffer

	identityMu              sync.RWMutex
	businessAuthKeyID       [8]byte
	businessAuthKeyResolved bool
	userID                  atomic.Int64
	userIDResolved          atomic.Bool
	receivesUpdates         atomic.Bool
}

// AuthKeyID 返回连接的 auth_key_id。
func (c *Conn) AuthKeyID() [8]byte { return c.authKeyID }

// BusinessAuthKeyID 返回业务视角的 auth_key_id。
//
// temp auth_key 绑定后解析为 perm auth_key；第二个返回值表示本连接是否已完成解析，
// 即便解析结果等于原始 auth_key_id 也会返回 true，以避免每个 RPC 重复查绑定表。
func (c *Conn) BusinessAuthKeyID() ([8]byte, bool) {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	return c.businessAuthKeyID, c.businessAuthKeyResolved
}

// SetBusinessAuthKeyID 缓存业务视角 auth_key_id。
func (c *Conn) SetBusinessAuthKeyID(id [8]byte) {
	c.identityMu.Lock()
	changed := !c.businessAuthKeyResolved || c.businessAuthKeyID != id
	c.businessAuthKeyID = id
	c.businessAuthKeyResolved = true
	c.identityMu.Unlock()
	if changed {
		c.userID.Store(0)
		c.userIDResolved.Store(false)
	}
}

// SessionID 返回连接的 session_id。
func (c *Conn) SessionID() int64 { return c.sessionID }

// UserID 返回绑定的用户 id；未登录为 0。
func (c *Conn) UserID() int64 { return c.userID.Load() }

// UserIDResolved 返回 user_id 授权状态是否已为当前连接解析过。
//
// resolved=true 且 userID=0 表示该 auth_key 当前未登录；这样登录前的多次 RPC
// 不会反复查询授权表，后续登录成功会由 BindUser 覆盖为真实用户。
func (c *Conn) UserIDResolved() (userID int64, resolved bool) {
	return c.userID.Load(), c.userIDResolved.Load()
}

// ReceivesUpdates 报告该连接是否接收主动推送的 updates。
func (c *Conn) ReceivesUpdates() bool { return c.receivesUpdates.Load() }

// SetReceivesUpdates 设置该连接是否接收主动推送的 updates。
// 登录后的主连接在 updates.getState/getDifference 建立同步基线后置为 true。
func (c *Conn) SetReceivesUpdates(v bool) { c.receivesUpdates.Store(v) }
