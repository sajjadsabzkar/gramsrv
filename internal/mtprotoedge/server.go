package mtprotoedge

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tmap"
	"github.com/gotd/td/transport"

	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

// RPCHandler 把解密后的 RPC 请求体路由到响应。由 internal/rpc 实现。
//
// b 是明文 RPC 请求（已剥离 MTProto 外壳）；返回的 bin.Encoder 会被包成 rpc_result。
// 返回 *tgerr.Error 时连接层将其转为 rpc_error 回发；其他 error 视为连接级故障。
type RPCHandler interface {
	Dispatch(ctx context.Context, authKeyID [8]byte, sessionID int64, b *bin.Buffer) (bin.Encoder, error)
}

// Options 配置 Server。
type Options struct {
	// Logger 日志器。默认 zap.NewNop()。
	Logger *zap.Logger
	// Codec 传输 codec 构造器。nil 表示自动探测（intermediate/abridged/full）。
	Codec func() transport.Codec
	// ObfuscatedTCP 先按 MTProto TCP obfuscation 解包，再自动探测 codec。
	// Telegram Desktop 的 tcpo_only endpoint 会走这个 64 字节前缀流程。
	ObfuscatedTCP bool
	// ReadTimeout 单次读取超时。默认 5m。
	ReadTimeout time.Duration
	// HandshakeIdleTimeout 是连接「建立 session 前」（握手 + 首个加密消息之前）的读超时，
	// 比 ReadTimeout 短，用于快速回收握手后静默的半开 / 异常连接。默认 60s。
	HandshakeIdleTimeout time.Duration
	// WriteTimeout 单次写入超时。默认 30s。
	WriteTimeout time.Duration
	// RPCMaxInflight 是单连接同时处理的 RPC 上限。默认 32。
	RPCMaxInflight int
	// RPCQueueSize 是单连接等待处理的 RPC 队列长度。默认 256。
	RPCQueueSize int
	// RPCTimeout 是单个 RPC 在连接层的最大处理时长。默认 30s。
	RPCTimeout time.Duration

	// DC 是本 server 的 DC ID。默认 2。
	DC int
	// RSAKey 是 server RSA 私钥，用于密钥交换。nil 时无法完成握手。
	RSAKey *rsa.PrivateKey
	// AuthKeys 持久化 auth key。默认内存实现。
	AuthKeys store.AuthKeyStore
	// Sessions 记录在线 MTProto session（持久化数据）。默认内存实现。
	Sessions store.SessionStore
	// ActiveSessions 管理活跃连接。默认新建；传入时可让 RPC 层共享同一注册表。
	ActiveSessions *SessionManager
	// RPC 是 typed RPC 路由。nil 时加密 RPC 被丢弃并记录。
	RPC RPCHandler
	// Metrics 接收连接层指标。默认 NopMetrics。
	Metrics Metrics
	// Clock 用于消息 ID 与时间戳。默认 clock.System。
	Clock clock.Clock
	// Rand 随机源。默认 crypto.DefaultRand()。
	Rand io.Reader
}

func (o *Options) setDefaults() {
	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
	if o.ReadTimeout == 0 {
		o.ReadTimeout = 5 * time.Minute
	}
	if o.HandshakeIdleTimeout == 0 {
		o.HandshakeIdleTimeout = 60 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 30 * time.Second
	}
	if o.RPCMaxInflight <= 0 {
		o.RPCMaxInflight = 32
	}
	if o.RPCQueueSize <= 0 {
		o.RPCQueueSize = 256
	}
	if o.RPCTimeout == 0 {
		o.RPCTimeout = 30 * time.Second
	}
	if o.DC == 0 {
		o.DC = 2
	}
	if o.AuthKeys == nil {
		o.AuthKeys = memory.NewAuthKeyStore()
	}
	if o.Sessions == nil {
		o.Sessions = memory.NewSessionStore()
	}
	if o.Metrics == nil {
		o.Metrics = NopMetrics{}
	}
	if o.Clock == nil {
		o.Clock = clock.System
	}
	if o.Rand == nil {
		o.Rand = crypto.DefaultRand()
	}
}

// Server 是 MTProto 连接层（mtprotoedge）。
//
// 职责见 doc.go。它把原始 TCP 字节流转换为「已解密、已识别 session 的 RPC 请求」：
// 接受连接、协商 codec、完成密钥交换、解密并分发加密消息到 RPC 路由，处理服务消息，
// 并把活跃连接注册到 SessionManager 以支持主动推送（updates 等）。不含业务逻辑。
type Server struct {
	log              *zap.Logger
	codec            func() transport.Codec
	obfuscated       bool
	readTimeout      time.Duration
	handshakeTimeout time.Duration
	writeTimeout     time.Duration
	rpcInflight      int
	rpcQueueSize     int
	rpcTimeout       time.Duration

	dc       int
	key      exchange.PrivateKey
	authKeys store.AuthKeyStore
	sessions store.SessionStore
	conns    *SessionManager
	rpc      RPCHandler
	metrics  Metrics
	cipher   crypto.Cipher
	clock    clock.Clock
	rand     io.Reader
	types    *tmap.Map

	// sessionUID 是本进程 server session 唯一标识，写入 new_session_created。
	sessionUID int64

	// onFrame 是测试钩子：收到一帧时回调其字节数；生产为 nil。
	onFrame func(n int)
}

// New 创建 Server。
func New(opts Options) *Server {
	opts.setDefaults()
	conns := opts.ActiveSessions
	if conns == nil {
		conns = NewSessionManager(opts.Logger.Named("sessions"))
	}
	return &Server{
		log:              opts.Logger,
		codec:            opts.Codec,
		obfuscated:       opts.ObfuscatedTCP,
		readTimeout:      opts.ReadTimeout,
		handshakeTimeout: opts.HandshakeIdleTimeout,
		writeTimeout:     opts.WriteTimeout,
		rpcInflight:      opts.RPCMaxInflight,
		rpcQueueSize:     opts.RPCQueueSize,
		rpcTimeout:       opts.RPCTimeout,
		dc:               opts.DC,
		key:              exchange.PrivateKey{RSA: opts.RSAKey},
		authKeys:         opts.AuthKeys,
		sessions:         opts.Sessions,
		conns:            conns,
		rpc:              opts.RPC,
		metrics:          opts.Metrics,
		cipher:           crypto.NewServerCipher(opts.Rand),
		clock:            opts.Clock,
		rand:             opts.Rand,
		types:            tmap.New(tg.TypesMap(), mt.TypesMap(), proto.TypesMap()),
		sessionUID:       opts.Clock.Now().UnixNano(),
	}
}

// Conns 返回活跃连接注册表，供业务层主动推送（updates 等）。
func (s *Server) Conns() *SessionManager {
	return s.conns
}

// newConn 基于一次解密结果创建一个可发送的连接对象。
func (s *Server) newConn(tc transport.Conn, key crypto.AuthKey, sessionID, salt int64) *Conn {
	c := &Conn{
		transport:    tc,
		writer:       tc,
		cipher:       s.cipher,
		msgID:        proto.NewMessageIDGen(s.clock.Now),
		writeTimeout: s.writeTimeout,
		metrics:      s.metrics,
		authKeyID:    key.ID,
		sessionID:    sessionID,
		salt:         salt,
		key:          key,
	}
	c.startOutbound()
	c.startInboundRPCScheduler(s.rpcInflight, s.rpcQueueSize, s.rpcTimeout)
	return c
}

// Serve 在 ln 上运行 MTProto 连接循环，直到 ctx 取消或发生不可恢复错误。
// ctx 取消时优雅退出：关闭 listener 并等待在途连接处理结束。
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	transportListener := ln
	if s.obfuscated {
		transportListener = transport.ObfuscatedListener(ln)
	}
	l := transport.ListenCodec(s.codec, transportListener)
	s.log.Info("Serving", zap.String("addr", ln.Addr().String()), zap.Int("dc", s.dc), zap.Bool("obfuscated_tcp", s.obfuscated))
	defer s.log.Info("Stopped")

	// ctx 取消时关闭 listener，解除 Accept 阻塞。
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			if s.obfuscated && isClientDisconnect(err) {
				s.log.Debug("Ignoring failed obfuscated accept", zap.Error(err))
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.serveConn(ctx, conn); err != nil && !isClientDisconnect(err) {
				s.log.Info("Connection closed with error", zap.Error(err))
			}
		}()
	}
}

// serveConn 处理单个传输连接：读帧并按 auth_key_id 分流。
//
//   - auth_key_id == 0：未加密的密钥交换起始消息，执行握手并落地 auth key。
//   - auth_key_id 已注册：加密消息，解密、注册连接并分发到 RPC 路由。
//   - auth_key_id 未注册：回 AuthKeyNotFound，促使客户端重新握手。
//
// 连接建立 session 后注册到 SessionManager，结束时注销。
func (s *Server) serveConn(ctx context.Context, conn transport.Conn) (err error) {
	s.metrics.ConnOpened()
	s.log.Debug("Connection accepted")

	var current *Conn
	defer func() {
		if current != nil {
			s.conns.Unregister(current)
			current.Close()
		}
		s.metrics.ConnClosed()
		s.log.Debug("Connection closed", zap.Error(err))
	}()

	// ctx 取消或处理结束时关闭连接，解除 Recv 阻塞。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	cs := newConnState()
	var b bin.Buffer
	var replay *bin.Buffer
	for {
		if replay != nil {
			b.ResetTo(replay.Copy())
			replay = nil
		} else {
			// 建立 session 前（current==nil，握手 + 首个加密消息之前）用较短的 handshakeTimeout
			// 快速回收静默的半开 / 异常连接；建立 session 后用 readTimeout（客户端有 ping 心跳）。
			timeout := s.readTimeout
			if current == nil {
				timeout = s.handshakeTimeout
			}
			if err := s.recv(ctx, conn, &b, timeout); err != nil {
				return err
			}
			if s.onFrame != nil {
				s.onFrame(b.Len())
			}
		}

		authKeyID, err := peekAuthKeyID(&b)
		if err != nil {
			return fmt.Errorf("peek auth key id: %w", err)
		}

		if authKeyID == emptyAuthKeyID {
			next, err := s.handleExchange(ctx, conn, &b)
			if err != nil {
				return err
			}
			replay = next
			continue
		}

		data, found, err := s.authKeys.Get(ctx, authKeyID)
		if err != nil {
			return fmt.Errorf("lookup auth key: %w", err)
		}
		if !found {
			if err := s.sendProtoError(ctx, conn, codec.CodeAuthKeyNotFound); err != nil {
				return err
			}
			continue
		}

		current, err = s.handleEncrypted(ctx, conn, cs, current, data, &b)
		if err != nil {
			return err
		}
	}
}

func (s *Server) recv(ctx context.Context, conn transport.Conn, b *bin.Buffer, timeout time.Duration) error {
	b.Reset()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return conn.Recv(ctx, b)
}

// isClientDisconnect 判断错误是否为正常的客户端断开/服务关闭，不应作为异常记录。
func isClientDisconnect(err error) bool {
	switch {
	case errors.Is(err, io.EOF),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}
	var nerr *net.OpError
	if errors.As(err, &nerr) && (nerr.Op == "read" || nerr.Op == "write") {
		return true
	}
	return false
}
