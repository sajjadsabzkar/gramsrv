package mtprotoedge

import (
	"context"
	"crypto/aes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gotd/ige"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
)

var (
	// ErrConnClosed 表示连接的出站 actor 已关闭。
	ErrConnClosed = errors.New("mtproto connection closed")
	// ErrOutboundQueueFull 表示 best-effort update push 未能在预算内进入出站队列。
	ErrOutboundQueueFull = errors.New("mtproto outbound queue full")
)

const (
	maxOutboundQueue       = 1024
	maxTrackedServerMsgIDs = 4096
	maxTrackedAckedMsgIDs  = 1024
	// maxTrackedServerBytes 是 pending（已发送待 ack、用于 resend）总 body 字节上限。
	// 与 maxTrackedServerMsgIDs 并列：客户端从不 ack 时，大响应体按字节滚动丢弃，
	// 防 pending 被「4096 条 × 大 body」撑爆。
	maxTrackedServerBytes = 64 << 20 // 64 MiB
)

type outboundOpKind byte

const (
	outboundSend outboundOpKind = iota + 1
	outboundAck
	outboundQueryState
	outboundResend
	outboundResendByRequest
)

type outboundOp struct {
	kind       outboundOpKind
	control    bool
	ctx        context.Context
	msgType    proto.MessageType
	msg        bin.Encoder
	encoded    *encodedOutboundMessage
	ids        []int64
	reqMsgID   int64
	enqueuedAt time.Time
	done       chan outboundResult
}

type encodedOutboundMessage struct {
	body     []byte
	typeID   uint32
	reqMsgID int64
}

type outboundResult struct {
	info   []byte
	resent bool
	err    error
}

type outboundFrame struct {
	msgID    int64
	seqNo    int32
	typeID   uint32
	body     []byte
	reqMsgID int64
	sentAt   time.Time
	sends    int
}

type outboundState struct {
	pending    map[int64]*outboundFrame
	order      []int64
	byRequest  map[int64]int64
	acked      map[int64]struct{}
	ackOrder   []int64
	totalBytes int
}

func newOutboundState() *outboundState {
	return &outboundState{
		pending:   make(map[int64]*outboundFrame),
		byRequest: make(map[int64]int64),
		acked:     make(map[int64]struct{}),
	}
}

func (c *Conn) startOutbound() {
	if c.metrics == nil {
		c.metrics = NopMetrics{}
	}
	c.outbound = make(chan outboundOp, maxOutboundQueue)
	c.outboundControl = make(chan outboundOp, maxOutboundQueue/4)
	c.outboundStop = make(chan struct{})
	c.outboundDone = make(chan struct{})
	go c.outboundLoop()
}

// Close 停止连接的出站 actor。它不关闭底层 transport；transport 生命周期仍由 serveConn 管理。
func (c *Conn) Close() {
	c.closeInboundRPCScheduler()
	c.outboundClose.Do(func() {
		if c.outboundStop != nil {
			close(c.outboundStop)
			<-c.outboundDone
		}
	})
}

// Send 加密并发送一条 server 消息。
func (c *Conn) Send(ctx context.Context, t proto.MessageType, msg bin.Encoder) error {
	return c.send(ctx, t, msg, false)
}

// SendPriority 加密并优先发送一条 server 控制消息。
func (c *Conn) SendPriority(ctx context.Context, t proto.MessageType, msg bin.Encoder) error {
	return c.send(ctx, t, msg, true)
}

// SendBestEffort 只等待消息进入普通 outbound 队列，不等待网络写完成。
// 用于 updates fanout：队列拥塞时返回 ErrOutboundQueueFull，durable outbox/getDifference 负责兜底。
func (c *Conn) SendBestEffort(ctx context.Context, t proto.MessageType, msg bin.Encoder, timeout time.Duration) error {
	return c.sendBestEffort(ctx, t, msg, nil, timeout)
}

func (c *Conn) SendBestEffortEncoded(ctx context.Context, t proto.MessageType, encoded *encodedOutboundMessage, timeout time.Duration) error {
	return c.sendBestEffort(ctx, t, nil, encoded, timeout)
}

func (c *Conn) sendBestEffort(ctx context.Context, t proto.MessageType, msg bin.Encoder, encoded *encodedOutboundMessage, timeout time.Duration) error {
	if c.outbound == nil || c.outboundControl == nil {
		return ErrConnClosed
	}
	writeCtx := context.Background()
	if ctx != nil {
		writeCtx = context.WithoutCancel(ctx)
	}
	op := outboundOp{
		kind:       outboundSend,
		ctx:        writeCtx,
		msgType:    t,
		msg:        msg,
		encoded:    encoded,
		enqueuedAt: time.Now(),
	}
	if timeout == 0 {
		select {
		case c.outbound <- op:
			return nil
		case <-c.outboundStop:
			return ErrConnClosed
		default:
			c.metrics.OutboundDropped("push_queue_full")
			return ErrOutboundQueueFull
		}
	}
	enqueueCtx := ctx
	if enqueueCtx == nil {
		enqueueCtx = context.Background()
	}
	var cancel context.CancelFunc
	if timeout > 0 {
		enqueueCtx, cancel = context.WithTimeout(enqueueCtx, timeout)
		defer cancel()
	}
	if err := c.enqueueOutbound(enqueueCtx, op); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && timeout > 0 {
			c.metrics.OutboundDropped("push_queue_timeout")
			return ErrOutboundQueueFull
		}
		return err
	}
	return nil
}

func (c *Conn) send(ctx context.Context, t proto.MessageType, msg bin.Encoder, control bool) error {
	return c.sendOutbound(ctx, t, msg, nil, control)
}

func (c *Conn) SendEncoded(ctx context.Context, t proto.MessageType, encoded *encodedOutboundMessage) error {
	return c.sendOutbound(ctx, t, nil, encoded, false)
}

func (c *Conn) sendOutbound(ctx context.Context, t proto.MessageType, msg bin.Encoder, encoded *encodedOutboundMessage, control bool) error {
	if c.outbound == nil || c.outboundControl == nil {
		return ErrConnClosed
	}
	op := outboundOp{
		kind:       outboundSend,
		control:    control,
		ctx:        ctx,
		msgType:    t,
		msg:        msg,
		encoded:    encoded,
		enqueuedAt: time.Now(),
		done:       make(chan outboundResult, 1),
	}
	if err := c.enqueueOutbound(ctx, op); err != nil {
		return err
	}
	select {
	case res := <-op.done:
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.outboundStop:
		return ErrConnClosed
	}
}

// SendAsync 入队一条 server 消息但不等待发送结果（fire-and-forget），用于读循环里的控制消息
// （ack/pong/new_session_created/bad_msg/future_salts/state_info）：避免读循环被 outbound 写
// 阻塞而连带卡死。走优先(control)队列保证不被普通 push 拖后；队列满时丢弃并记 metrics——此时
// 连接多已严重拥塞，控制消息丢失由客户端重传 / 读写超时兜底。返回非 nil 仅表示连接已关闭。
func (c *Conn) SendAsync(ctx context.Context, t proto.MessageType, msg bin.Encoder) error {
	if c.outbound == nil || c.outboundControl == nil {
		return ErrConnClosed
	}
	op := outboundOp{
		kind:       outboundSend,
		control:    true,
		ctx:        ctx,
		msgType:    t,
		msg:        msg,
		enqueuedAt: time.Now(),
		// done 为 nil：fire-and-forget，handleOutboundSend 的 finish 对 nil done 安全跳过。
	}
	select {
	case c.outboundControl <- op:
		return nil
	case <-c.outboundStop:
		return ErrConnClosed
	default:
		c.metrics.OutboundDropped("control_queue_full")
		return nil
	}
}

// AckServerMessages 接收客户端 msgs_ack，释放已确认的 server 出站消息。
func (c *Conn) AckServerMessages(ids []int64) {
	if len(ids) == 0 || c.outbound == nil || c.outboundControl == nil {
		return
	}
	copied := append([]int64(nil), ids...)
	op := outboundOp{kind: outboundAck, control: true, ids: copied}
	select {
	case c.outboundControl <- op:
	case <-c.outboundStop:
	default:
		c.metrics.OutboundDropped("ack_queue_full")
	}
}

// OutgoingStateInfo 返回本连接出站消息的状态。返回值中 0 表示无出站侧意见，
// 调用方可继续用入站 connState 兜底。
func (c *Conn) OutgoingStateInfo(ctx context.Context, ids []int64) ([]byte, error) {
	if c.outbound == nil {
		return nil, ErrConnClosed
	}
	op := outboundOp{
		kind:    outboundQueryState,
		control: true,
		ctx:     ctx,
		ids:     append([]int64(nil), ids...),
		done:    make(chan outboundResult, 1),
	}
	if err := c.enqueueOutbound(ctx, op); err != nil {
		return nil, err
	}
	select {
	case res := <-op.done:
		return res.info, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.outboundStop:
		return nil, ErrConnClosed
	}
}

// ResendMessages 重发仍在 outgoing queue 中的 server 消息，并返回对应状态。
func (c *Conn) ResendMessages(ctx context.Context, ids []int64) ([]byte, error) {
	if c.outbound == nil {
		return nil, ErrConnClosed
	}
	op := outboundOp{
		kind:    outboundResend,
		control: true,
		ctx:     ctx,
		ids:     append([]int64(nil), ids...),
		done:    make(chan outboundResult, 1),
	}
	if err := c.enqueueOutbound(ctx, op); err != nil {
		return nil, err
	}
	select {
	case res := <-op.done:
		return res.info, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.outboundStop:
		return nil, ErrConnClosed
	}
}

// ResendByRequest 在重复 RPC 请求到达时，按原 client msg_id 找到并重发已有 rpc_result。
func (c *Conn) ResendByRequest(ctx context.Context, reqMsgID int64) (bool, error) {
	if c.outbound == nil {
		return false, ErrConnClosed
	}
	op := outboundOp{
		kind:     outboundResendByRequest,
		control:  true,
		ctx:      ctx,
		reqMsgID: reqMsgID,
		done:     make(chan outboundResult, 1),
	}
	if err := c.enqueueOutbound(ctx, op); err != nil {
		return false, err
	}
	select {
	case res := <-op.done:
		return res.resent, res.err
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.outboundStop:
		return false, ErrConnClosed
	}
}

func (c *Conn) enqueueOutbound(ctx context.Context, op outboundOp) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q := c.outbound
	if op.control {
		q = c.outboundControl
	}
	select {
	case q <- op:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.outboundStop:
		return ErrConnClosed
	default:
	}
	c.metrics.OutboundQueueWait(len(q), cap(q))
	select {
	case q <- op:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.outboundStop:
		return ErrConnClosed
	}
}

func (c *Conn) outboundLoop() {
	defer close(c.outboundDone)
	state := newOutboundState()
	for {
		select {
		case op := <-c.outboundControl:
			c.handleOutboundOp(state, op)
			continue
		default:
		}
		select {
		case <-c.outboundStop:
			c.drainOutbound()
			return
		case op := <-c.outboundControl:
			c.handleOutboundOp(state, op)
		case op := <-c.outbound:
			c.handleOutboundOp(state, op)
		}
	}
}

func (c *Conn) drainOutbound() {
	for {
		select {
		case op := <-c.outboundControl:
			op.finish(outboundResult{err: ErrConnClosed})
		case op := <-c.outbound:
			op.finish(outboundResult{err: ErrConnClosed})
		default:
			return
		}
	}
}

func (c *Conn) handleOutboundOp(state *outboundState, op outboundOp) {
	switch op.kind {
	case outboundSend:
		c.handleOutboundSend(state, op)
	case outboundAck:
		state.ack(op.ids)
	case outboundQueryState:
		op.finish(outboundResult{info: state.stateInfo(op.ids)})
	case outboundResend:
		info, err := c.handleOutboundResend(state, op.ctx, op.ids)
		op.finish(outboundResult{info: info, err: err})
	case outboundResendByRequest:
		resent, err := c.handleOutboundResendByRequest(state, op.ctx, op.reqMsgID)
		op.finish(outboundResult{resent: resent, err: err})
	default:
		op.finish(outboundResult{err: fmt.Errorf("unknown outbound op %d", op.kind)})
	}
}

func (c *Conn) handleOutboundSend(state *outboundState, op outboundOp) {
	frame, err := c.buildFrame(op.msgType, op.msg, op.encoded)
	if err == nil {
		err = c.writeFrame(op.ctx, frame)
	}
	if err == nil && frameNeedsAck(frame.typeID) {
		if dropped := state.add(frame); dropped > 0 {
			for i := 0; i < dropped; i++ {
				c.metrics.OutboundDropped("tracked_queue_overflow")
			}
		}
	}
	queueWait := time.Since(op.enqueuedAt)
	bytes := 0
	typeID := uint32(0)
	if frame != nil {
		bytes = len(frame.body)
		typeID = frame.typeID
	}
	c.metrics.OutboundSend(typeID, queueWait, bytes, err)
	op.finish(outboundResult{err: err})
}

func (c *Conn) handleOutboundResend(state *outboundState, ctx context.Context, ids []int64) ([]byte, error) {
	info := make([]byte, len(ids))
	resent := 0
	for i, id := range ids {
		if state.isKnown(id) {
			info[i] = msgStateReceived
		}
		frame, ok := state.pending[id]
		if !ok {
			continue
		}
		if err := c.writeFrame(ctx, frame); err != nil {
			c.metrics.OutboundResend(resent, err)
			return info, err
		}
		frame.sentAt = time.Now()
		frame.sends++
		resent++
	}
	c.metrics.OutboundResend(resent, nil)
	return info, nil
}

func (c *Conn) handleOutboundResendByRequest(state *outboundState, ctx context.Context, reqMsgID int64) (bool, error) {
	msgID, ok := state.byRequest[reqMsgID]
	if !ok {
		return false, nil
	}
	frame, ok := state.pending[msgID]
	if !ok {
		return false, nil
	}
	if err := c.writeFrame(ctx, frame); err != nil {
		c.metrics.OutboundResend(0, err)
		return false, err
	}
	frame.sentAt = time.Now()
	frame.sends++
	c.metrics.OutboundResend(1, nil)
	return true, nil
}

func (op outboundOp) finish(res outboundResult) {
	if op.done == nil {
		return
	}
	select {
	case op.done <- res:
	default:
	}
}

func (c *Conn) buildFrame(t proto.MessageType, msg bin.Encoder, encoded *encodedOutboundMessage) (*outboundFrame, error) {
	if encoded == nil {
		var err error
		encoded, err = encodeOutboundMessage(msg)
		if err != nil {
			return nil, err
		}
	}
	content := frameNeedsAck(encoded.typeID)
	msgID := c.msgID.New(t)
	return &outboundFrame{
		msgID:    msgID,
		seqNo:    c.nextSeqNo(content),
		typeID:   encoded.typeID,
		body:     encoded.body,
		reqMsgID: encoded.reqMsgID,
	}, nil
}

func encodeOutboundMessage(msg bin.Encoder) (*encodedOutboundMessage, error) {
	if msg == nil {
		return nil, errors.New("nil outbound message")
	}
	var body bin.Buffer
	if err := msg.Encode(&body); err != nil {
		return nil, fmt.Errorf("encode outbound: %w", err)
	}
	typeID, err := (&bin.Buffer{Buf: body.Raw()}).PeekID()
	if err != nil {
		return nil, fmt.Errorf("peek outbound type id: %w", err)
	}
	return &encodedOutboundMessage{
		typeID:   typeID,
		body:     body.Raw(),
		reqMsgID: outboundRequestMsgID(msg),
	}, nil
}

func (c *Conn) nextSeqNo(content bool) int32 {
	seqNo := c.sentContentMessages * 2
	if content {
		seqNo++
		c.sentContentMessages++
	}
	return seqNo
}

func (c *Conn) writeFrame(ctx context.Context, frame *outboundFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	out, err := c.encryptOutboundFrame(frame)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	sendCtx := ctx
	cancel := func() {}
	if c.writeTimeout > 0 {
		sendCtx, cancel = context.WithTimeout(ctx, c.writeTimeout)
	}
	defer cancel()
	writer := c.writer
	if writer == nil {
		writer = c.transport
	}
	if err := writer.Send(sendCtx, out); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	if frame.sentAt.IsZero() {
		frame.sentAt = time.Now()
		frame.sends = 1
	}
	return nil
}

func (c *Conn) encryptOutboundFrame(frame *outboundFrame) (*bin.Buffer, error) {
	plain := &c.outboundPlain
	plain.Reset()
	plain.PutLong(c.salt)
	plain.PutLong(c.sessionID)
	plain.PutLong(frame.msgID)
	plain.PutInt32(frame.seqNo)
	plain.PutInt32(int32(len(frame.body)))
	plain.Put(frame.body)

	paddingOffset := plain.Len()
	paddingLen := encryptedPaddingLen(paddingOffset)
	growBinBufferLen(plain, paddingOffset+paddingLen)
	if _, err := io.ReadFull(c.cipher.Rand(), plain.Buf[paddingOffset:]); err != nil {
		return nil, err
	}

	msgKey := crypto.MessageKey(c.key.Value, plain.Raw(), crypto.Server)
	key, iv := crypto.Keys(c.key.Value, msgKey, crypto.Server)
	aesBlock, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	wireLen := len(c.key.ID) + len(msgKey) + plain.Len()
	wire := &c.outboundWire
	ensureBinBufferLen(wire, wireLen)
	copy(wire.Buf[:len(c.key.ID)], c.key.ID[:])
	copy(wire.Buf[len(c.key.ID):len(c.key.ID)+len(msgKey)], msgKey[:])
	ige.EncryptBlocks(aesBlock, iv[:], wire.Buf[len(c.key.ID)+len(msgKey):], plain.Raw())
	return wire, nil
}

func encryptedPaddingLen(l int) int {
	return 16 + (16 - (l % 16))
}

func ensureBinBufferLen(b *bin.Buffer, n int) {
	if cap(b.Buf) < n {
		b.Buf = make([]byte, n)
		return
	}
	b.Buf = b.Buf[:n]
}

func growBinBufferLen(b *bin.Buffer, n int) {
	if cap(b.Buf) < n {
		next := make([]byte, n)
		copy(next, b.Buf)
		b.Buf = next
		return
	}
	b.Buf = b.Buf[:n]
}

func frameNeedsAck(typeID uint32) bool {
	switch typeID {
	case mt.MsgsAckTypeID,
		mt.BadMsgNotificationTypeID,
		mt.BadServerSaltTypeID,
		mt.MsgsStateInfoTypeID,
		mt.MsgsAllInfoTypeID,
		mt.MsgDetailedInfoTypeID,
		mt.MsgNewDetailedInfoTypeID,
		proto.MessageContainerTypeID:
		return false
	default:
		return true
	}
}

func outboundRequestMsgID(msg bin.Encoder) int64 {
	switch v := msg.(type) {
	case *proto.Result:
		return v.RequestMessageID
	default:
		return 0
	}
}

func (s *outboundState) add(frame *outboundFrame) int {
	s.pending[frame.msgID] = frame
	s.order = append(s.order, frame.msgID)
	s.totalBytes += len(frame.body)
	if frame.reqMsgID != 0 {
		s.byRequest[frame.reqMsgID] = frame.msgID
	}
	return s.shrinkPending()
}

func (s *outboundState) ack(ids []int64) {
	for _, id := range ids {
		frame, ok := s.pending[id]
		if !ok {
			continue
		}
		delete(s.pending, id)
		s.totalBytes -= len(frame.body)
		if frame.reqMsgID != 0 {
			delete(s.byRequest, frame.reqMsgID)
		}
		s.markAcked(id)
	}
	if len(s.order) > maxTrackedServerMsgIDs*2 {
		s.compactOrder()
	}
}

func (s *outboundState) stateInfo(ids []int64) []byte {
	info := make([]byte, len(ids))
	for i, id := range ids {
		if s.isKnown(id) {
			info[i] = msgStateReceived
		}
	}
	return info
}

func (s *outboundState) isKnown(id int64) bool {
	if _, ok := s.pending[id]; ok {
		return true
	}
	_, ok := s.acked[id]
	return ok
}

func (s *outboundState) markAcked(id int64) {
	if _, ok := s.acked[id]; ok {
		return
	}
	s.acked[id] = struct{}{}
	s.ackOrder = append(s.ackOrder, id)
	for len(s.ackOrder) > maxTrackedAckedMsgIDs {
		oldest := s.ackOrder[0]
		s.ackOrder = s.ackOrder[1:]
		delete(s.acked, oldest)
	}
}

func (s *outboundState) shrinkPending() int {
	dropped := 0
	for (len(s.pending) > maxTrackedServerMsgIDs || s.totalBytes > maxTrackedServerBytes) && len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		frame, ok := s.pending[oldest]
		if !ok {
			continue
		}
		delete(s.pending, oldest)
		s.totalBytes -= len(frame.body)
		if frame.reqMsgID != 0 {
			delete(s.byRequest, frame.reqMsgID)
		}
		dropped++
	}
	return dropped
}

func (s *outboundState) compactOrder() {
	filtered := s.order[:0]
	for _, id := range s.order {
		if _, ok := s.pending[id]; ok {
			filtered = append(filtered, id)
		}
	}
	s.order = filtered
}
