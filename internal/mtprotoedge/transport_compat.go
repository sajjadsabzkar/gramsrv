package mtprotoedge

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/gotd/td/bin"
	tdcrypto "github.com/gotd/td/crypto"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"
)

const maxTransportMessageSize = 1 << 24
const quickAckResponseFlag = uint32(1 << 31)

type transportListener interface {
	Accept() (transport.Conn, error)
	Close() error
	Addr() net.Addr
}

type quickAckTransport interface {
	ConsumeQuickAckRequested() bool
	SendQuickAck(ctx context.Context, token uint32) error
}

type compatTransportListener struct {
	codec    func() transport.Codec
	listener net.Listener
}

func newCompatTransportListener(codec func() transport.Codec, listener net.Listener) transportListener {
	if codec != nil {
		return transport.ListenCodec(codec, listener)
	}
	return &compatTransportListener{listener: listener}
}

func (l *compatTransportListener) Accept() (_ transport.Conn, rErr error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	defer func() {
		if rErr != nil {
			multierr.AppendInto(&rErr, conn.Close())
		}
	}()

	connCodec, reader, err := detectCompatCodec(conn)
	if err != nil {
		return nil, errors.Wrap(err, "detect codec")
	}

	return &compatTransportConn{
		conn: wrappedCompatConn{
			reader: reader,
			Conn:   conn,
		},
		codec: connCodec,
	}, nil
}

func (l *compatTransportListener) Close() error {
	return l.listener.Close()
}

func (l *compatTransportListener) Addr() net.Addr {
	return l.listener.Addr()
}

type wrappedCompatConn struct {
	reader io.Reader
	net.Conn
}

func (w wrappedCompatConn) Read(p []byte) (int, error) {
	return w.reader.Read(p)
}

type compatTransportConn struct {
	conn  net.Conn
	codec transport.Codec

	readMux  sync.Mutex
	writeMux sync.Mutex
}

func (c *compatTransportConn) Send(ctx context.Context, b *bin.Buffer) error {
	c.writeMux.Lock()
	defer c.writeMux.Unlock()

	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return errors.Wrap(err, "reset write deadline")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
	}

	if err := c.codec.Write(c.conn, b); err != nil {
		return errors.Wrap(err, "write")
	}
	return nil
}

func (c *compatTransportConn) ConsumeQuickAckRequested() bool {
	q, ok := c.codec.(quickAckCodec)
	if !ok {
		return false
	}
	return q.consumeQuickAckRequested()
}

func (c *compatTransportConn) SendQuickAck(ctx context.Context, token uint32) error {
	q, ok := c.codec.(quickAckCodec)
	if !ok {
		return nil
	}

	c.writeMux.Lock()
	defer c.writeMux.Unlock()

	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return errors.Wrap(err, "reset write deadline")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
	}

	raw := q.quickAckResponse(token)
	if err := writeAll(c.conn, raw[:]); err != nil {
		return errors.Wrap(err, "write quick ack")
	}
	return nil
}

func (c *compatTransportConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.readMux.Lock()
	defer c.readMux.Unlock()

	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return errors.Wrap(err, "reset read deadline")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			return errors.Wrap(err, "set read deadline")
		}
	}

	if err := c.codec.Read(c.conn, b); err != nil {
		return errors.Wrap(err, "read")
	}
	return nil
}

func (c *compatTransportConn) Close() error {
	return c.conn.Close()
}

func detectCompatCodec(c io.Reader) (transport.Codec, io.Reader, error) {
	var buf [4]byte
	if _, err := io.ReadFull(c, buf[:1]); err != nil {
		return nil, nil, errors.Wrap(err, "read first byte")
	}

	if buf[0] == codec.AbridgedClientStart[0] {
		return &quickAckAbridgedCodec{}, c, nil
	}

	if _, err := io.ReadFull(c, buf[1:4]); err != nil {
		return nil, nil, errors.Wrap(err, "read header")
	}
	switch buf {
	case codec.IntermediateClientStart:
		return &quickAckIntermediateCodec{}, c, nil
	case codec.PaddedIntermediateClientStart:
		return &quickAckPaddedIntermediateCodec{}, c, nil
	default:
		return transport.Full.Codec(), io.MultiReader(bytes.NewReader(buf[:]), c), nil
	}
}

type quickAckCodec interface {
	transport.Codec
	consumeQuickAckRequested() bool
	quickAckResponse(token uint32) [4]byte
}

type quickAckAbridgedCodec struct {
	quickAckRequested bool
}

func (*quickAckAbridgedCodec) WriteHeader(w io.Writer) error {
	return (codec.Abridged{}).WriteHeader(w)
}

func (*quickAckAbridgedCodec) ReadHeader(r io.Reader) error {
	return (codec.Abridged{}).ReadHeader(r)
}

func (*quickAckAbridgedCodec) Write(w io.Writer, b *bin.Buffer) error {
	if err := validateOutgoingCompatMessage(b); err != nil {
		return err
	}

	words := b.Len() >> 2
	var header [4]byte
	headerLen := 1
	if words < 0x7f {
		header[0] = byte(words)
	} else {
		header[0] = 0x7f
		header[1] = byte(words)
		header[2] = byte(words >> 8)
		header[3] = byte(words >> 16)
		headerLen = 4
	}
	return writeCompatPacket(w, header[:headerLen], b.Raw())
}

func (q *quickAckAbridgedCodec) Read(r io.Reader, b *bin.Buffer) error {
	requested, err := readQuickAckAbridged(r, b)
	if err != nil {
		return errors.Wrap(err, "read abridged")
	}
	q.quickAckRequested = requested
	return checkCompatProtocolError(b)
}

func (q *quickAckAbridgedCodec) consumeQuickAckRequested() bool {
	v := q.quickAckRequested
	q.quickAckRequested = false
	return v
}

func (*quickAckAbridgedCodec) quickAckResponse(token uint32) [4]byte {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], (token&^quickAckResponseFlag)|quickAckResponseFlag)
	return raw
}

type quickAckIntermediateCodec struct {
	quickAckRequested bool
}

func (*quickAckIntermediateCodec) WriteHeader(w io.Writer) error {
	return (codec.Intermediate{}).WriteHeader(w)
}

func (*quickAckIntermediateCodec) ReadHeader(r io.Reader) error {
	return (codec.Intermediate{}).ReadHeader(r)
}

func (*quickAckIntermediateCodec) Write(w io.Writer, b *bin.Buffer) error {
	if err := validateOutgoingCompatMessage(b); err != nil {
		return err
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(b.Len()))
	return writeCompatPacket(w, header[:], b.Raw())
}

func (q *quickAckIntermediateCodec) Read(r io.Reader, b *bin.Buffer) error {
	requested, err := readQuickAckIntermediate(r, b, false)
	if err != nil {
		return errors.Wrap(err, "read intermediate")
	}
	q.quickAckRequested = requested
	return checkCompatProtocolError(b)
}

func (q *quickAckIntermediateCodec) consumeQuickAckRequested() bool {
	v := q.quickAckRequested
	q.quickAckRequested = false
	return v
}

func (*quickAckIntermediateCodec) quickAckResponse(token uint32) [4]byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], (token&^quickAckResponseFlag)|quickAckResponseFlag)
	return raw
}

type quickAckPaddedIntermediateCodec struct {
	quickAckRequested bool
}

func (*quickAckPaddedIntermediateCodec) WriteHeader(w io.Writer) error {
	return (codec.PaddedIntermediate{}).WriteHeader(w)
}

func (*quickAckPaddedIntermediateCodec) ReadHeader(r io.Reader) error {
	return (codec.PaddedIntermediate{}).ReadHeader(r)
}

func (*quickAckPaddedIntermediateCodec) Write(w io.Writer, b *bin.Buffer) error {
	if err := validateOutgoingCompatMessage(b); err != nil {
		return err
	}
	var padding [4]byte
	if _, err := io.ReadFull(tdcrypto.DefaultRand(), padding[:]); err != nil {
		return err
	}
	n := int(padding[0] % 4)
	payload := b.Raw()
	if n > 0 {
		payload = append(append([]byte(nil), payload...), padding[:n]...)
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	return writeCompatPacket(w, header[:], payload)
}

func (q *quickAckPaddedIntermediateCodec) Read(r io.Reader, b *bin.Buffer) error {
	requested, err := readQuickAckIntermediate(r, b, true)
	if err != nil {
		return errors.Wrap(err, "read padded intermediate")
	}
	q.quickAckRequested = requested
	return checkCompatProtocolError(b)
}

func (q *quickAckPaddedIntermediateCodec) consumeQuickAckRequested() bool {
	v := q.quickAckRequested
	q.quickAckRequested = false
	return v
}

func (*quickAckPaddedIntermediateCodec) quickAckResponse(token uint32) [4]byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], (token&^quickAckResponseFlag)|quickAckResponseFlag)
	return raw
}

func readQuickAckAbridged(r io.Reader, b *bin.Buffer) (bool, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return false, err
	}

	requested := first[0]&0x80 != 0
	lengthByte := first[0] & 0x7f
	var n int
	if lengthByte == 0x7f {
		var tail [3]byte
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return false, err
		}
		words := uint32(tail[0]) | uint32(tail[1])<<8 | uint32(tail[2])<<16
		n = int(words << 2)
	} else {
		n = int(lengthByte) << 2
	}

	if err := validateCompatTransportLength(n); err != nil {
		return false, err
	}
	resetCompatBufferN(b, n)
	if _, err := io.ReadFull(r, b.Buf); err != nil {
		return false, errors.Wrap(err, "read payload")
	}
	return requested, nil
}

func readQuickAckIntermediate(r io.Reader, b *bin.Buffer, padding bool) (bool, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return false, errors.Wrap(err, "read length")
	}
	rawLength := binary.LittleEndian.Uint32(lengthBuf[:])
	requested := rawLength&quickAckResponseFlag != 0
	n := int(rawLength &^ quickAckResponseFlag)
	if err := validateCompatTransportLength(n); err != nil {
		return false, err
	}
	resetCompatBufferN(b, n)
	if _, err := io.ReadFull(r, b.Buf); err != nil {
		return false, errors.Wrap(err, "read payload")
	}
	if padding {
		paddingLength := n % 4
		b.Buf = b.Buf[:n-paddingLength]
	}
	return requested, nil
}

func validateOutgoingCompatMessage(b *bin.Buffer) error {
	n := b.Len()
	if err := validateCompatTransportLength(n); err != nil {
		return err
	}
	if n%bin.Word != 0 {
		return fmt.Errorf("invalid message length %d: not aligned to %d", n, bin.Word)
	}
	return nil
}

func writeCompatPacket(w io.Writer, header, payload []byte) error {
	packet := make([]byte, len(header)+len(payload))
	copy(packet, header)
	copy(packet[len(header):], payload)
	return writeAll(w, packet)
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func validateCompatTransportLength(n int) error {
	if n <= 0 || n > maxTransportMessageSize {
		return fmt.Errorf("invalid message length %d", n)
	}
	return nil
}

func resetCompatBufferN(b *bin.Buffer, n int) {
	if cap(b.Buf) < n {
		b.Buf = make([]byte, n)
		return
	}
	b.Buf = b.Buf[:n]
}

func checkCompatProtocolError(b *bin.Buffer) error {
	if b.Len() != bin.Word {
		return nil
	}
	code, err := b.Int32()
	if err != nil {
		return err
	}
	return &codec.ProtocolErr{Code: -code}
}
