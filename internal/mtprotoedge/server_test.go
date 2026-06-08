package mtprotoedge

import (
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproxy"
	"github.com/gotd/td/mtproxy/obfuscator"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"
)

// TestServerAcceptAndCodec 验证 M0：
// server 能接受连接、自动协商 codec、读到客户端帧，并在 ctx 取消时优雅退出。
func TestServerAcceptAndCodec(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	frames := make(chan int, 1)
	srv := New(Options{Logger: zaptest.NewLogger(t)})
	srv.onFrame = func(n int) {
		select {
		case frames <- n:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, ln) }()

	// 客户端：TCP 拨号 + intermediate 协议握手 + 发送一帧。
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn, err := transport.Intermediate.Handshake(raw)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}

	// payload 必须 ≠ 4 字节：codec 把恰好 4 字节的帧当作 transport 协议错误码（checkProtocolError）。
	// 真实 MTProto 帧远大于 4 字节，这里发 8 字节模拟一个普通帧。
	var b bin.Buffer
	b.PutInt32(0x12345678)
	b.PutInt32(0x0badf00d)
	sendCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	if err := conn.Send(sendCtx, &b); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case n := <-frames:
		if n <= 0 {
			t.Fatalf("received empty frame, len = %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive frame in time")
	}

	_ = conn.Close()

	// 验证优雅退出。
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after ctx cancel")
	}
}

// TestServerAcceptObfuscatedAbridged 验证 TDesktop tcpo_only 连接形态：
// 先做 MTProto TCP obfuscation，再在解密后的流上使用 abridged codec。
func TestServerAcceptObfuscatedAbridged(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	frames := make(chan int, 1)
	srv := New(Options{Logger: zaptest.NewLogger(t), ObfuscatedTCP: true})
	srv.onFrame = func(n int) {
		select {
		case frames <- n:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, ln) }()

	bad, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("bad dial: %v", err)
	}
	_ = bad.Close()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-serveErr:
		t.Fatalf("server stopped after bad obfuscated accept: %v", err)
	default:
	}

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	obfs := obfuscator.Obfuscated2(rand.Reader, raw)
	if err := obfs.Handshake((codec.Abridged{}).ObfuscatedTag(), 2, mtproxy.Secret{}); err != nil {
		t.Fatalf("obfuscated handshake: %v", err)
	}
	conn, err := transport.NewProtocol(func() transport.Codec {
		return transport.Abridged.CodecNoHeader()
	}).Handshake(obfs)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}

	var b bin.Buffer
	b.PutInt32(0x12345678)
	b.PutInt32(0x0badf00d)
	sendCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	defer sc()
	if err := conn.Send(sendCtx, &b); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case n := <-frames:
		if n <= 0 {
			t.Fatalf("received empty frame, len = %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive frame in time")
	}

	_ = conn.Close()

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after ctx cancel")
	}
}

func TestServerAcceptObfuscatedAbridgedQuickAckFrame(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	frames := make(chan int, 1)
	srv := New(Options{Logger: zaptest.NewLogger(t), ObfuscatedTCP: true})
	srv.onFrame = func(n int) {
		select {
		case frames <- n:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx, ln) }()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	obfs := obfuscator.Obfuscated2(rand.Reader, raw)
	if err := obfs.Handshake((codec.Abridged{}).ObfuscatedTag(), 2, mtproxy.Secret{}); err != nil {
		t.Fatalf("obfuscated handshake: %v", err)
	}

	var b bin.Buffer
	b.PutInt32(0x12345678)
	b.PutInt32(0x0badf00d)
	packet := append([]byte{0x80 | byte(b.Len()/4)}, b.Raw()...)
	if _, err := obfs.Write(packet); err != nil {
		t.Fatalf("write quick ack frame: %v", err)
	}

	select {
	case n := <-frames:
		if n != b.Len() {
			t.Fatalf("received frame len = %d, want %d", n, b.Len())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive quick-ack abridged frame in time")
	}

	_ = raw.Close()

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after ctx cancel")
	}
}
