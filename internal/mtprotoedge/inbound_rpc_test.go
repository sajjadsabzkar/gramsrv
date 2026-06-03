package mtprotoedge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestInboundRPCSchedulerBoundsConcurrentWork(t *testing.T) {
	c := &Conn{metrics: NopMetrics{}}
	c.startInboundRPCScheduler(2, 4, time.Second)
	defer c.closeInboundRPCScheduler()

	var active atomic.Int64
	var maxActive atomic.Int64
	var done atomic.Int64
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	task := inboundRPC{
		method: "test.method",
		run: func(ctx context.Context) error {
			cur := active.Add(1)
			for {
				old := maxActive.Load()
				if cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			active.Add(-1)
			done.Add(1)
			return nil
		},
	}

	for i := 0; i < 2; i++ {
		if err := c.enqueueInboundRPC(context.Background(), task); err != nil {
			t.Fatalf("enqueue active task %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for active rpc workers")
		}
	}
	for i := 0; i < 4; i++ {
		if err := c.enqueueInboundRPC(context.Background(), task); err != nil {
			t.Fatalf("enqueue queued task %d: %v", i, err)
		}
	}
	if err := c.enqueueInboundRPC(context.Background(), task); !errors.Is(err, ErrInboundRPCQueueFull) {
		t.Fatalf("enqueue over capacity err = %v, want ErrInboundRPCQueueFull", err)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("max active = %d, want 2", got)
	}

	close(release)
	deadline := time.After(2 * time.Second)
	for done.Load() != 6 {
		select {
		case <-deadline:
			t.Fatalf("done = %d, want 6", done.Load())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
