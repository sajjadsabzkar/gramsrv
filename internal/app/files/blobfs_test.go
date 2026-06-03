package files

import (
	"bytes"
	"context"
	"testing"
)

func TestLocalFSPutGetRoundTrip(t *testing.T) {
	fs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs: %v", err)
	}
	ctx := context.Background()
	data := []byte("hello telesrv media blob 你好")

	key, err := fs.Put(ctx, data)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if key == "" {
		t.Fatal("empty object key")
	}

	// 内容寻址：相同内容应得到相同 key（去重）。
	key2, err := fs.Put(ctx, data)
	if err != nil {
		t.Fatalf("put again: %v", err)
	}
	if key != key2 {
		t.Fatalf("expected dedup key %q == %q", key, key2)
	}

	got, err := fs.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, data)
	}
}

func TestLocalFSDistinctContent(t *testing.T) {
	fs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs: %v", err)
	}
	ctx := context.Background()
	k1, _ := fs.Put(ctx, []byte("aaa"))
	k2, _ := fs.Put(ctx, []byte("bbb"))
	if k1 == k2 {
		t.Fatal("distinct content must yield distinct keys")
	}
}

func TestLocalFSGetRange(t *testing.T) {
	fs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs: %v", err)
	}
	ctx := context.Background()
	key, err := fs.Put(ctx, []byte("0123456789"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	cases := []struct {
		name          string
		offset, limit int64
		want          string
	}{
		{"head", 0, 4, "0123"},
		{"middle", 3, 4, "3456"},
		{"limit-exceeds-remaining", 7, 100, "789"},
		{"zero-limit-reads-to-end", 2, 0, "23456789"},
		{"offset-at-end", 10, 5, ""},
		{"offset-past-end", 20, 5, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, total, err := fs.GetRange(ctx, key, tc.offset, tc.limit)
			if err != nil {
				t.Fatalf("getrange: %v", err)
			}
			if string(data) != tc.want {
				t.Errorf("data = %q, want %q", data, tc.want)
			}
			if total != 10 {
				t.Errorf("total = %d, want 10", total)
			}
		})
	}
}
