package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// BlobBackend 是 blob 字节内容的存储后端。第一阶段只有本地磁盘实现。
// 内容寻址：Put 返回的 objectKey 是内容 sha256，相同内容自动去重。
type BlobBackend interface {
	Name() string
	Put(ctx context.Context, data []byte) (objectKey string, err error)
	Get(ctx context.Context, objectKey string) ([]byte, error)
	// GetRange 只读 [offset, offset+limit) 段并返回该段字节与文件总大小（limit<=0 读到末尾），
	// 避免大文件每个 chunk 都整文件读入内存（getFile 按 chunk 多次请求 ⇒ 否则 O(N²) 放大）。
	GetRange(ctx context.Context, objectKey string, offset, limit int64) (data []byte, total int64, err error)
}

// LocalFS 把 blob 字节存到本地磁盘根目录下，路径按内容 hash 两级 fanout。
type LocalFS struct {
	root string
}

// NewLocalFS 创建本地磁盘 blob backend，确保根目录存在。
func NewLocalFS(root string) (*LocalFS, error) {
	if root == "" {
		return nil, fmt.Errorf("blob root dir is empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create blob root %q: %w", root, err)
	}
	return &LocalFS{root: root}, nil
}

// Name 返回后端标识，与 file_blobs.backend 一致。
func (l *LocalFS) Name() string { return "localfs" }

func (l *LocalFS) pathFor(objectKey string) string {
	if len(objectKey) < 4 {
		return filepath.Join(l.root, "_", objectKey)
	}
	return filepath.Join(l.root, objectKey[:2], objectKey[2:4], objectKey)
}

// Put 写入内容并返回 sha256 hex 作为 objectKey；同内容已存在则跳过写入（去重）。
func (l *LocalFS) Put(_ context.Context, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	key := hex.EncodeToString(sum[:])
	path := l.pathFor(key)
	if _, err := os.Stat(path); err == nil {
		return key, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create blob dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", fmt.Errorf("write blob: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("commit blob: %w", err)
	}
	return key, nil
}

// Get 读取 objectKey 对应的全部字节。
func (l *LocalFS) Get(_ context.Context, objectKey string) ([]byte, error) {
	return os.ReadFile(l.pathFor(objectKey))
}

// GetRange 用 ReadAt 只读 [offset, offset+limit) 段，total 取自文件大小；
// n 受 total 约束，故即便客户端传超大 limit 也只分配文件实际大小，不会按客户端巨值分配。
func (l *LocalFS) GetRange(_ context.Context, objectKey string, offset, limit int64) ([]byte, int64, error) {
	f, err := os.Open(l.pathFor(objectKey))
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	total := info.Size()
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []byte{}, total, nil
	}
	n := total - offset
	if limit > 0 && limit < n {
		n = limit
	}
	buf := make([]byte, n)
	read, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, 0, err
	}
	return buf[:read], total, nil
}
