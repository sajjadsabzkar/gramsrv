package langpack

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SeedDirectory 将导出的 .strings 文件导入 LangPackStore。
// root 可直接指向 data/langpack，也可指向包含 .strings 的具体平台目录。
func (s *Service) SeedDirectory(ctx context.Context, root string) (int, error) {
	if s == nil || s.packs == nil || root == "" {
		return 0, nil
	}
	dir := filepath.Clean(root)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("stat langpack seed dir: %w", err)
	}
	tdesktopDir := filepath.Join(dir, "tdesktop")
	if info, err := os.Stat(tdesktopDir); err == nil && info.IsDir() {
		dir = tdesktopDir
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read langpack seed dir: %w", err)
	}
	seeded := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".strings") {
			continue
		}
		pack, err := ParseTDesktopFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return seeded, err
		}
		existing, err := s.packs.GetPack(ctx, pack.LangPack, pack.LangCode, pack.Version)
		if err != nil {
			return seeded, err
		}
		if existing.Version >= pack.Version {
			continue
		}
		if err := s.packs.UpsertPack(ctx, pack); err != nil {
			return seeded, err
		}
		seeded += len(pack.Strings)
	}
	return seeded, nil
}
