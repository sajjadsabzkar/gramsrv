package rpc

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestStorageFileTypePrefersMagicOverMime(t *testing.T) {
	webp := []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}
	if _, ok := storageFileType("image/jpeg", webp).(*tg.StorageFileWebp); !ok {
		t.Fatalf("webp bytes mislabeled as jpeg should return StorageFileWebp")
	}
}

func TestStorageFileTypeFallsBackToMime(t *testing.T) {
	if _, ok := storageFileType("image/png", nil).(*tg.StorageFilePng); !ok {
		t.Fatalf("png mime without bytes should return StorageFilePng")
	}
}

func TestFileLocationKeyUsesDocumentID(t *testing.T) {
	key, ok := fileLocationKey(&tg.InputDocumentFileLocation{
		ID:        1382305375846410902,
		ThumbSize: "m",
	})
	if !ok {
		t.Fatal("fileLocationKey returned !ok")
	}
	const want = "doc:1382305375846410902:m"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
}
