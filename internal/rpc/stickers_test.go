package rpc

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

func TestStickerSetsCatalogHashMatchesTDesktopFormula(t *testing.T) {
	sets := []domain.StickerSet{
		{ID: 10, Hash: 123},
		{ID: 11, Hash: 456},
	}
	got := stickerSetsCatalogHash(sets)
	const want int64 = 4284229878340
	if got != want {
		t.Fatalf("stickerSetsCatalogHash() = %d, want %d", got, want)
	}
	if old := mediaCatalogHash([]int64{10, 123, 11, 456}); old == got {
		t.Fatalf("test fixture no longer distinguishes old media hash from TDesktop hash: %d", got)
	}
}

func TestMessagesGetAllStickersUsesTDesktopHashForNotModified(t *testing.T) {
	ctx := context.Background()
	files := &fakeFiles{
		sets: map[domain.StickerSetKind][]domain.StickerSet{
			domain.StickerSetKindStickers: {
				{
					ID:         10,
					AccessHash: 100,
					ShortName:  "one",
					Title:      "One",
					Count:      1,
					Hash:       123,
					Installed:  true,
				},
				{
					ID:         11,
					AccessHash: 110,
					ShortName:  "two",
					Title:      "Two",
					Count:      1,
					Hash:       456,
					Installed:  true,
				},
			},
		},
	}
	r := &Router{deps: Deps{Files: files}}

	first, err := r.onMessagesGetAllStickers(ctx, 0)
	if err != nil {
		t.Fatalf("first getAllStickers: %v", err)
	}
	full, ok := first.(*tg.MessagesAllStickers)
	if !ok {
		t.Fatalf("first getAllStickers = %T, want *tg.MessagesAllStickers", first)
	}
	const wantHash int64 = 4284229878340
	if full.Hash != wantHash {
		t.Fatalf("first hash = %d, want %d", full.Hash, wantHash)
	}

	second, err := r.onMessagesGetAllStickers(ctx, full.Hash)
	if err != nil {
		t.Fatalf("second getAllStickers: %v", err)
	}
	if _, ok := second.(*tg.MessagesAllStickersNotModified); !ok {
		t.Fatalf("second getAllStickers = %T, want *tg.MessagesAllStickersNotModified", second)
	}
}

func TestMessagesGetAvailableReactionsNotModified(t *testing.T) {
	ctx := context.Background()
	reactions := []domain.AvailableReaction{
		{
			Reaction:            "👍",
			Title:               "Like",
			StaticIconID:        101,
			AppearAnimationID:   102,
			SelectAnimationID:   103,
			ActivateAnimationID: 104,
			EffectAnimationID:   105,
			AroundAnimationID:   106,
			CenterIconID:        107,
		},
	}
	files := &fakeFiles{reactions: reactions}
	r := &Router{deps: Deps{Files: files}}

	first, err := r.onMessagesGetAvailableReactions(ctx, 0)
	if err != nil {
		t.Fatalf("first getAvailableReactions: %v", err)
	}
	full, ok := first.(*tg.MessagesAvailableReactions)
	if !ok {
		t.Fatalf("first getAvailableReactions = %T, want *tg.MessagesAvailableReactions", first)
	}
	if full.Hash == 0 {
		t.Fatal("first getAvailableReactions returned zero hash")
	}

	second, err := r.onMessagesGetAvailableReactions(ctx, full.Hash)
	if err != nil {
		t.Fatalf("second getAvailableReactions: %v", err)
	}
	if _, ok := second.(*tg.MessagesAvailableReactionsNotModified); !ok {
		t.Fatalf("second getAvailableReactions = %T, want *tg.MessagesAvailableReactionsNotModified", second)
	}
}

func TestTGDocumentCompactsCachedThumbToDownloadableSize(t *testing.T) {
	doc := tgDocument(domain.Document{
		ID:         100,
		AccessHash: 1,
		DCID:       2,
		MimeType:   "application/x-tgsticker",
		Thumbs: []domain.PhotoSize{
			{Kind: domain.PhotoSizeKindCached, Type: "m", W: 128, H: 128, Bytes: []byte("webp")},
		},
	})
	full, ok := doc.(*tg.Document)
	if !ok {
		t.Fatalf("tgDocument = %T, want *tg.Document", doc)
	}
	if len(full.Thumbs) != 1 {
		t.Fatalf("thumbs = %d, want 1", len(full.Thumbs))
	}
	size, ok := full.Thumbs[0].(*tg.PhotoSize)
	if !ok {
		t.Fatalf("thumb = %T, want *tg.PhotoSize", full.Thumbs[0])
	}
	if size.Type != "m" || size.W != 128 || size.H != 128 || size.Size != 4 {
		t.Fatalf("thumb size = %+v, want downloadable m 128x128 size=4", size)
	}
}

func TestTGDocumentUsesDomainDocumentID(t *testing.T) {
	const documentID int64 = 1382305375846410902

	doc := tgDocument(domain.Document{
		ID:         documentID,
		AccessHash: 1,
		DCID:       2,
		MimeType:   "application/x-tgsticker",
	})
	full, ok := doc.(*tg.Document)
	if !ok {
		t.Fatalf("tgDocument = %T, want *tg.Document", doc)
	}
	if full.ID != documentID {
		t.Fatalf("document id = %d, want %d", full.ID, documentID)
	}
}

func TestMessagesGetCustomEmojiDocumentsUsesDomainIDs(t *testing.T) {
	const documentID int64 = 1382305375846410902
	ctx := WithUserID(context.Background(), 1780269504)
	r := &Router{deps: Deps{Files: &fakeFiles{
		docs: map[int64]domain.Document{
			documentID: {
				ID:         documentID,
				AccessHash: 1,
				DCID:       2,
				MimeType:   "application/x-tgsticker",
			},
		},
	}}}

	docs, err := r.onMessagesGetCustomEmojiDocuments(ctx, []int64{documentID})
	if err != nil {
		t.Fatalf("getCustomEmojiDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("docs = %d, want 1", len(docs))
	}
	doc, ok := docs[0].(*tg.Document)
	if !ok {
		t.Fatalf("doc = %T, want *tg.Document", docs[0])
	}
	if doc.ID != documentID {
		t.Fatalf("doc id = %d, want %d", doc.ID, documentID)
	}
}
