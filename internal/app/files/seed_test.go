package files

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"telesrv/internal/domain"
)

// fakeMediaStore 是 store.MediaStore 的内存替身，用于在无 PG 时验证 seed 导入器。
type fakeMediaStore struct {
	mu        sync.Mutex
	blobs     map[string]domain.FileBlob
	docs      map[int64]domain.Document
	photos    map[int64]domain.Photo
	sets      map[int64]domain.StickerSet
	reactions []domain.AvailableReaction
	parts     map[string][]domain.UploadPart
}

func newFakeMediaStore() *fakeMediaStore {
	return &fakeMediaStore{
		blobs:  map[string]domain.FileBlob{},
		docs:   map[int64]domain.Document{},
		photos: map[int64]domain.Photo{},
		sets:   map[int64]domain.StickerSet{},
		parts:  map[string][]domain.UploadPart{},
	}
}

func (f *fakeMediaStore) SaveFilePart(_ context.Context, _ domain.UploadPart) error { return nil }
func (f *fakeMediaStore) LoadFileParts(_ context.Context, _, _ int64) ([]domain.UploadPart, error) {
	return nil, nil
}
func (f *fakeMediaStore) DeleteFileParts(_ context.Context, _, _ int64) error { return nil }

func (f *fakeMediaStore) PutFileBlob(_ context.Context, blob domain.FileBlob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[blob.LocationKey] = blob
	return nil
}
func (f *fakeMediaStore) GetFileBlob(_ context.Context, key string) (domain.FileBlob, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.blobs[key]
	return b, ok, nil
}

func (f *fakeMediaStore) PutDocument(_ context.Context, doc domain.Document) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.docs[doc.ID] = doc
	return nil
}
func (f *fakeMediaStore) GetDocument(_ context.Context, id int64) (domain.Document, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.docs[id]
	return d, ok, nil
}
func (f *fakeMediaStore) GetDocuments(_ context.Context, ids []int64) ([]domain.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.Document, 0, len(ids))
	for _, id := range ids {
		if d, ok := f.docs[id]; ok {
			out = append(out, d)
		}
	}
	return out, nil
}
func (f *fakeMediaStore) PutPhoto(_ context.Context, p domain.Photo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.photos[p.ID] = p
	return nil
}
func (f *fakeMediaStore) GetPhoto(_ context.Context, id int64) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.photos[id]
	return p, ok, nil
}

func (f *fakeMediaStore) PutStickerSet(_ context.Context, set domain.StickerSet) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets[set.ID] = set
	return nil
}
func (f *fakeMediaStore) GetStickerSetByID(_ context.Context, id int64) (domain.StickerSet, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sets[id]
	return s, ok, nil
}
func (f *fakeMediaStore) GetStickerSetByShortName(_ context.Context, name string) (domain.StickerSet, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.sets {
		if s.ShortName == name {
			return s, true, nil
		}
	}
	return domain.StickerSet{}, false, nil
}
func (f *fakeMediaStore) GetStickerSetBySystemKey(_ context.Context, key string) (domain.StickerSet, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.sets {
		if s.SystemKey == key {
			return s, true, nil
		}
	}
	return domain.StickerSet{}, false, nil
}
func (f *fakeMediaStore) ListStickerSets(_ context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.StickerSet
	for _, s := range f.sets {
		if s.Kind == kind {
			out = append(out, s)
		}
	}
	return out, nil
}
func (f *fakeMediaStore) CountStickerSets(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sets), nil
}
func (f *fakeMediaStore) PutAvailableReaction(_ context.Context, r domain.AvailableReaction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, existing := range f.reactions {
		if existing.Reaction == r.Reaction {
			f.reactions[i] = r
			return nil
		}
	}
	f.reactions = append(f.reactions, r)
	return nil
}
func (f *fakeMediaStore) ListAvailableReactions(_ context.Context) ([]domain.AvailableReaction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.AvailableReaction(nil), f.reactions...), nil
}
func (f *fakeMediaStore) CountAvailableReactions(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reactions), nil
}
func (f *fakeMediaStore) AddProfilePhoto(_ context.Context, _ domain.PeerType, _, _ int64, _ int) error {
	return nil
}
func (f *fakeMediaStore) CurrentProfilePhoto(_ context.Context, _ domain.PeerType, _ int64) (int64, bool, error) {
	return 0, false, nil
}
func (f *fakeMediaStore) CurrentProfilePhotos(_ context.Context, _ domain.PeerType, _ []int64) (map[int64]domain.ProfilePhotoRef, error) {
	return map[int64]domain.ProfilePhotoRef{}, nil
}
func (f *fakeMediaStore) ListProfilePhotos(_ context.Context, _ domain.PeerType, _ int64, _, _ int, _ int64) ([]int64, int, error) {
	return nil, 0, nil
}
func (f *fakeMediaStore) DeleteProfilePhotos(_ context.Context, _ domain.PeerType, _ int64, _ []int64) ([]int64, error) {
	return nil, nil
}

func TestSeedMediaRepairsPartialReactionBlobs(t *testing.T) {
	seedDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(seedDir, "telegram_reactions_export", "global_json"), 0o755); err != nil {
		t.Fatal(err)
	}
	reactionsDir := filepath.Join(seedDir, "telegram_reactions_export", "reactions")
	if err := os.MkdirAll(reactionsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := `{"result":{"reactions":[{"reaction":"👍","title":"Like","static_icon":{"id":1111111,"access_hash":1,"file_reference":"","date":"2026-06-03T00:00:00Z","mime_type":"image/webp","size":4,"attributes":[],"thumbs":[]},"select_animation":{"id":2222222,"access_hash":2,"file_reference":"","date":"2026-06-03T00:00:00Z","mime_type":"application/x-tgsticker","size":4,"attributes":[],"thumbs":[]}}]}}`
	if err := os.WriteFile(filepath.Join(seedDir, "telegram_reactions_export", "global_json", "available_reactions_raw.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reactionsDir, "reaction_thumbs_up_sign_static_icon_Like_1111111.webp"), []byte("webp"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reactionsDir, "reaction_thumbs_up_sign_static_icon_Like_1111111_thumb1_PhotoSize_types_72x72.jpg"), []byte("jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reactionsDir, "reaction_select_2222222.tgs"), []byte("tgs!"), 0o644); err != nil {
		t.Fatal(err)
	}

	media := newFakeMediaStore()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(media, blobs, 2)
	if stats, err := svc.SeedMedia(context.Background(), seedDir, 0); err != nil {
		t.Fatalf("initial seed: %v", err)
	} else if stats.Reactions != 1 || stats.Blobs != 2 {
		t.Fatalf("initial stats = %+v, want one reaction and two blobs", stats)
	}
	chunk, ok, err := svc.GetFile(context.Background(), domain.FileDownloadRequest{LocationKey: "doc:2222222", Offset: 0, Limit: 4})
	if err != nil || !ok {
		t.Fatalf("prewarmed getfile ok=%v err=%v", ok, err)
	}
	if string(chunk.Bytes) != "tgs!" {
		t.Fatalf("prewarmed chunk = %q, want tgs!", chunk.Bytes)
	}
	if blobs.getRangeCalls != 0 {
		t.Fatalf("seeded small blob should be served from byte cache, GetRange calls = %d", blobs.getRangeCalls)
	}

	media.mu.Lock()
	delete(media.blobs, "doc:2222222")
	media.mu.Unlock()

	stats, err := svc.SeedMedia(context.Background(), seedDir, 0)
	if err != nil {
		t.Fatalf("repair seed: %v", err)
	}
	if stats.Reactions != 1 || stats.Blobs != 2 || stats.Skipped {
		t.Fatalf("repair stats = %+v, want repair import", stats)
	}
	if _, ok, _ := media.GetFileBlob(context.Background(), "doc:2222222"); !ok {
		t.Fatal("missing reaction blob was not repaired")
	}
	if reactions, _ := media.ListAvailableReactions(context.Background()); len(reactions) != 1 {
		t.Fatalf("reaction upsert duplicated rows: got %d", len(reactions))
	}
}

func TestSeedMediaFromRealExport(t *testing.T) {
	seedDir := os.Getenv("TELESRV_REAL_STICKER_SEED_DIR")
	if seedDir == "" {
		t.Skip("TELESRV_REAL_STICKER_SEED_DIR not set")
	}
	if _, err := os.Stat(seedDir); err != nil {
		t.Skipf("seed dir %s not present: %v", seedDir, err)
	}
	media := newFakeMediaStore()
	blobs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	svc := NewService(media, blobs, 2)
	stats, err := svc.SeedMedia(context.Background(), seedDir, 2)
	if err != nil {
		t.Fatalf("seed media: %v", err)
	}
	t.Logf("seed stats: reactions=%d sets=%d docs=%d blobs=%d", stats.Reactions, stats.StickerSets, stats.Documents, stats.Blobs)
	if stats.Reactions == 0 {
		t.Error("expected reactions imported")
	}
	if stats.StickerSets == 0 {
		t.Error("expected sticker sets imported")
	}
	if stats.Documents == 0 {
		t.Error("expected documents imported")
	}
	if stats.Blobs == 0 {
		t.Error("expected blobs imported")
	}

	// reaction 引用的文档应能被解析回真实 document（带 sticker 属性 + 主体 blob）。
	reactions, _ := media.ListAvailableReactions(context.Background())
	if len(reactions) == 0 {
		t.Fatal("no reactions stored")
	}
	first := reactions[0]
	if first.Reaction == "" {
		t.Error("reaction emoticon empty")
	}
	if first.StaticIconID == 0 || first.SelectAnimationID == 0 {
		t.Error("reaction missing document ids")
	}
	if d, ok, _ := media.GetDocument(context.Background(), first.SelectAnimationID); !ok {
		t.Error("reaction select animation document missing")
	} else {
		if d.ID > seedExternalDocumentIDOffset {
			t.Errorf("reaction document kept external source id: %d", d.ID)
		}
		if d.DCID != 2 {
			t.Errorf("document dc_id not rewritten: %d", d.DCID)
		}
		if _, ok, _ := media.GetFileBlob(context.Background(), blobKeyDoc(d.ID)); !ok {
			t.Errorf("reaction document %d main blob missing", d.ID)
		}
	}

	// 一个常规贴纸集应有 documents 且能按 short_name 解析。
	for _, s := range media.sets {
		for _, thumb := range s.Thumbs {
			if thumb.Downloadable() {
				t.Fatalf("sticker set %s exposes downloadable cover thumb %q without a serviceable blob", s.ShortName, thumb.Type)
			}
		}
	}

	var sample domain.StickerSet
	for _, s := range media.sets {
		if s.Kind == domain.StickerSetKindStickers && len(s.DocumentIDs) > 0 {
			sample = s
			break
		}
	}
	if sample.ID == 0 {
		t.Fatal("no regular sticker set with documents imported")
	}
	if got, ok, _ := media.GetStickerSetByShortName(context.Background(), sample.ShortName); !ok || got.ID != sample.ID {
		t.Error("sticker set not resolvable by short name")
	}
	if doc, ok, _ := media.GetDocument(context.Background(), sample.DocumentIDs[0]); !ok {
		t.Fatalf("sample sticker document %d missing", sample.DocumentIDs[0])
	} else {
		if doc.ID > seedExternalDocumentIDOffset {
			t.Fatalf("sample sticker kept external source id: %d", doc.ID)
		}
		thumb, ok := findCachedThumb(doc.Thumbs)
		if !ok {
			t.Fatalf("sample sticker document thumbs are not inline cached: %+v", doc.Thumbs)
		}
		blob, ok, err := media.GetFileBlob(context.Background(), blobKeyDoc(doc.ID)+":"+thumb.Type)
		if err != nil || !ok {
			t.Fatalf("sample sticker thumb blob ok=%v err=%v", ok, err)
		}
		if want := seedThumbMimeType(thumb.Bytes); blob.MimeType != want {
			t.Fatalf("sample sticker thumb mime = %q, want %q", blob.MimeType, want)
		}
		if hasPathThumb(doc.Thumbs) {
			t.Fatalf("sample sticker document still exposes path thumb together with raster: %+v", doc.Thumbs)
		}
	}
}

func TestSeedDocumentStorageIDNormalizesExternalIDs(t *testing.T) {
	const sourceID int64 = 5382305375846410902
	const want int64 = 1382305375846410902
	if got := seedDocumentStorageID(sourceID); got != want {
		t.Fatalf("seedDocumentStorageID(%d) = %d, want %d", sourceID, got, want)
	}
	if got := seedDocumentStorageID(2222222); got != 2222222 {
		t.Fatalf("small server id changed: %d", got)
	}
}

func TestSeedStickerSetInstalledFlagExcludesSystemSets(t *testing.T) {
	cases := []struct {
		name string
		kind domain.StickerSetKind
		want bool
	}{
		{name: "regular stickers", kind: domain.StickerSetKindStickers, want: true},
		{name: "custom emoji", kind: domain.StickerSetKindEmoji, want: true},
		{name: "masks", kind: domain.StickerSetKindMasks, want: true},
		{name: "system resources", kind: domain.StickerSetKindSystem, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := seedStickerSetInstalled(tc.kind); got != tc.want {
				t.Fatalf("seedStickerSetInstalled(%q) = %v, want %v", tc.kind, got, tc.want)
			}
		})
	}
}

func TestSeedInlineCachedDocumentThumb(t *testing.T) {
	input := domain.PhotoSize{Kind: domain.PhotoSizeKindDefault, Type: "m", W: 128, H: 128, Size: 6400}
	got := seedInlineCachedDocumentThumb(input, []byte("jpeg"))
	if got.Kind != domain.PhotoSizeKindCached {
		t.Fatalf("kind = %q, want cached", got.Kind)
	}
	if got.Size != 0 || string(got.Bytes) != "jpeg" {
		t.Fatalf("cached thumb = %+v, want inline bytes without downloadable size", got)
	}
	large := make([]byte, seedInlineCachedDocumentThumbMaxBytes+1)
	if got := seedInlineCachedDocumentThumb(input, large); got.Kind != domain.PhotoSizeKindDefault || got.Size != input.Size || len(got.Bytes) != 0 {
		t.Fatalf("large thumb = %+v, want unchanged downloadable thumb", got)
	}
}

func TestSeedThumbMimeType(t *testing.T) {
	webp := []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}
	if got := seedThumbMimeType(webp); got != "image/webp" {
		t.Fatalf("webp mime = %q, want image/webp", got)
	}
	jpeg := []byte{0xFF, 0xD8, 0xFF}
	if got := seedThumbMimeType(jpeg); got != "image/jpeg" {
		t.Fatalf("jpeg mime = %q, want image/jpeg", got)
	}
}

func TestSeedPreferRasterDocumentThumbsDropsPathWhenRasterExists(t *testing.T) {
	sizes := []domain.PhotoSize{
		{Kind: domain.PhotoSizeKindPath, Type: "j", Bytes: []byte("path")},
		{Kind: domain.PhotoSizeKindCached, Type: "m", Bytes: []byte("webp")},
	}
	got := seedPreferRasterDocumentThumbs(sizes)
	if hasPathThumb(got) {
		t.Fatalf("path thumb should be dropped when raster exists: %+v", got)
	}
	if !hasCachedThumb(got) {
		t.Fatalf("cached thumb should be kept: %+v", got)
	}

	onlyPath := []domain.PhotoSize{{Kind: domain.PhotoSizeKindPath, Type: "j", Bytes: []byte("path")}}
	if got := seedPreferRasterDocumentThumbs(onlyPath); !hasPathThumb(got) {
		t.Fatalf("path-only thumbs should be kept: %+v", got)
	}
}

func TestDocumentsNeedInlineCachedThumbsDetectsStaleMime(t *testing.T) {
	ctx := context.Background()
	media := newFakeMediaStore()
	webp := []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}
	doc := domain.Document{
		ID: 100,
		Thumbs: []domain.PhotoSize{
			{Kind: domain.PhotoSizeKindCached, Type: "m", Bytes: webp},
		},
	}
	if err := media.PutDocument(ctx, doc); err != nil {
		t.Fatalf("put doc: %v", err)
	}
	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:100:m", MimeType: "image/jpeg"}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	svc := NewService(media, nil, 2)
	stale, err := svc.documentsNeedInlineCachedThumbs(ctx, []int64{doc.ID})
	if err != nil {
		t.Fatalf("documentsNeedInlineCachedThumbs: %v", err)
	}
	if !stale {
		t.Fatal("expected stale mime to require repair")
	}

	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:100:m", MimeType: "image/webp"}); err != nil {
		t.Fatalf("put repaired blob: %v", err)
	}
	stale, err = svc.documentsNeedInlineCachedThumbs(ctx, []int64{doc.ID})
	if err != nil {
		t.Fatalf("documentsNeedInlineCachedThumbs after repair: %v", err)
	}
	if stale {
		t.Fatal("repaired mime should not require repair")
	}
}

func TestDocumentsNeedInlineCachedThumbsDetectsPathWithRaster(t *testing.T) {
	ctx := context.Background()
	media := newFakeMediaStore()
	doc := domain.Document{
		ID: 100,
		Thumbs: []domain.PhotoSize{
			{Kind: domain.PhotoSizeKindPath, Type: "j", Bytes: []byte("path")},
			{Kind: domain.PhotoSizeKindCached, Type: "m", Bytes: []byte("webp")},
		},
	}
	if err := media.PutDocument(ctx, doc); err != nil {
		t.Fatalf("put doc: %v", err)
	}
	svc := NewService(media, nil, 2)
	stale, err := svc.documentsNeedInlineCachedThumbs(ctx, []int64{doc.ID})
	if err != nil {
		t.Fatalf("documentsNeedInlineCachedThumbs: %v", err)
	}
	if !stale {
		t.Fatal("path thumb with raster should require repair")
	}
}

func hasCachedThumb(sizes []domain.PhotoSize) bool {
	_, ok := findCachedThumb(sizes)
	return ok
}

func findCachedThumb(sizes []domain.PhotoSize) (domain.PhotoSize, bool) {
	for _, size := range sizes {
		if size.Kind == domain.PhotoSizeKindCached && len(size.Bytes) > 0 {
			return size, true
		}
	}
	return domain.PhotoSize{}, false
}

func hasPathThumb(sizes []domain.PhotoSize) bool {
	for _, size := range sizes {
		if size.Kind == domain.PhotoSizeKindPath && len(size.Bytes) > 0 {
			return true
		}
	}
	return false
}

func blobKeyDoc(id int64) string {
	return "doc:" + itoa(id)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
