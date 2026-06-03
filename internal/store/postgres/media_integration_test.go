package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
)

// TestMediaStoreRoundTrip 验证 MediaStore 各表的写读往返（含 nil bytea 归一、JSONB attributes/sizes、
// 头像历史 current/list/delete、上传分片）。直接证明媒体元数据落 PG 后可原样读回。
func TestMediaStoreRoundTrip(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	s := NewMediaStore(pool)

	const docID = int64(9100000000000000001)
	const photoID = int64(9100000000000000002)
	const setID = int64(9100000000000000003)
	const ownerID = int64(9100000000000000099)
	const reactionEmoji = "\U0001f9ea"

	cleanupMediaStoreRoundTripRows(t, ctx, pool)
	t.Cleanup(func() {
		cleanupMediaStoreRoundTripRows(t, context.Background(), pool)
	})

	// ---- file blob（nil sha256 应被归一为空，不报 NOT NULL）----
	if err := s.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: "doc:9100000000000000001",
		ObjectKey:   "ab/cd/abcdef",
		Size:        1234,
		MimeType:    "application/x-tgsticker",
	}); err != nil {
		t.Fatalf("put file blob (nil sha256): %v", err)
	}
	blob, ok, err := s.GetFileBlob(ctx, "doc:9100000000000000001")
	if err != nil || !ok {
		t.Fatalf("get file blob: ok=%v err=%v", ok, err)
	}
	if blob.ObjectKey != "ab/cd/abcdef" || blob.Size != 1234 || blob.Backend != domain.MediaBackendLocalFS {
		t.Fatalf("file blob mismatch: %+v", blob)
	}

	// ---- document（含 sticker 属性 + thumbs JSONB；nil file_reference 路径）----
	doc := domain.Document{
		ID:         docID,
		AccessHash: 77,
		DCID:       2,
		MimeType:   "application/x-tgsticker",
		Size:       2048,
		Attributes: []domain.DocumentAttribute{
			{Kind: domain.DocAttrImageSize, W: 512, H: 512},
			{Kind: domain.DocAttrSticker, Alt: "\U0001f600", StickerSetID: setID, StickerSetAccessHash: 5},
		},
		Thumbs: []domain.PhotoSize{{Kind: domain.PhotoSizeKindPath, Type: "j", Bytes: []byte{1, 2, 3}}},
	}
	if err := s.PutDocument(ctx, doc); err != nil {
		t.Fatalf("put document: %v", err)
	}
	got, ok, err := s.GetDocument(ctx, docID)
	if err != nil || !ok {
		t.Fatalf("get document: ok=%v err=%v", ok, err)
	}
	if got.DCID != 2 || len(got.Attributes) != 2 || len(got.Thumbs) != 1 {
		t.Fatalf("document mismatch: %+v", got)
	}
	if id, hash, ok := got.StickerSetRef(); !ok || id != setID || hash != 5 {
		t.Fatalf("document sticker set ref = (%d,%d,%v)", id, hash, ok)
	}
	docs, err := s.GetDocuments(ctx, []int64{docID})
	if err != nil || len(docs) != 1 {
		t.Fatalf("get documents: n=%d err=%v", len(docs), err)
	}

	// ---- photo（sizes JSONB）----
	photo := domain.Photo{
		ID:         photoID,
		AccessHash: 88,
		DCID:       2,
		Sizes:      []domain.PhotoSize{{Kind: domain.PhotoSizeKindDefault, Type: "x", W: 800, H: 600, Size: 4096}},
	}
	if err := s.PutPhoto(ctx, photo); err != nil {
		t.Fatalf("put photo: %v", err)
	}
	gotPhoto, ok, err := s.GetPhoto(ctx, photoID)
	if err != nil || !ok || len(gotPhoto.Sizes) != 1 || gotPhoto.Sizes[0].Type != "x" {
		t.Fatalf("get photo mismatch: ok=%v err=%v photo=%+v", ok, err, gotPhoto)
	}

	// ---- sticker set ----
	set := domain.StickerSet{
		ID:          setID,
		AccessHash:  5,
		ShortName:   "telesrv_test_set_9100000000000000003",
		Title:       "Test Set",
		Count:       1,
		Kind:        domain.StickerSetKindStickers,
		Animated:    true,
		Installed:   true,
		DocumentIDs: []int64{docID},
		Packs:       []domain.StickerPack{{Emoticon: "\U0001f600", DocumentIDs: []int64{docID}}},
		SystemKey:   "test_system_9100000000000000003",
	}
	if err := s.PutStickerSet(ctx, set); err != nil {
		t.Fatalf("put sticker set: %v", err)
	}
	byID, ok, err := s.GetStickerSetByID(ctx, setID)
	if err != nil || !ok || len(byID.DocumentIDs) != 1 || len(byID.Packs) != 1 {
		t.Fatalf("get sticker set by id: ok=%v err=%v set=%+v", ok, err, byID)
	}
	if byShort, ok, _ := s.GetStickerSetByShortName(ctx, set.ShortName); !ok || byShort.ID != setID {
		t.Fatalf("get sticker set by short name failed: ok=%v", ok)
	}
	if bySys, ok, _ := s.GetStickerSetBySystemKey(ctx, set.SystemKey); !ok || bySys.ID != setID {
		t.Fatalf("get sticker set by system key failed: ok=%v", ok)
	}

	// ---- available reaction ----
	if err := s.PutAvailableReaction(ctx, domain.AvailableReaction{
		Reaction: reactionEmoji, Title: "Test", StaticIconID: docID, SelectAnimationID: docID, Order: 9999,
	}); err != nil {
		t.Fatalf("put available reaction: %v", err)
	}
	reactions, err := s.ListAvailableReactions(ctx)
	if err != nil {
		t.Fatalf("list available reactions: %v", err)
	}
	foundReaction := false
	for _, r := range reactions {
		if r.Reaction == reactionEmoji {
			foundReaction = true
			if r.StaticIconID != docID {
				t.Fatalf("reaction static icon id = %d", r.StaticIconID)
			}
		}
	}
	if !foundReaction {
		t.Fatal("inserted reaction not found in list")
	}

	// ---- profile photo 历史 ----
	if err := s.AddProfilePhoto(ctx, domain.PeerTypeUser, ownerID, photoID, 1700000000); err != nil {
		t.Fatalf("add profile photo: %v", err)
	}
	cur, ok, err := s.CurrentProfilePhoto(ctx, domain.PeerTypeUser, ownerID)
	if err != nil || !ok || cur != photoID {
		t.Fatalf("current profile photo = (%d,%v,%v)", cur, ok, err)
	}
	refs, err := s.CurrentProfilePhotos(ctx, domain.PeerTypeUser, []int64{ownerID})
	if err != nil || refs[ownerID].PhotoID != photoID || refs[ownerID].DCID != 2 {
		t.Fatalf("current profile photos batch = %+v err=%v", refs, err)
	}
	ids, total, err := s.ListProfilePhotos(ctx, domain.PeerTypeUser, ownerID, 0, 10, 0)
	if err != nil || total < 1 || len(ids) < 1 {
		t.Fatalf("list profile photos: ids=%v total=%d err=%v", ids, total, err)
	}
	deleted, err := s.DeleteProfilePhotos(ctx, domain.PeerTypeUser, ownerID, []int64{photoID})
	if err != nil || len(deleted) != 1 {
		t.Fatalf("delete profile photos: deleted=%v err=%v", deleted, err)
	}
	if _, ok, _ := s.CurrentProfilePhoto(ctx, domain.PeerTypeUser, ownerID); ok {
		t.Fatal("profile photo still current after delete")
	}

	// ---- upload parts ----
	if err := s.SaveFilePart(ctx, domain.UploadPart{OwnerUserID: ownerID, FileID: 555, Part: 0, Bytes: []byte("hello")}); err != nil {
		t.Fatalf("save file part: %v", err)
	}
	parts, err := s.LoadFileParts(ctx, ownerID, 555)
	if err != nil || len(parts) != 1 || string(parts[0].Bytes) != "hello" {
		t.Fatalf("load file parts: parts=%+v err=%v", parts, err)
	}
	if err := s.DeleteFileParts(ctx, ownerID, 555); err != nil {
		t.Fatalf("delete file parts: %v", err)
	}
	if parts, _ := s.LoadFileParts(ctx, ownerID, 555); len(parts) != 0 {
		t.Fatal("file parts not cleared")
	}
}

func cleanupMediaStoreRoundTripRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	const docID = int64(9100000000000000001)
	const photoID = int64(9100000000000000002)
	const setID = int64(9100000000000000003)
	const ownerID = int64(9100000000000000099)
	const reactionEmoji = "\U0001f9ea"

	statements := []struct {
		sql  string
		args []any
	}{
		{sql: "DELETE FROM upload_parts WHERE owner_user_id = $1 AND file_id = 555", args: []any{ownerID}},
		{sql: "DELETE FROM profile_photos WHERE owner_peer_type = 'user' AND owner_peer_id = $1 AND photo_id = $2", args: []any{ownerID, photoID}},
		{sql: "DELETE FROM available_reactions WHERE reaction IN ($1, 'telesrv-test-😀')", args: []any{reactionEmoji}},
		{sql: "DELETE FROM sticker_sets WHERE id = $1 OR short_name = 'telesrv_test_set_9100000000000000003' OR system_key = 'test_system_9100000000000000003'", args: []any{setID}},
		{sql: "DELETE FROM file_blobs WHERE location_key = 'doc:9100000000000000001'"},
		{sql: "DELETE FROM documents WHERE id = $1", args: []any{docID}},
		{sql: "DELETE FROM photos WHERE id = $1", args: []any{photoID}},
	}
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("cleanup media store round trip rows: %v", err)
		}
	}
}
