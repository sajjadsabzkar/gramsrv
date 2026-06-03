package rpc

import (
	"context"
	"testing"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/tg"
	"go.uber.org/zap/zaptest"

	appmessages "telesrv/internal/app/messages"
	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

// fakeFiles 是 FilesService 的最小测试替身：贴纸文档可解析，上传图片返回固定 Photo。
type fakeFiles struct {
	docs      map[int64]domain.Document
	photos    map[int64]domain.Photo
	reactions []domain.AvailableReaction
	sets      map[domain.StickerSetKind][]domain.StickerSet
}

func (f *fakeFiles) SaveFilePart(context.Context, int64, int64, int, []byte) (bool, error) {
	return true, nil
}
func (f *fakeFiles) SaveBigFilePart(context.Context, int64, int64, int, int, []byte) (bool, error) {
	return true, nil
}
func (f *fakeFiles) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	return domain.FileChunk{}, false, nil
}
func (f *fakeFiles) ListAvailableReactions(context.Context) ([]domain.AvailableReaction, error) {
	return append([]domain.AvailableReaction(nil), f.reactions...), nil
}
func (f *fakeFiles) GetDocuments(_ context.Context, ids []int64) ([]domain.Document, error) {
	out := make([]domain.Document, 0, len(ids))
	for _, id := range ids {
		if d, ok := f.docs[id]; ok {
			out = append(out, d)
		}
	}
	return out, nil
}
func (f *fakeFiles) ResolveStickerSet(context.Context, domain.StickerSetRef) (domain.StickerSet, []domain.Document, bool, error) {
	return domain.StickerSet{}, nil, false, nil
}
func (f *fakeFiles) ListStickerSets(_ context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error) {
	sets := f.sets[kind]
	return append([]domain.StickerSet(nil), sets...), nil
}
func (f *fakeFiles) CreatePhotoFromUpload(_ context.Context, _ domain.UploadedFileRef) (domain.Photo, error) {
	return domain.Photo{ID: 777, AccessHash: 7, DCID: 2, Sizes: []domain.PhotoSize{{Kind: domain.PhotoSizeKindDefault, Type: "x", W: 800, H: 600}}}, nil
}
func (f *fakeFiles) CreateAvatarFromUpload(_ context.Context, _ domain.UploadedFileRef) (domain.Photo, error) {
	return domain.Photo{ID: 778, AccessHash: 7, DCID: 2, Sizes: []domain.PhotoSize{{Kind: domain.PhotoSizeKindDefault, Type: "a", W: 160, H: 160}, {Kind: domain.PhotoSizeKindDefault, Type: "c", W: 640, H: 640}}}, nil
}
func (f *fakeFiles) CreateDocumentFromUpload(_ context.Context, _ domain.UploadedFileRef, spec domain.DocumentSpec) (domain.Document, error) {
	return domain.Document{ID: 888, AccessHash: 8, DCID: 2, MimeType: spec.MimeType, Attributes: spec.Attributes}, nil
}
func (f *fakeFiles) GetPhoto(_ context.Context, id int64) (domain.Photo, bool, error) {
	p, ok := f.photos[id]
	return p, ok, nil
}
func (f *fakeFiles) GetDocument(_ context.Context, id int64) (domain.Document, bool, error) {
	d, ok := f.docs[id]
	return d, ok, nil
}
func (f *fakeFiles) UploadProfilePhoto(context.Context, domain.PeerType, int64, domain.UploadedFileRef, int) (domain.Photo, error) {
	return domain.Photo{}, nil
}
func (f *fakeFiles) SetCurrentProfilePhoto(context.Context, domain.PeerType, int64, int64, int) (domain.Photo, bool, error) {
	return domain.Photo{}, false, nil
}
func (f *fakeFiles) CurrentProfilePhoto(context.Context, domain.PeerType, int64) (domain.Photo, bool, error) {
	return domain.Photo{}, false, nil
}
func (f *fakeFiles) GetProfilePhotos(context.Context, domain.PeerType, int64, int, int, int64) ([]domain.Photo, int, error) {
	return nil, 0, nil
}
func (f *fakeFiles) DeleteProfilePhotos(context.Context, domain.PeerType, int64, []int64) (int, error) {
	return 0, nil
}

func newMediaTestRouter(t *testing.T) (*Router, domain.User, domain.User) {
	t.Helper()
	ctx := context.Background()
	userStore := memory.NewUserStore()
	owner, _ := userStore.Create(ctx, domain.User{AccessHash: 11, Phone: "15550009001", FirstName: "Owner"})
	friend, _ := userStore.Create(ctx, domain.User{AccessHash: 12, Phone: "15550009002", FirstName: "Friend"})
	dialogStore := memory.NewDialogStore()
	messageStore := memory.NewMessageStore(dialogStore)
	files := &fakeFiles{
		docs: map[int64]domain.Document{
			555: {
				ID:         555,
				AccessHash: 5,
				DCID:       2,
				MimeType:   "application/x-tgsticker",
				Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrSticker, Alt: "\U0001f600", StickerSetID: 99, StickerSetAccessHash: 7}},
			},
		},
		photos: map[int64]domain.Photo{},
	}
	r := New(Config{DC: 2, IP: "127.0.0.1", Port: 2398}, Deps{
		Users:    appusers.NewService(userStore),
		Messages: appmessages.NewService(messageStore, dialogStore),
		Files:    files,
		Sessions: &captureSessions{},
	}, zaptest.NewLogger(t), clock.System)
	return r, owner, friend
}

func newMessageFromUpdates(t *testing.T, updates tg.UpdatesClass) *tg.Message {
	t.Helper()
	upd, ok := updates.(*tg.Updates)
	if !ok {
		t.Fatalf("expected *tg.Updates, got %T", updates)
	}
	for _, u := range upd.Updates {
		if nm, ok := u.(*tg.UpdateNewMessage); ok {
			msg, ok := nm.Message.(*tg.Message)
			if !ok {
				t.Fatalf("expected *tg.Message, got %T", nm.Message)
			}
			return msg
		}
	}
	t.Fatal("no UpdateNewMessage found")
	return nil
}

func TestSendMediaPrivateSticker(t *testing.T) {
	ctx := context.Background()
	r, owner, friend := newMediaTestRouter(t)

	updates, err := r.onMessagesSendMedia(WithUserID(ctx, owner.ID), &tg.MessagesSendMediaRequest{
		Peer:     &tg.InputPeerUser{UserID: friend.ID, AccessHash: friend.AccessHash},
		Media:    &tg.InputMediaDocument{ID: &tg.InputDocument{ID: 555, AccessHash: 5}},
		RandomID: 1001,
	})
	if err != nil {
		t.Fatalf("sendMedia sticker: %v", err)
	}
	msg := newMessageFromUpdates(t, updates)
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		t.Fatalf("expected MessageMediaDocument, got %T", msg.Media)
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		t.Fatalf("expected tg.Document, got %T", media.Document)
	}
	if want := int64(555); doc.ID != want {
		t.Errorf("document id = %d, want %d", doc.ID, want)
	}
	if doc.DCID != 2 {
		t.Errorf("document dc_id = %d, want 2", doc.DCID)
	}
	hasSticker := false
	for _, a := range doc.Attributes {
		if _, ok := a.(*tg.DocumentAttributeSticker); ok {
			hasSticker = true
		}
	}
	if !hasSticker {
		t.Error("document missing sticker attribute")
	}
}

func TestSendMediaPrivateUploadedPhoto(t *testing.T) {
	ctx := context.Background()
	r, owner, friend := newMediaTestRouter(t)

	updates, err := r.onMessagesSendMedia(WithUserID(ctx, owner.ID), &tg.MessagesSendMediaRequest{
		Peer:     &tg.InputPeerUser{UserID: friend.ID, AccessHash: friend.AccessHash},
		Media:    &tg.InputMediaUploadedPhoto{File: &tg.InputFile{ID: 42, Parts: 1, Name: "p.jpg"}},
		Message:  "caption",
		RandomID: 1002,
	})
	if err != nil {
		t.Fatalf("sendMedia photo: %v", err)
	}
	msg := newMessageFromUpdates(t, updates)
	if msg.Message != "caption" {
		t.Errorf("caption = %q, want %q", msg.Message, "caption")
	}
	media, ok := msg.Media.(*tg.MessageMediaPhoto)
	if !ok {
		t.Fatalf("expected MessageMediaPhoto, got %T", msg.Media)
	}
	photo, ok := media.Photo.(*tg.Photo)
	if !ok {
		t.Fatalf("expected tg.Photo, got %T", media.Photo)
	}
	if photo.ID != 777 {
		t.Errorf("photo id = %d, want 777", photo.ID)
	}
}

func TestUploadMediaReturnsReusableMedia(t *testing.T) {
	ctx := context.Background()
	r, owner, _ := newMediaTestRouter(t)

	media, err := r.onMessagesUploadMedia(WithUserID(ctx, owner.ID), &tg.MessagesUploadMediaRequest{
		Peer:  &tg.InputPeerEmpty{},
		Media: &tg.InputMediaDocument{ID: &tg.InputDocument{ID: 555, AccessHash: 5}},
	})
	if err != nil {
		t.Fatalf("uploadMedia: %v", err)
	}
	if _, ok := media.(*tg.MessageMediaDocument); !ok {
		t.Fatalf("expected MessageMediaDocument, got %T", media)
	}
}

func TestStickerSetDoesNotExposeUnserviceableDownloadThumb(t *testing.T) {
	set := tgStickerSet(domain.StickerSet{
		ID:           99,
		AccessHash:   7,
		Title:        "Set",
		ShortName:    "set",
		ThumbDCID:    2,
		ThumbVersion: 123,
		Thumbs: []domain.PhotoSize{
			{Kind: domain.PhotoSizeKindPath, Type: "j", Bytes: []byte{1, 2, 3}},
			{Kind: domain.PhotoSizeKindDefault, Type: "a", W: 100, H: 100, Size: 4096},
		},
	})
	thumbs, ok := set.GetThumbs()
	if !ok || len(thumbs) != 1 {
		t.Fatalf("thumbs = %#v, want only non-downloadable path thumb", thumbs)
	}
	if _, ok := thumbs[0].(*tg.PhotoPathSize); !ok {
		t.Fatalf("thumb[0] = %T, want PhotoPathSize", thumbs[0])
	}
}
