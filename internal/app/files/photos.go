package files

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"image"
	_ "image/jpeg" // 注册 jpeg DecodeConfig，用于读取上传头像/图片尺寸
	_ "image/png"  // 注册 png DecodeConfig
	"time"

	"telesrv/internal/domain"
)

// 头像与图片消息共用的尺寸 type：'a' 小图（≤160），'c' 大图，'x' 通用下载尺寸。
// 同一份上传字节在多个 location_key 下建 blob（不做实际缩放，dev 主路径足够）。

// UploadProfilePhoto 把已上传文件组装成头像 Photo，落 blob/photos/profile_photos，并设为当前头像。
func (s *Service) UploadProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64, file domain.UploadedFileRef, date int) (domain.Photo, error) {
	data, err := s.assembleUpload(ctx, file.OwnerUserID, file.FileID, file.Parts)
	if err != nil {
		return domain.Photo{}, err
	}
	if len(data) == 0 {
		return domain.Photo{}, domain.ErrPhotoInvalid
	}
	if date == 0 {
		date = int(time.Now().Unix())
	}
	photo, err := s.createPhoto(ctx, data, photoSizeSpecsForAvatar(data))
	if err != nil {
		return domain.Photo{}, err
	}
	if err := s.media.AddProfilePhoto(ctx, ownerType, ownerID, photo.ID, date); err != nil {
		return domain.Photo{}, err
	}
	return photo, nil
}

// CreatePhotoFromUpload 把已上传文件组装成 Photo（不绑定 profile_photos），用于频道头像 / 图片消息。
func (s *Service) CreatePhotoFromUpload(ctx context.Context, file domain.UploadedFileRef) (domain.Photo, error) {
	data, err := s.assembleUpload(ctx, file.OwnerUserID, file.FileID, file.Parts)
	if err != nil {
		return domain.Photo{}, err
	}
	if len(data) == 0 {
		return domain.Photo{}, domain.ErrPhotoInvalid
	}
	return s.createPhoto(ctx, data, photoSizeSpecsForMessage(data))
}

// GetPhoto 按 id 返回已存储照片。
func (s *Service) GetPhoto(ctx context.Context, id int64) (domain.Photo, bool, error) {
	return s.media.GetPhoto(ctx, id)
}

// GetDocument 按 id 返回已存储文档（贴纸 / 文件）。
func (s *Service) GetDocument(ctx context.Context, id int64) (domain.Document, bool, error) {
	return s.media.GetDocument(ctx, id)
}

// CreateAvatarFromUpload 把已上传文件组装成头像 Photo（'a'/'c' 尺寸，匹配 InputPeerPhotoFileLocation
// big/small 与 channelFull 合成尺寸的下载路径），不绑定 profile_photos。用于频道 editPhoto。
func (s *Service) CreateAvatarFromUpload(ctx context.Context, file domain.UploadedFileRef) (domain.Photo, error) {
	data, err := s.assembleUpload(ctx, file.OwnerUserID, file.FileID, file.Parts)
	if err != nil {
		return domain.Photo{}, err
	}
	if len(data) == 0 {
		return domain.Photo{}, domain.ErrPhotoInvalid
	}
	return s.createPhoto(ctx, data, photoSizeSpecsForAvatar(data))
}

// CreateDocumentFromUpload 把已上传文件组装成 Document（文件/视频/音频/gif/贴纸消息），落 blob + documents。
func (s *Service) CreateDocumentFromUpload(ctx context.Context, file domain.UploadedFileRef, spec domain.DocumentSpec) (domain.Document, error) {
	data, err := s.assembleUpload(ctx, file.OwnerUserID, file.FileID, file.Parts)
	if err != nil {
		return domain.Document{}, err
	}
	if len(data) == 0 {
		return domain.Document{}, domain.ErrDocumentInvalid
	}
	objectKey, err := s.blobs.Put(ctx, data)
	if err != nil {
		return domain.Document{}, err
	}
	docID := randomID()
	if err := s.media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: fmt.Sprintf("doc:%d", docID),
		Backend:     domain.MediaBackend(s.blobs.Name()),
		ObjectKey:   objectKey,
		Size:        int64(len(data)),
		MimeType:    spec.MimeType,
	}); err != nil {
		return domain.Document{}, err
	}
	doc := domain.Document{
		ID:            docID,
		AccessHash:    randomID(),
		FileReference: randomFileReference(),
		Date:          int(time.Now().Unix()),
		MimeType:      spec.MimeType,
		Size:          int64(len(data)),
		DCID:          s.dc,
		Attributes:    spec.Attributes,
	}
	if spec.Thumb != nil {
		thumbData, err := s.assembleUpload(ctx, spec.Thumb.OwnerUserID, spec.Thumb.FileID, spec.Thumb.Parts)
		if err == nil && len(thumbData) > 0 {
			thumbKey, err := s.blobs.Put(ctx, thumbData)
			if err == nil {
				w, h := imageDimensions(thumbData, 0, 0)
				if err := s.media.PutFileBlob(ctx, domain.FileBlob{
					LocationKey: fmt.Sprintf("doc:%d:m", docID),
					Backend:     domain.MediaBackend(s.blobs.Name()),
					ObjectKey:   thumbKey,
					Size:        int64(len(thumbData)),
					MimeType:    "image/jpeg",
				}); err == nil {
					doc.Thumbs = []domain.PhotoSize{{Kind: domain.PhotoSizeKindDefault, Type: "m", W: w, H: h, Size: len(thumbData)}}
				}
			}
		}
	}
	if err := s.media.PutDocument(ctx, doc); err != nil {
		return domain.Document{}, err
	}
	return doc, nil
}

// SetCurrentProfilePhoto 把已存在的 photo 设为当前头像（updateProfilePhoto 选历史头像）。
func (s *Service) SetCurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID, photoID int64, date int) (domain.Photo, bool, error) {
	photo, ok, err := s.media.GetPhoto(ctx, photoID)
	if err != nil || !ok {
		return domain.Photo{}, ok, err
	}
	if date == 0 {
		date = int(time.Now().Unix())
	}
	if err := s.media.AddProfilePhoto(ctx, ownerType, ownerID, photoID, date); err != nil {
		return domain.Photo{}, false, err
	}
	return photo, true, nil
}

// CurrentProfilePhoto 返回某 owner 的当前头像 Photo。
func (s *Service) CurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64) (domain.Photo, bool, error) {
	id, ok, err := s.media.CurrentProfilePhoto(ctx, ownerType, ownerID)
	if err != nil || !ok {
		return domain.Photo{}, ok, err
	}
	return s.media.GetPhoto(ctx, id)
}

// GetProfilePhotos 返回 owner 的头像历史（最新在前）。
func (s *Service) GetProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, offset, limit int, maxID int64) ([]domain.Photo, int, error) {
	ids, total, err := s.media.ListProfilePhotos(ctx, ownerType, ownerID, offset, limit, maxID)
	if err != nil {
		return nil, 0, err
	}
	photos := make([]domain.Photo, 0, len(ids))
	for _, id := range ids {
		if p, ok, err := s.media.GetPhoto(ctx, id); err != nil {
			return nil, 0, err
		} else if ok {
			photos = append(photos, p)
		}
	}
	return photos, total, nil
}

// DeleteProfilePhotos 停用指定头像，返回成功停用数量。
func (s *Service) DeleteProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, photoIDs []int64) (int, error) {
	deleted, err := s.media.DeleteProfilePhotos(ctx, ownerType, ownerID, photoIDs)
	if err != nil {
		return 0, err
	}
	return len(deleted), nil
}

// createPhoto 把字节落 blob（每个尺寸一个 location_key，指向同一内容）并写 photos 表。
func (s *Service) createPhoto(ctx context.Context, data []byte, specs []photoSizeSpec) (domain.Photo, error) {
	objectKey, err := s.blobs.Put(ctx, data)
	if err != nil {
		return domain.Photo{}, err
	}
	photoID := randomID()
	sizes := make([]domain.PhotoSize, 0, len(specs))
	for _, spec := range specs {
		if err := s.media.PutFileBlob(ctx, domain.FileBlob{
			LocationKey: fmt.Sprintf("photo:%d:%s", photoID, spec.Type),
			Backend:     domain.MediaBackend(s.blobs.Name()),
			ObjectKey:   objectKey,
			Size:        int64(len(data)),
			MimeType:    "image/jpeg",
		}); err != nil {
			return domain.Photo{}, err
		}
		sizes = append(sizes, domain.PhotoSize{Kind: domain.PhotoSizeKindDefault, Type: spec.Type, W: spec.W, H: spec.H, Size: len(data)})
	}
	photo := domain.Photo{
		ID:            photoID,
		AccessHash:    randomID(),
		FileReference: randomFileReference(),
		Date:          int(time.Now().Unix()),
		DCID:          s.dc,
		Sizes:         sizes,
	}
	if err := s.media.PutPhoto(ctx, photo); err != nil {
		return domain.Photo{}, err
	}
	return photo, nil
}

type photoSizeSpec struct {
	Type string
	W    int
	H    int
}

func photoSizeSpecsForAvatar(data []byte) []photoSizeSpec {
	w, h := imageDimensions(data, 640, 640)
	small := 160
	if w < small {
		small = w
	}
	return []photoSizeSpec{
		{Type: "a", W: small, H: small},
		{Type: "c", W: w, H: h},
	}
}

// photoSizeSpecsForMessage 给图片消息生成下载尺寸（'m' 缩略 + 'x'/'y' 大图）。
func photoSizeSpecsForMessage(data []byte) []photoSizeSpec {
	w, h := imageDimensions(data, 1280, 1280)
	thumbW, thumbH := scaleDown(w, h, 320)
	return []photoSizeSpec{
		{Type: "m", W: thumbW, H: thumbH},
		{Type: "x", W: w, H: h},
	}
}

func imageDimensions(data []byte, defW, defH int) (int, int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		return defW, defH
	}
	return cfg.Width, cfg.Height
}

func scaleDown(w, h, max int) (int, int) {
	if w <= max && h <= max {
		return w, h
	}
	if w >= h {
		return max, max * h / w
	}
	return max * w / h, max
}

func randomID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	v := int64(binary.BigEndian.Uint64(b[:]) >> 1)
	if v == 0 {
		v = 1
	}
	return v
}

func randomFileReference() []byte {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return b
}
