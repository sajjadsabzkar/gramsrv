package store

import (
	"context"

	"telesrv/internal/domain"
)

// MediaStore 持久化媒体元数据：上传分片、blob 索引、文档/照片注册表、
// 贴纸集、可用 reaction、头像历史。blob 字节本身由 blob backend 按 object_key 读写，
// 本接口只管 file_blobs 索引行。
type MediaStore interface {
	// 上传分片（transient：组装成 blob 后即清理）。
	SaveFilePart(ctx context.Context, part domain.UploadPart) error
	LoadFileParts(ctx context.Context, ownerUserID, fileID int64) ([]domain.UploadPart, error)
	DeleteFileParts(ctx context.Context, ownerUserID, fileID int64) error

	// blob 索引。
	PutFileBlob(ctx context.Context, blob domain.FileBlob) error
	GetFileBlob(ctx context.Context, locationKey string) (domain.FileBlob, bool, error)

	// 文档 / 照片注册表。
	PutDocument(ctx context.Context, doc domain.Document) error
	GetDocument(ctx context.Context, id int64) (domain.Document, bool, error)
	GetDocuments(ctx context.Context, ids []int64) ([]domain.Document, error)
	PutPhoto(ctx context.Context, photo domain.Photo) error
	GetPhoto(ctx context.Context, id int64) (domain.Photo, bool, error)

	// 贴纸集 / 可用 reaction。
	PutStickerSet(ctx context.Context, set domain.StickerSet) error
	GetStickerSetByID(ctx context.Context, id int64) (domain.StickerSet, bool, error)
	GetStickerSetByShortName(ctx context.Context, shortName string) (domain.StickerSet, bool, error)
	GetStickerSetBySystemKey(ctx context.Context, systemKey string) (domain.StickerSet, bool, error)
	ListStickerSets(ctx context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error)
	CountStickerSets(ctx context.Context) (int, error)
	PutAvailableReaction(ctx context.Context, r domain.AvailableReaction) error
	ListAvailableReactions(ctx context.Context) ([]domain.AvailableReaction, error)
	CountAvailableReactions(ctx context.Context) (int, error)

	// 头像历史（owner = user/channel；current = active 中 sort_order 最大者）。
	AddProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID, photoID int64, date int) error
	CurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64) (int64, bool, error)
	CurrentProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerIDs []int64) (map[int64]domain.ProfilePhotoRef, error)
	ListProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, offset, limit int, maxID int64) (ids []int64, total int, err error)
	DeleteProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, photoIDs []int64) ([]int64, error)
}
