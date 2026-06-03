package files

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// 上传分片上限：与 Telegram 客户端约定一致（单片 ≤512KB；分片总数有上限防止 OOM）。
const (
	MaxUploadPartBytes = 524288 // 512KB
	MaxUploadParts     = 8000   // 512KB * 8000 ≈ 4GB 理论上限，足够主路径媒体
)

// blobMetaCacheCapacity 是 location_key→FileBlob 元数据 LRU 容量（每项约百字节，约 13MB）。
const blobMetaCacheCapacity = 1 << 16

// 小文件热缓存只覆盖 sticker/reaction/thumbnail 一类不可变小 blob；大媒体继续分段读。
const (
	blobBytesCacheMaxEntryBytes = 256 << 10 // 256KB
	blobBytesCacheMaxBytes      = 64 << 20  // 64MB
)

// Service 实现 upload 分片累积、blob 落盘、getFile 下载，并把上传文件组装成 Photo / Document。
type Service struct {
	media           store.MediaStore
	blobs           BlobBackend
	dc              int
	blobCache       *blobMetaCache
	byteCache       *blobBytesCache
	stickerSetCache *stickerSetFullCache
}

// NewService 创建 files 服务。dc 是本 server 的 DC id，写入新建 document/photo 的 dc_id。
func NewService(media store.MediaStore, blobs BlobBackend, dc int) *Service {
	return &Service{
		media:           media,
		blobs:           blobs,
		dc:              dc,
		blobCache:       newBlobMetaCache(blobMetaCacheCapacity),
		byteCache:       newBlobBytesCache(blobBytesCacheMaxBytes),
		stickerSetCache: newStickerSetFullCache(),
	}
}

// SaveFilePart 累积一个 small file 分片。
func (s *Service) SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, bytes []byte) (bool, error) {
	if err := validatePart(part, len(bytes)); err != nil {
		return false, err
	}
	if err := s.media.SaveFilePart(ctx, domain.UploadPart{
		OwnerUserID: ownerUserID,
		FileID:      fileID,
		Part:        part,
		Bytes:       bytes,
	}); err != nil {
		return false, err
	}
	return true, nil
}

// SaveBigFilePart 累积一个 big file 分片（带已知总分片数）。
func (s *Service) SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, bytes []byte) (bool, error) {
	if err := validatePart(part, len(bytes)); err != nil {
		return false, err
	}
	if totalParts <= 0 || totalParts > MaxUploadParts {
		return false, domain.ErrFilePartsInvalid
	}
	if err := s.media.SaveFilePart(ctx, domain.UploadPart{
		OwnerUserID: ownerUserID,
		FileID:      fileID,
		Part:        part,
		TotalParts:  totalParts,
		Big:         true,
		Bytes:       bytes,
	}); err != nil {
		return false, err
	}
	return true, nil
}

// GetFile 按 location_key 取一段 blob 内容。found=false 表示该 location 无对应 blob。
// 元数据走进程内 LRU（消除每 chunk 一次 PG 查）；小 blob 全量字节进 LRU，供 sticker /
// reaction / thumbnail 热路径直接内存切片；大 blob 仍按 offset/limit 段读。
func (s *Service) GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	blob, ok := s.blobCache.get(req.LocationKey)
	if !ok {
		var (
			found bool
			err   error
		)
		blob, found, err = s.media.GetFileBlob(ctx, req.LocationKey)
		if err != nil {
			return domain.FileChunk{}, false, err
		}
		if !found {
			return domain.FileChunk{}, false, nil
		}
		s.blobCache.put(req.LocationKey, blob)
	}
	if blob.Size > 0 && blob.Size <= blobBytesCacheMaxEntryBytes {
		if data, ok := s.byteCache.get(blob.ObjectKey); ok {
			return domain.FileChunk{
				Bytes:    sliceBlobBytes(data, req.Offset, int64(req.Limit)),
				MimeType: blob.MimeType,
				Total:    int64(len(data)),
			}, true, nil
		}
		data, total, err := s.blobs.GetRange(ctx, blob.ObjectKey, 0, blobBytesCacheMaxEntryBytes+1)
		if err != nil {
			return domain.FileChunk{}, false, fmt.Errorf("read blob %q: %w", blob.LocationKey, err)
		}
		if total <= blobBytesCacheMaxEntryBytes && int64(len(data)) == total {
			s.byteCache.put(blob.ObjectKey, data)
			return domain.FileChunk{
				Bytes:    sliceBlobBytes(data, req.Offset, int64(req.Limit)),
				MimeType: blob.MimeType,
				Total:    total,
			}, true, nil
		}
	}
	data, total, err := s.blobs.GetRange(ctx, blob.ObjectKey, req.Offset, int64(req.Limit))
	if err != nil {
		return domain.FileChunk{}, false, fmt.Errorf("read blob %q: %w", blob.LocationKey, err)
	}
	return domain.FileChunk{
		Bytes:    data,
		MimeType: blob.MimeType,
		Total:    total,
	}, true, nil
}

func sliceBlobBytes(data []byte, offset, limit int64) []byte {
	total := int64(len(data))
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []byte{}
	}
	end := total
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return append([]byte(nil), data[offset:end]...)
}

// ---- 资源读取（reaction / sticker / document）----

// ListAvailableReactions 返回可用 reaction 目录（带真实文档 id）。
func (s *Service) ListAvailableReactions(ctx context.Context) ([]domain.AvailableReaction, error) {
	return s.media.ListAvailableReactions(ctx)
}

// GetDocuments 按 id 批量加载文档（自定义 emoji / 贴纸）。
func (s *Service) GetDocuments(ctx context.Context, ids []int64) ([]domain.Document, error) {
	return s.media.GetDocuments(ctx, ids)
}

// ListStickerSets 列出某类贴纸集（用于 getAllStickers 等）。
func (s *Service) ListStickerSets(ctx context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error) {
	return s.media.ListStickerSets(ctx, kind)
}

// ResolveStickerSet 按 ref 解析贴纸集，并按 DocumentIDs 顺序加载其文档。
func (s *Service) ResolveStickerSet(ctx context.Context, ref domain.StickerSetRef) (domain.StickerSet, []domain.Document, bool, error) {
	if set, docs, ok := s.stickerSetCache.get(ref); ok {
		return set, docs, true, nil
	}
	var (
		set   domain.StickerSet
		found bool
		err   error
	)
	switch ref.Kind {
	case domain.StickerSetRefByID:
		set, found, err = s.media.GetStickerSetByID(ctx, ref.ID)
	case domain.StickerSetRefByShortName:
		set, found, err = s.media.GetStickerSetByShortName(ctx, ref.ShortName)
	case domain.StickerSetRefBySystem:
		set, found, err = s.media.GetStickerSetBySystemKey(ctx, ref.SystemKey)
	default:
		return domain.StickerSet{}, nil, false, nil
	}
	if err != nil || !found {
		return domain.StickerSet{}, nil, found, err
	}
	docs, err := s.media.GetDocuments(ctx, set.DocumentIDs)
	if err != nil {
		return domain.StickerSet{}, nil, false, err
	}
	ordered := orderDocuments(docs, set.DocumentIDs)
	s.stickerSetCache.put(set, ordered)
	return set, ordered, true, nil
}

// orderDocuments 把无序的文档按 ids 顺序重排（GetDocuments 用 ANY 查询不保证顺序）。
func orderDocuments(docs []domain.Document, ids []int64) []domain.Document {
	byID := make(map[int64]domain.Document, len(docs))
	for _, d := range docs {
		byID[d.ID] = d
	}
	out := make([]domain.Document, 0, len(ids))
	for _, id := range ids {
		if d, ok := byID[id]; ok {
			out = append(out, d)
		}
	}
	return out
}

// assembleUpload 把已上传分片按 part 顺序拼成完整字节，并清理分片。
// expectedParts>0 时校验分片连续且齐全。
func (s *Service) assembleUpload(ctx context.Context, ownerUserID, fileID int64, expectedParts int) ([]byte, error) {
	parts, err := s.media.LoadFileParts(ctx, ownerUserID, fileID)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, domain.ErrFilePartsInvalid
	}
	if expectedParts > 0 && len(parts) != expectedParts {
		return nil, domain.ErrFilePartsInvalid
	}
	total := 0
	for i, p := range parts {
		if p.Part != i {
			return nil, domain.ErrFilePartsInvalid // 缺片或乱序
		}
		total += len(p.Bytes)
	}
	buf := make([]byte, 0, total)
	for _, p := range parts {
		buf = append(buf, p.Bytes...)
	}
	if err := s.media.DeleteFileParts(ctx, ownerUserID, fileID); err != nil {
		return nil, err
	}
	return buf, nil
}

func validatePart(part, size int) error {
	if part < 0 || part >= MaxUploadParts {
		return domain.ErrFilePartInvalid
	}
	if size == 0 {
		return domain.ErrFilePartInvalid
	}
	if size > MaxUploadPartBytes {
		return domain.ErrFilePartTooBig
	}
	return nil
}
