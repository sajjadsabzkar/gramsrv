package files

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

// WarmStats 汇报一次启动资源缓存预热结果。
type WarmStats struct {
	StickerSets int
	Documents   int
	Blobs       int
}

// WarmCaches 从已持久化的 sticker/reaction 元数据预热小 blob 字节缓存与完整 sticker set 缓存。
// SeedMedia 在已有数据时会跳过导入；该方法保证普通 server 重启后历史 sticker 首次渲染也不是冷缓存。
func (s *Service) WarmCaches(ctx context.Context) (WarmStats, error) {
	var stats WarmStats
	seenDocs := make(map[int64]struct{})
	for _, kind := range []domain.StickerSetKind{
		domain.StickerSetKindStickers,
		domain.StickerSetKindEmoji,
		domain.StickerSetKindMasks,
		domain.StickerSetKindSystem,
	} {
		sets, err := s.media.ListStickerSets(ctx, kind)
		if err != nil {
			return stats, err
		}
		for _, set := range sets {
			docs, err := s.media.GetDocuments(ctx, set.DocumentIDs)
			if err != nil {
				return stats, err
			}
			ordered := orderDocuments(docs, set.DocumentIDs)
			s.stickerSetCache.put(set, ordered)
			stats.StickerSets++
			for _, doc := range ordered {
				if _, ok := seenDocs[doc.ID]; ok {
					continue
				}
				seenDocs[doc.ID] = struct{}{}
				stats.Documents++
				warmed, err := s.prewarmDocumentBlobs(ctx, doc)
				if err != nil {
					return stats, err
				}
				stats.Blobs += warmed
			}
		}
	}
	reactions, err := s.media.ListAvailableReactions(ctx)
	if err != nil {
		return stats, err
	}
	reactionIDs := make([]int64, 0, len(reactions)*4)
	for _, reaction := range reactions {
		reactionIDs = append(reactionIDs, reaction.DocumentIDs()...)
	}
	docs, err := s.media.GetDocuments(ctx, reactionIDs)
	if err != nil {
		return stats, err
	}
	for _, doc := range docs {
		if _, ok := seenDocs[doc.ID]; ok {
			continue
		}
		seenDocs[doc.ID] = struct{}{}
		stats.Documents++
		warmed, err := s.prewarmDocumentBlobs(ctx, doc)
		if err != nil {
			return stats, err
		}
		stats.Blobs += warmed
	}
	return stats, nil
}

func (s *Service) prewarmDocumentBlobs(ctx context.Context, doc domain.Document) (int, error) {
	if doc.ID == 0 {
		return 0, nil
	}
	warmed := 0
	ok, err := s.prewarmLocationKey(ctx, fmt.Sprintf("doc:%d", doc.ID))
	if err != nil {
		return 0, err
	}
	if ok {
		warmed++
	}
	for _, thumb := range doc.Thumbs {
		if !thumb.Downloadable() {
			continue
		}
		ok, err := s.prewarmLocationKey(ctx, fmt.Sprintf("doc:%d:%s", doc.ID, thumb.Type))
		if err != nil {
			return 0, err
		}
		if ok {
			warmed++
		}
	}
	return warmed, nil
}

func (s *Service) prewarmLocationKey(ctx context.Context, locationKey string) (bool, error) {
	blob, ok := s.blobCache.get(locationKey)
	if !ok {
		var (
			found bool
			err   error
		)
		blob, found, err = s.media.GetFileBlob(ctx, locationKey)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		s.blobCache.put(locationKey, blob)
	}
	if blob.Size <= 0 || blob.Size > blobBytesCacheMaxEntryBytes || s.byteCache.has(blob.ObjectKey) {
		return false, nil
	}
	data, total, err := s.blobs.GetRange(ctx, blob.ObjectKey, 0, blobBytesCacheMaxEntryBytes+1)
	if err != nil {
		return false, fmt.Errorf("read blob %q: %w", blob.LocationKey, err)
	}
	if total <= blobBytesCacheMaxEntryBytes && int64(len(data)) == total {
		s.byteCache.put(blob.ObjectKey, data)
		return true, nil
	}
	return false, nil
}
