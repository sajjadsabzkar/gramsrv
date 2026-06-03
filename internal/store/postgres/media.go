package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// MediaStore 用 PostgreSQL 实现 store.MediaStore（媒体元数据 + blob 索引）。
type MediaStore struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

// NewMediaStore 基于 pgx 连接池（或事务）创建 MediaStore。
func NewMediaStore(db sqlcgen.DBTX) *MediaStore {
	return &MediaStore{db: db, q: sqlcgen.New(db)}
}

// bytesOrEmpty 把 nil []byte 归一为空切片，避免落入 NOT NULL bytea 列时被当作 NULL。
func bytesOrEmpty(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

var _ store.MediaStore = (*MediaStore)(nil)

// ---- 上传分片 ----

func (s *MediaStore) SaveFilePart(ctx context.Context, part domain.UploadPart) error {
	return s.q.SaveUploadPart(ctx, sqlcgen.SaveUploadPartParams{
		OwnerUserID: part.OwnerUserID,
		FileID:      part.FileID,
		Part:        int32(part.Part),
		TotalParts:  int32(part.TotalParts),
		IsBig:       part.Big,
		Bytes:       part.Bytes,
	})
}

func (s *MediaStore) LoadFileParts(ctx context.Context, ownerUserID, fileID int64) ([]domain.UploadPart, error) {
	rows, err := s.q.ListUploadParts(ctx, sqlcgen.ListUploadPartsParams{OwnerUserID: ownerUserID, FileID: fileID})
	if err != nil {
		return nil, err
	}
	out := make([]domain.UploadPart, 0, len(rows))
	for _, r := range rows {
		out = append(out, domain.UploadPart{
			OwnerUserID: ownerUserID,
			FileID:      fileID,
			Part:        int(r.Part),
			TotalParts:  int(r.TotalParts),
			Big:         r.IsBig,
			Bytes:       r.Bytes,
		})
	}
	return out, nil
}

func (s *MediaStore) DeleteFileParts(ctx context.Context, ownerUserID, fileID int64) error {
	return s.q.DeleteUploadParts(ctx, sqlcgen.DeleteUploadPartsParams{OwnerUserID: ownerUserID, FileID: fileID})
}

// ---- blob 索引 ----

func (s *MediaStore) PutFileBlob(ctx context.Context, blob domain.FileBlob) error {
	backend := string(blob.Backend)
	if backend == "" {
		backend = string(domain.MediaBackendLocalFS)
	}
	sha := blob.SHA256
	if sha == nil {
		sha = []byte{} // 列为 NOT NULL；nil []byte 会被 pgx 当作 NULL。
	}
	return s.q.PutFileBlob(ctx, sqlcgen.PutFileBlobParams{
		LocationKey: blob.LocationKey,
		Backend:     backend,
		ObjectKey:   blob.ObjectKey,
		Size:        blob.Size,
		Sha256:      sha,
		MimeType:    blob.MimeType,
	})
}

func (s *MediaStore) GetFileBlob(ctx context.Context, locationKey string) (domain.FileBlob, bool, error) {
	row, err := s.q.GetFileBlob(ctx, locationKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.FileBlob{}, false, nil
		}
		return domain.FileBlob{}, false, err
	}
	return domain.FileBlob{
		LocationKey: row.LocationKey,
		Backend:     domain.MediaBackend(row.Backend),
		ObjectKey:   row.ObjectKey,
		Size:        row.Size,
		SHA256:      row.Sha256,
		MimeType:    row.MimeType,
	}, true, nil
}

// ---- 文档 ----

func (s *MediaStore) PutDocument(ctx context.Context, doc domain.Document) error {
	attrs, err := jsonArrayOrEmpty(doc.Attributes)
	if err != nil {
		return err
	}
	thumbs, err := jsonArrayOrEmpty(doc.Thumbs)
	if err != nil {
		return err
	}
	return s.q.PutDocument(ctx, sqlcgen.PutDocumentParams{
		ID:             doc.ID,
		AccessHash:     doc.AccessHash,
		FileReference:  bytesOrEmpty(doc.FileReference),
		Date:           int32(doc.Date),
		MimeType:       doc.MimeType,
		Size:           doc.Size,
		DcID:           int32(doc.DCID),
		AttributesJson: attrs,
		ThumbsJson:     thumbs,
	})
}

func (s *MediaStore) GetDocument(ctx context.Context, id int64) (domain.Document, bool, error) {
	row, err := s.q.GetDocument(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Document{}, false, nil
		}
		return domain.Document{}, false, err
	}
	doc, err := documentFromRow(row)
	if err != nil {
		return domain.Document{}, false, err
	}
	return doc, true, nil
}

func (s *MediaStore) GetDocuments(ctx context.Context, ids []int64) ([]domain.Document, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.GetDocuments(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Document, 0, len(rows))
	for _, r := range rows {
		doc, err := documentFromRow(sqlcgen.GetDocumentRow(r))
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, nil
}

func documentFromRow(row sqlcgen.GetDocumentRow) (domain.Document, error) {
	attrs, err := decodeDocumentAttributes(row.AttributesJson)
	if err != nil {
		return domain.Document{}, err
	}
	thumbs, err := decodePhotoSizes(row.ThumbsJson)
	if err != nil {
		return domain.Document{}, err
	}
	return domain.Document{
		ID:            row.ID,
		AccessHash:    row.AccessHash,
		FileReference: row.FileReference,
		Date:          int(row.Date),
		MimeType:      row.MimeType,
		Size:          row.Size,
		DCID:          int(row.DcID),
		Attributes:    attrs,
		Thumbs:        thumbs,
	}, nil
}

// ---- 照片 ----

func (s *MediaStore) PutPhoto(ctx context.Context, photo domain.Photo) error {
	sizes, err := jsonArrayOrEmpty(photo.Sizes)
	if err != nil {
		return err
	}
	return s.q.PutPhoto(ctx, sqlcgen.PutPhotoParams{
		ID:            photo.ID,
		AccessHash:    photo.AccessHash,
		FileReference: bytesOrEmpty(photo.FileReference),
		Date:          int32(photo.Date),
		DcID:          int32(photo.DCID),
		HasStickers:   photo.HasStickers,
		SizesJson:     sizes,
	})
}

func (s *MediaStore) GetPhoto(ctx context.Context, id int64) (domain.Photo, bool, error) {
	row, err := s.q.GetPhoto(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Photo{}, false, nil
		}
		return domain.Photo{}, false, err
	}
	sizes, err := decodePhotoSizes(row.SizesJson)
	if err != nil {
		return domain.Photo{}, false, err
	}
	return domain.Photo{
		ID:            row.ID,
		AccessHash:    row.AccessHash,
		FileReference: row.FileReference,
		Date:          int(row.Date),
		DCID:          int(row.DcID),
		HasStickers:   row.HasStickers,
		Sizes:         sizes,
	}, true, nil
}

// ---- 贴纸集 ----

func (s *MediaStore) PutStickerSet(ctx context.Context, set domain.StickerSet) error {
	thumbs, err := jsonArrayOrEmpty(set.Thumbs)
	if err != nil {
		return err
	}
	docIDs, err := jsonArrayOrEmpty(set.DocumentIDs)
	if err != nil {
		return err
	}
	packs, err := jsonArrayOrEmpty(set.Packs)
	if err != nil {
		return err
	}
	kind := string(set.Kind)
	if kind == "" {
		kind = string(domain.StickerSetKindStickers)
	}
	return s.q.PutStickerSet(ctx, sqlcgen.PutStickerSetParams{
		ID:              set.ID,
		AccessHash:      set.AccessHash,
		ShortName:       set.ShortName,
		Title:           set.Title,
		Count:           int32(set.Count),
		Hash:            int32(set.Hash),
		SetKind:         kind,
		Official:        set.Official,
		Animated:        set.Animated,
		Videos:          set.Videos,
		Emojis:          set.Emojis,
		Masks:           set.Masks,
		Installed:       set.Installed,
		Archived:        set.Archived,
		InstalledDate:   int32(set.InstalledDate),
		ThumbDocumentID: set.ThumbDocumentID,
		ThumbsJson:      thumbs,
		ThumbDcID:       int32(set.ThumbDCID),
		ThumbVersion:    int32(set.ThumbVersion),
		DocumentIdsJson: docIDs,
		PacksJson:       packs,
		SortOrder:       int32(set.SortOrder),
		SystemKey:       set.SystemKey,
	})
}

func (s *MediaStore) GetStickerSetByID(ctx context.Context, id int64) (domain.StickerSet, bool, error) {
	row, err := s.q.GetStickerSetByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.StickerSet{}, false, nil
		}
		return domain.StickerSet{}, false, err
	}
	return stickerSetFromRow(row)
}

func (s *MediaStore) GetStickerSetByShortName(ctx context.Context, shortName string) (domain.StickerSet, bool, error) {
	row, err := s.q.GetStickerSetByShortName(ctx, shortName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.StickerSet{}, false, nil
		}
		return domain.StickerSet{}, false, err
	}
	return stickerSetFromRow(sqlcgen.GetStickerSetByIDRow(row))
}

func (s *MediaStore) GetStickerSetBySystemKey(ctx context.Context, systemKey string) (domain.StickerSet, bool, error) {
	row, err := s.q.GetStickerSetBySystemKey(ctx, systemKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.StickerSet{}, false, nil
		}
		return domain.StickerSet{}, false, err
	}
	return stickerSetFromRow(sqlcgen.GetStickerSetByIDRow(row))
}

func (s *MediaStore) ListStickerSets(ctx context.Context, kind domain.StickerSetKind) ([]domain.StickerSet, error) {
	rows, err := s.q.ListStickerSetsByKind(ctx, string(kind))
	if err != nil {
		return nil, err
	}
	out := make([]domain.StickerSet, 0, len(rows))
	for _, r := range rows {
		set, _, err := stickerSetFromRow(sqlcgen.GetStickerSetByIDRow(r))
		if err != nil {
			return nil, err
		}
		out = append(out, set)
	}
	return out, nil
}

func (s *MediaStore) CountStickerSets(ctx context.Context) (int, error) {
	n, err := s.q.CountStickerSets(ctx)
	return int(n), err
}

func stickerSetFromRow(row sqlcgen.GetStickerSetByIDRow) (domain.StickerSet, bool, error) {
	thumbs, err := decodePhotoSizes(row.ThumbsJson)
	if err != nil {
		return domain.StickerSet{}, false, err
	}
	docIDs, err := decodeInt64Slice(row.DocumentIdsJson)
	if err != nil {
		return domain.StickerSet{}, false, err
	}
	packs, err := decodeStickerPacks(row.PacksJson)
	if err != nil {
		return domain.StickerSet{}, false, err
	}
	return domain.StickerSet{
		ID:              row.ID,
		AccessHash:      row.AccessHash,
		ShortName:       row.ShortName,
		Title:           row.Title,
		Count:           int(row.Count),
		Hash:            int(row.Hash),
		Kind:            domain.StickerSetKind(row.SetKind),
		Official:        row.Official,
		Animated:        row.Animated,
		Videos:          row.Videos,
		Emojis:          row.Emojis,
		Masks:           row.Masks,
		Installed:       row.Installed,
		Archived:        row.Archived,
		InstalledDate:   int(row.InstalledDate),
		ThumbDocumentID: row.ThumbDocumentID,
		Thumbs:          thumbs,
		ThumbDCID:       int(row.ThumbDcID),
		ThumbVersion:    int(row.ThumbVersion),
		DocumentIDs:     docIDs,
		Packs:           packs,
		SortOrder:       int(row.SortOrder),
		SystemKey:       row.SystemKey,
	}, true, nil
}

// ---- 可用 reaction ----

func (s *MediaStore) PutAvailableReaction(ctx context.Context, r domain.AvailableReaction) error {
	return s.q.PutAvailableReaction(ctx, sqlcgen.PutAvailableReactionParams{
		Reaction:            r.Reaction,
		Title:               r.Title,
		Inactive:            r.Inactive,
		Premium:             r.Premium,
		StaticIconID:        r.StaticIconID,
		AppearAnimationID:   r.AppearAnimationID,
		SelectAnimationID:   r.SelectAnimationID,
		ActivateAnimationID: r.ActivateAnimationID,
		EffectAnimationID:   r.EffectAnimationID,
		AroundAnimationID:   r.AroundAnimationID,
		CenterIconID:        r.CenterIconID,
		SortOrder:           int32(r.Order),
	})
}

func (s *MediaStore) ListAvailableReactions(ctx context.Context) ([]domain.AvailableReaction, error) {
	rows, err := s.q.ListAvailableReactions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.AvailableReaction, 0, len(rows))
	for _, r := range rows {
		out = append(out, domain.AvailableReaction{
			Reaction:            r.Reaction,
			Title:               r.Title,
			Inactive:            r.Inactive,
			Premium:             r.Premium,
			StaticIconID:        r.StaticIconID,
			AppearAnimationID:   r.AppearAnimationID,
			SelectAnimationID:   r.SelectAnimationID,
			ActivateAnimationID: r.ActivateAnimationID,
			EffectAnimationID:   r.EffectAnimationID,
			AroundAnimationID:   r.AroundAnimationID,
			CenterIconID:        r.CenterIconID,
			Order:               int(r.SortOrder),
		})
	}
	return out, nil
}

func (s *MediaStore) CountAvailableReactions(ctx context.Context) (int, error) {
	n, err := s.q.CountAvailableReactions(ctx)
	return int(n), err
}

// ---- 头像历史 ----

func (s *MediaStore) AddProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID, photoID int64, date int) error {
	next, err := s.q.NextProfilePhotoOrder(ctx, sqlcgen.NextProfilePhotoOrderParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
	})
	if err != nil {
		return err
	}
	return s.q.AddProfilePhoto(ctx, sqlcgen.AddProfilePhotoParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
		PhotoID:       photoID,
		Date:          int32(date),
		SortOrder:     next + 1,
	})
}

func (s *MediaStore) CurrentProfilePhoto(ctx context.Context, ownerType domain.PeerType, ownerID int64) (int64, bool, error) {
	id, err := s.q.CurrentProfilePhoto(ctx, sqlcgen.CurrentProfilePhotoParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return id, true, nil
}

func (s *MediaStore) CurrentProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerIDs []int64) (map[int64]domain.ProfilePhotoRef, error) {
	if len(ownerIDs) == 0 {
		return map[int64]domain.ProfilePhotoRef{}, nil
	}
	rows, err := s.q.CurrentProfilePhotosForOwners(ctx, sqlcgen.CurrentProfilePhotosForOwnersParams{
		OwnerPeerType: string(ownerType),
		OwnerIds:      ownerIDs,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int64]domain.ProfilePhotoRef, len(rows))
	for _, r := range rows {
		sizes, err := decodePhotoSizes(r.SizesJson)
		if err != nil {
			return nil, err
		}
		out[r.OwnerPeerID] = domain.ProfilePhotoRef{
			PhotoID:  r.PhotoID,
			DCID:     int(r.DcID),
			Stripped: domain.StrippedFromSizes(sizes),
		}
	}
	return out, nil
}

func (s *MediaStore) ListProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, offset, limit int, maxID int64) ([]int64, int, error) {
	ids, err := s.q.ListProfilePhotos(ctx, sqlcgen.ListProfilePhotosParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
		MaxID:         maxID,
		OffsetCount:   int32(offset),
		LimitCount:    int32(limit),
	})
	if err != nil {
		return nil, 0, err
	}
	total, err := s.q.CountProfilePhotos(ctx, sqlcgen.CountProfilePhotosParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
	})
	if err != nil {
		return nil, 0, err
	}
	return ids, int(total), nil
}

func (s *MediaStore) DeleteProfilePhotos(ctx context.Context, ownerType domain.PeerType, ownerID int64, photoIDs []int64) ([]int64, error) {
	if len(photoIDs) == 0 {
		return nil, nil
	}
	return s.q.DeactivateProfilePhotos(ctx, sqlcgen.DeactivateProfilePhotosParams{
		OwnerPeerType: string(ownerType),
		OwnerPeerID:   ownerID,
		PhotoIds:      photoIDs,
	})
}
