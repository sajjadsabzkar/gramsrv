package rpc

import (
	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

// 本文件集中 domain media 值对象 → tg.* 的转换；tg.* 只在 rpc 层出现。
// 供 reaction / sticker 资源 RPC 与消息 media 共用。

// tgMessageMedia 把消息 media 快照转成 tg.MessageMediaClass；空载荷回退 MessageMediaEmpty。
func tgMessageMedia(m *domain.MessageMedia) tg.MessageMediaClass {
	if m.IsZero() {
		return &tg.MessageMediaEmpty{}
	}
	switch m.Kind {
	case domain.MessageMediaKindPhoto:
		out := &tg.MessageMediaPhoto{Spoiler: m.Spoiler}
		if m.Photo != nil {
			out.Photo = tgPhoto(*m.Photo)
		}
		if m.TTLSeconds > 0 {
			out.TTLSeconds = m.TTLSeconds
		}
		return out
	case domain.MessageMediaKindDocument:
		out := &tg.MessageMediaDocument{
			Spoiler:   m.Spoiler,
			Nopremium: m.Nopremium,
			Voice:     m.Voice,
			Round:     m.Round,
			Video:     m.Video,
		}
		if m.Document != nil {
			out.Document = tgDocument(*m.Document)
		}
		if m.TTLSeconds > 0 {
			out.TTLSeconds = m.TTLSeconds
		}
		return out
	default:
		return &tg.MessageMediaEmpty{}
	}
}

// tgChatPhoto 由 domain.Channel 反范式头像字段构造 ChatPhoto（频道/群头像缩略）。
func tgChatPhoto(ch domain.Channel) tg.ChatPhotoClass {
	if ch.PhotoID == 0 {
		return &tg.ChatPhotoEmpty{}
	}
	p := &tg.ChatPhoto{PhotoID: ch.PhotoID, DCID: ch.PhotoDCID}
	if len(ch.PhotoStripped) > 0 {
		p.SetStrippedThumb(ch.PhotoStripped)
	}
	return p
}

// tgChannelChatPhotoFull 为 channelFull.chat_photo 构造完整 Photo（合成 a/c 尺寸；
// getFile 按 photo:<id>:<type> 解析，忽略 access_hash，故合成尺寸也可下载）。
func tgChannelChatPhotoFull(ch domain.Channel) tg.PhotoClass {
	if ch.PhotoID == 0 {
		return &tg.PhotoEmpty{}
	}
	photo := &tg.Photo{ID: ch.PhotoID, DCID: ch.PhotoDCID, Sizes: syntheticAvatarSizes()}
	if len(ch.PhotoStripped) > 0 {
		photo.Sizes = append([]tg.PhotoSizeClass{&tg.PhotoStrippedSize{Type: "i", Bytes: ch.PhotoStripped}}, photo.Sizes...)
	}
	return photo
}

func syntheticAvatarSizes() []tg.PhotoSizeClass {
	return []tg.PhotoSizeClass{
		&tg.PhotoSize{Type: "a", W: 160, H: 160, Size: 0},
		&tg.PhotoSize{Type: "c", W: 640, H: 640, Size: 0},
	}
}

// tgPhoto 把 domain.Photo 转成 tg.PhotoClass。
func tgPhoto(p domain.Photo) tg.PhotoClass {
	if p.ID == 0 {
		return &tg.PhotoEmpty{}
	}
	return &tg.Photo{
		ID:            p.ID,
		AccessHash:    p.AccessHash,
		FileReference: p.FileReference,
		Date:          p.Date,
		Sizes:         tgPhotoSizes(p.Sizes),
		DCID:          p.DCID,
		HasStickers:   p.HasStickers,
	}
}

// tgDocument 把 domain.Document 转成 tg.DocumentClass。
func tgDocument(d domain.Document) tg.DocumentClass {
	if d.ID == 0 {
		return &tg.DocumentEmpty{}
	}
	return &tg.Document{
		ID:            d.ID,
		AccessHash:    d.AccessHash,
		FileReference: d.FileReference,
		Date:          d.Date,
		MimeType:      d.MimeType,
		Size:          d.Size,
		Thumbs:        tgDocumentThumbs(d.Thumbs),
		DCID:          d.DCID,
		Attributes:    tgDocumentAttributes(d.Attributes),
	}
}

func tgDocuments(docs []domain.Document) []tg.DocumentClass {
	out := make([]tg.DocumentClass, 0, len(docs))
	for _, d := range docs {
		out = append(out, tgDocument(d))
	}
	return out
}

func tgDocumentThumbs(sizes []domain.PhotoSize) []tg.PhotoSizeClass {
	if len(sizes) == 0 {
		return nil
	}
	out := make([]tg.PhotoSizeClass, 0, len(sizes))
	for _, s := range sizes {
		if s.Kind == domain.PhotoSizeKindCached && len(s.Bytes) > 0 {
			size := s.Size
			if size == 0 {
				size = len(s.Bytes)
			}
			if s.Type != "" && s.W > 0 && s.H > 0 && size > 0 {
				out = append(out, &tg.PhotoSize{Type: s.Type, W: s.W, H: s.H, Size: size})
			}
			continue
		}
		out = append(out, tgPhotoSize(s))
	}
	return compactPhotoSizeClasses(out)
}

func tgPhotoSizes(sizes []domain.PhotoSize) []tg.PhotoSizeClass {
	if len(sizes) == 0 {
		return nil
	}
	out := make([]tg.PhotoSizeClass, 0, len(sizes))
	for _, s := range sizes {
		out = append(out, tgPhotoSize(s))
	}
	return compactPhotoSizeClasses(out)
}

func tgPhotoSize(s domain.PhotoSize) tg.PhotoSizeClass {
	switch s.Kind {
	case domain.PhotoSizeKindDefault:
		return &tg.PhotoSize{Type: s.Type, W: s.W, H: s.H, Size: s.Size}
	case domain.PhotoSizeKindStripped:
		return &tg.PhotoStrippedSize{Type: s.Type, Bytes: s.Bytes}
	case domain.PhotoSizeKindCached:
		return &tg.PhotoCachedSize{Type: s.Type, W: s.W, H: s.H, Bytes: s.Bytes}
	case domain.PhotoSizeKindPath:
		return &tg.PhotoPathSize{Type: s.Type, Bytes: s.Bytes}
	case domain.PhotoSizeKindProgressive:
		return &tg.PhotoSizeProgressive{Type: s.Type, W: s.W, H: s.H, Sizes: s.Sizes}
	default:
		return nil
	}
}

func compactPhotoSizeClasses(in []tg.PhotoSizeClass) []tg.PhotoSizeClass {
	out := in[:0]
	for _, s := range in {
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

func tgDocumentAttributes(attrs []domain.DocumentAttribute) []tg.DocumentAttributeClass {
	out := make([]tg.DocumentAttributeClass, 0, len(attrs))
	for _, a := range attrs {
		switch a.Kind {
		case domain.DocAttrImageSize:
			out = append(out, &tg.DocumentAttributeImageSize{W: a.W, H: a.H})
		case domain.DocAttrAnimated:
			out = append(out, &tg.DocumentAttributeAnimated{})
		case domain.DocAttrSticker:
			out = append(out, &tg.DocumentAttributeSticker{
				Mask:       a.Mask,
				Alt:        a.Alt,
				Stickerset: tgInputStickerSetFromIDs(a.StickerSetID, a.StickerSetAccessHash),
			})
		case domain.DocAttrVideo:
			out = append(out, &tg.DocumentAttributeVideo{
				RoundMessage:      a.RoundMessage,
				SupportsStreaming: a.SupportsStreaming,
				Duration:          a.Duration,
				W:                 a.W,
				H:                 a.H,
			})
		case domain.DocAttrAudio:
			attr := &tg.DocumentAttributeAudio{
				Voice:     a.Voice,
				Duration:  a.AudioDuration,
				Title:     a.Title,
				Performer: a.Performer,
			}
			if len(a.Waveform) > 0 {
				attr.SetWaveform(a.Waveform)
			}
			out = append(out, attr)
		case domain.DocAttrFilename:
			out = append(out, &tg.DocumentAttributeFilename{FileName: a.FileName})
		case domain.DocAttrCustomEmoji:
			out = append(out, &tg.DocumentAttributeCustomEmoji{
				Free:       a.Free,
				TextColor:  a.TextColor,
				Alt:        a.Alt,
				Stickerset: tgInputStickerSetFromIDs(a.StickerSetID, a.StickerSetAccessHash),
			})
		}
	}
	return out
}

func tgInputStickerSetFromIDs(id, accessHash int64) tg.InputStickerSetClass {
	if id == 0 {
		return &tg.InputStickerSetEmpty{}
	}
	return &tg.InputStickerSetID{ID: id, AccessHash: accessHash}
}

// ---- available reactions ----

// tgAvailableReactions 用真实文档构造 messages.availableReactions；docByID 由 handler 预加载。
func tgAvailableReactions(reactions []domain.AvailableReaction, docByID map[int64]domain.Document, hash int) *tg.MessagesAvailableReactions {
	out := &tg.MessagesAvailableReactions{Hash: hash, Reactions: make([]tg.AvailableReaction, 0, len(reactions))}
	doc := func(id int64) tg.DocumentClass {
		if d, ok := docByID[id]; ok {
			return tgDocument(d)
		}
		return &tg.DocumentEmpty{ID: id}
	}
	for _, r := range reactions {
		ar := tg.AvailableReaction{
			Inactive:          r.Inactive,
			Premium:           r.Premium,
			Reaction:          r.Reaction,
			Title:             r.Title,
			StaticIcon:        doc(r.StaticIconID),
			AppearAnimation:   doc(r.AppearAnimationID),
			SelectAnimation:   doc(r.SelectAnimationID),
			ActivateAnimation: doc(r.ActivateAnimationID),
			EffectAnimation:   doc(r.EffectAnimationID),
		}
		if r.AroundAnimationID != 0 {
			ar.SetAroundAnimation(doc(r.AroundAnimationID))
		}
		if r.CenterIconID != 0 {
			ar.SetCenterIcon(doc(r.CenterIconID))
		}
		out.Reactions = append(out.Reactions, ar)
	}
	return out
}

// reactionDocumentIDs 收集一组 reaction 引用的全部文档 id（用于批量预加载）。
func reactionDocumentIDs(reactions []domain.AvailableReaction) []int64 {
	seen := make(map[int64]struct{})
	out := make([]int64, 0, len(reactions)*7)
	for _, r := range reactions {
		for _, id := range r.DocumentIDs() {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// ---- sticker sets ----

func tgStickerSet(set domain.StickerSet) tg.StickerSet {
	out := tg.StickerSet{
		Archived:   set.Archived,
		Official:   set.Official,
		Masks:      set.Masks,
		Emojis:     set.Emojis,
		ID:         set.ID,
		AccessHash: set.AccessHash,
		Title:      set.Title,
		ShortName:  set.ShortName,
		Count:      set.Count,
		Hash:       set.Hash,
	}
	if set.Installed {
		date := set.InstalledDate
		if date == 0 {
			date = 1
		}
		out.SetInstalledDate(date)
	}
	if thumbs := tgStickerSetThumbs(set.Thumbs); len(thumbs) > 0 {
		out.SetThumbs(thumbs)
		out.SetThumbDCID(set.ThumbDCID)
		out.SetThumbVersion(set.ThumbVersion)
	}
	if set.ThumbDocumentID != 0 {
		out.SetThumbDocumentID(set.ThumbDocumentID)
	}
	return out
}

func tgStickerSetThumbs(sizes []domain.PhotoSize) []tg.PhotoSizeClass {
	if len(sizes) == 0 {
		return nil
	}
	filtered := make([]domain.PhotoSize, 0, len(sizes))
	for _, s := range sizes {
		if s.Downloadable() {
			continue
		}
		filtered = append(filtered, s)
	}
	return tgPhotoSizes(filtered)
}

func tgStickerSets(sets []domain.StickerSet) []tg.StickerSet {
	out := make([]tg.StickerSet, 0, len(sets))
	for _, s := range sets {
		out = append(out, tgStickerSet(s))
	}
	return out
}

func tgStickerPacks(packs []domain.StickerPack) []tg.StickerPack {
	out := make([]tg.StickerPack, 0, len(packs))
	for _, p := range packs {
		out = append(out, tg.StickerPack{Emoticon: p.Emoticon, Documents: append([]int64(nil), p.DocumentIDs...)})
	}
	return out
}

// tgMessagesStickerSet 构造完整 messages.stickerSet（set + packs + documents）。
func tgMessagesStickerSet(set domain.StickerSet, docs []domain.Document) *tg.MessagesStickerSet {
	return &tg.MessagesStickerSet{
		Set:       tgStickerSet(set),
		Packs:     tgStickerPacks(set.Packs),
		Keywords:  []tg.StickerKeyword{},
		Documents: tgDocuments(docs),
	}
}

// stickerSetRefFromInput 把 tg.InputStickerSet 转成 domain.StickerSetRef。
func stickerSetRefFromInput(input tg.InputStickerSetClass) (domain.StickerSetRef, bool) {
	switch in := input.(type) {
	case *tg.InputStickerSetID:
		return domain.StickerSetRef{Kind: domain.StickerSetRefByID, ID: in.ID, AccessHash: in.AccessHash}, true
	case *tg.InputStickerSetShortName:
		return domain.StickerSetRef{Kind: domain.StickerSetRefByShortName, ShortName: in.ShortName}, true
	case *tg.InputStickerSetAnimatedEmoji:
		return domain.StickerSetRef{Kind: domain.StickerSetRefBySystem, SystemKey: "animated_emoji"}, true
	case *tg.InputStickerSetAnimatedEmojiAnimations:
		return domain.StickerSetRef{Kind: domain.StickerSetRefBySystem, SystemKey: "animated_emoji_animations"}, true
	case *tg.InputStickerSetEmojiGenericAnimations:
		return domain.StickerSetRef{Kind: domain.StickerSetRefBySystem, SystemKey: "emoji_generic_animations"}, true
	case *tg.InputStickerSetDice:
		return domain.StickerSetRef{Kind: domain.StickerSetRefBySystem, SystemKey: "dice:" + in.Emoticon}, true
	default:
		return domain.StickerSetRef{}, false
	}
}

// mediaCatalogHash 用一组 int64（文档/集合 id）算稳定 hash，供 *NotModified 缓存判定。
func mediaCatalogHash(values []int64) int64 {
	var hash uint64
	for _, v := range values {
		hash ^= uint64(v)
		hash = hash*0x4f25 + uint64(v)
	}
	return int64(hash & 0x7fffffffffffffff)
}
