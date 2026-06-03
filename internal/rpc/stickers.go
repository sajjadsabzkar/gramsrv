package rpc

import (
	"context"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

// 本文件把 reaction / sticker 资源 RPC 接到真实 seed 数据（documents / sticker_sets /
// available_reactions）；Files 服务缺失或资源未导入时回退到 tdesktop 兼容 stub。

func (r *Router) onMessagesGetAvailableReactions(ctx context.Context, hash int) (tg.MessagesAvailableReactionsClass, error) {
	if r.deps.Files == nil {
		return tdesktop.AvailableReactions(hash), nil
	}
	reactions, err := r.deps.Files.ListAvailableReactions(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if len(reactions) == 0 {
		return tdesktop.AvailableReactions(hash), nil
	}
	catalogHash := availableReactionsHash(reactions)
	if hash == catalogHash {
		return &tg.MessagesAvailableReactionsNotModified{}, nil
	}
	docs, err := r.deps.Files.GetDocuments(ctx, reactionDocumentIDs(reactions))
	if err != nil {
		return nil, internalErr()
	}
	return tgAvailableReactions(reactions, documentsByID(docs), catalogHash), nil
}

func (r *Router) onMessagesGetStickerSet(ctx context.Context, req *tg.MessagesGetStickerSetRequest) (tg.MessagesStickerSetClass, error) {
	if r.deps.Files == nil {
		return tdesktop.StickerSet(req), nil
	}
	ref, ok := stickerSetRefFromInput(req.Stickerset)
	if !ok {
		return tdesktop.StickerSet(req), nil
	}
	set, docs, found, err := r.deps.Files.ResolveStickerSet(ctx, ref)
	if err != nil {
		return nil, internalErr()
	}
	if !found {
		// 未 seed 的系统集 / 未知短名：回退兼容 stub，避免破坏客户端。
		return tdesktop.StickerSet(req), nil
	}
	if req.Hash != 0 && req.Hash == set.Hash {
		return &tg.MessagesStickerSetNotModified{}, nil
	}
	return tgMessagesStickerSet(set, docs), nil
}

func (r *Router) onMessagesGetAllStickers(ctx context.Context, hash int64) (tg.MessagesAllStickersClass, error) {
	return r.allStickersForKind(ctx, hash, domain.StickerSetKindStickers)
}

func (r *Router) onMessagesGetEmojiStickers(ctx context.Context, hash int64) (tg.MessagesAllStickersClass, error) {
	return r.allStickersForKind(ctx, hash, domain.StickerSetKindEmoji)
}

func (r *Router) allStickersForKind(ctx context.Context, hash int64, kind domain.StickerSetKind) (tg.MessagesAllStickersClass, error) {
	if r.deps.Files == nil {
		return messagesAllStickersEmpty(hash), nil
	}
	sets, err := r.deps.Files.ListStickerSets(ctx, kind)
	if err != nil {
		return nil, internalErr()
	}
	if len(sets) == 0 {
		return messagesAllStickersEmpty(hash), nil
	}
	catalogHash := stickerSetsCatalogHash(sets)
	if hash == catalogHash {
		return &tg.MessagesAllStickersNotModified{}, nil
	}
	return &tg.MessagesAllStickers{Hash: catalogHash, Sets: tgStickerSets(sets)}, nil
}

func documentsByID(docs []domain.Document) map[int64]domain.Document {
	m := make(map[int64]domain.Document, len(docs))
	for _, d := range docs {
		m[d.ID] = d
	}
	return m
}

// availableReactionsHash 用 reaction 的核心字段算稳定 hash（供 *NotModified 缓存判定）。
func availableReactionsHash(reactions []domain.AvailableReaction) int {
	values := make([]int64, 0, len(reactions)*10)
	for _, r := range reactions {
		values = append(values,
			int64(len([]rune(r.Reaction))),
			boolHashValue(r.Inactive),
			boolHashValue(r.Premium),
			r.StaticIconID,
			r.AppearAnimationID,
			r.SelectAnimationID,
			r.ActivateAnimationID,
			r.EffectAnimationID,
			r.AroundAnimationID,
			r.CenterIconID,
		)
	}
	return int(tdesktopCountHash(values) & 0x7fffffff)
}

func stickerSetsCatalogHash(sets []domain.StickerSet) int64 {
	values := make([]int64, 0, len(sets))
	for _, set := range sets {
		if set.ID == 0 {
			return 0
		}
		if set.Archived {
			continue
		}
		values = append(values, int64(set.Hash))
	}
	return int64(tdesktopCountHash(values))
}

func boolHashValue(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func tdesktopCountHash(values []int64) uint64 {
	var hash uint64
	for _, value := range values {
		hash = tdesktopHashUpdate(hash, value)
	}
	return hash
}

func tdesktopHashUpdate(hash uint64, value int64) uint64 {
	hash ^= hash >> 21
	hash ^= hash << 35
	hash ^= hash >> 4
	hash += uint64(value)
	return hash
}
