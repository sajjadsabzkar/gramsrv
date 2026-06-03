package rpc

import (
	"context"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

// registerStories 注册第一阶段 TDesktop 启动所需 stories.* RPC 兼容响应。
func (r *Router) registerStories(d *tg.ServerDispatcher) {
	d.OnStoriesGetAllStories(func(ctx context.Context, req *tg.StoriesGetAllStoriesRequest) (tg.StoriesAllStoriesClass, error) {
		return tdesktop.AllStories(), nil
	})
	d.OnStoriesGetStoriesArchive(func(ctx context.Context, req *tg.StoriesGetStoriesArchiveRequest) (*tg.StoriesStories, error) {
		return tdesktop.StoriesArchive(), nil
	})
	d.OnStoriesGetPinnedStories(func(ctx context.Context, req *tg.StoriesGetPinnedStoriesRequest) (*tg.StoriesStories, error) {
		return tdesktop.PinnedStories(), nil
	})
	d.OnStoriesGetAlbums(func(ctx context.Context, req *tg.StoriesGetAlbumsRequest) (tg.StoriesAlbumsClass, error) {
		return tdesktop.StoryAlbums(), nil
	})
	d.OnStoriesSendReaction(r.onStoriesSendReaction)
}

func (r *Router) onStoriesSendReaction(ctx context.Context, req *tg.StoriesSendReactionRequest) (tg.UpdatesClass, error) {
	if req.StoryID <= 0 || req.StoryID > domain.MaxMessageBoxID {
		return nil, storyIDInvalidErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if _, err := r.checkedDomainPeerFromInputPeer(ctx, userID, req.Peer); err != nil {
		return nil, err
	}
	if err := validateReactionClass(req.Reaction); err != nil {
		return nil, err
	}
	if parsed, err := domainMessageReactionFromTL(req.Reaction); err == nil {
		if err := r.recordMessageReactionUse(ctx, userID, []domain.MessageReaction{parsed}, req.GetAddToRecent(), int(r.clock.Now().Unix())); err != nil {
			return nil, internalErr()
		}
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}
