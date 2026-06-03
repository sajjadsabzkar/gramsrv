package rpc

import (
	"context"

	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

const (
	maxPremiumBoostsListLimit   = 100
	maxPremiumBoostsOffsetBytes = 128
	maxPremiumApplyBoostSlots   = 16
)

func (r *Router) registerPremium(d *tg.ServerDispatcher) {
	d.OnPremiumGetBoostsStatus(r.onPremiumGetBoostsStatus)
	d.OnPremiumGetBoostsList(r.onPremiumGetBoostsList)
	d.OnPremiumGetMyBoosts(r.onPremiumGetMyBoosts)
	d.OnPremiumApplyBoost(r.onPremiumApplyBoost)
	d.OnPremiumGetUserBoosts(r.onPremiumGetUserBoosts)
}

func (r *Router) onPremiumGetBoostsStatus(ctx context.Context, peer tg.InputPeerClass) (*tg.PremiumBoostsStatus, error) {
	if _, _, err := r.premiumBoostChannelView(ctx, peer, false); err != nil {
		return nil, err
	}
	return emptyPremiumBoostsStatus(), nil
}

func (r *Router) onPremiumGetBoostsList(ctx context.Context, req *tg.PremiumGetBoostsListRequest) (*tg.PremiumBoostsList, error) {
	if req.Limit < 0 || req.Limit > maxPremiumBoostsListLimit || len(req.Offset) > maxPremiumBoostsOffsetBytes {
		return nil, limitInvalidErr()
	}
	if _, _, err := r.premiumBoostChannelView(ctx, req.Peer, true); err != nil {
		return nil, err
	}
	return emptyPremiumBoostsList(), nil
}

func (r *Router) onPremiumGetMyBoosts(ctx context.Context) (*tg.PremiumMyBoosts, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	return emptyPremiumMyBoosts(), nil
}

func (r *Router) onPremiumApplyBoost(ctx context.Context, req *tg.PremiumApplyBoostRequest) (*tg.PremiumMyBoosts, error) {
	slots, ok := req.GetSlots()
	if !ok {
		return nil, tgerr400("BOOSTS_EMPTY")
	}
	if len(slots) == 0 {
		return nil, tgerr400("SLOTS_EMPTY")
	}
	if len(slots) > maxPremiumApplyBoostSlots {
		return nil, limitInvalidErr()
	}
	for _, slot := range slots {
		if slot < 0 {
			return nil, tgerr400("SLOTS_INVALID")
		}
	}
	if _, _, err := r.premiumBoostChannelView(ctx, req.Peer, false); err != nil {
		return nil, err
	}
	return emptyPremiumMyBoosts(), nil
}

func (r *Router) onPremiumGetUserBoosts(ctx context.Context, req *tg.PremiumGetUserBoostsRequest) (*tg.PremiumBoostsList, error) {
	userID, view, err := r.premiumBoostChannelView(ctx, req.Peer, true)
	if err != nil {
		return nil, err
	}
	if _, err := r.userIDsFromInputUsers(ctx, userID, []tg.InputUserClass{req.UserID}); err != nil {
		return nil, err
	}
	_ = view
	return emptyPremiumBoostsList(), nil
}

func (r *Router) premiumBoostChannelView(ctx context.Context, peer tg.InputPeerClass, requireAdmin bool) (int64, domain.ChannelView, error) {
	ref, ok := premiumBoostChannelRef(peer)
	if !ok {
		return 0, domain.ChannelView{}, peerIDInvalidErr()
	}
	input := &tg.InputChannel{ChannelID: ref.ID}
	if ref.CheckAccessHash {
		input.AccessHash = ref.AccessHash
	}
	userID, view, err := r.channelView(ctx, input)
	if err != nil {
		return 0, domain.ChannelView{}, err
	}
	if requireAdmin && view.Self.Role != domain.ChannelRoleCreator && view.Self.Role != domain.ChannelRoleAdmin {
		return 0, domain.ChannelView{}, tgerr400("CHAT_ADMIN_REQUIRED")
	}
	return userID, view, nil
}

func premiumBoostChannelRef(peer tg.InputPeerClass) (channelInputRef, bool) {
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		return channelInputRef{
			ID:              p.ChannelID,
			AccessHash:      p.AccessHash,
			CheckAccessHash: p.AccessHash != 0,
		}, p.ChannelID > 0
	case *tg.InputPeerChannelFromMessage:
		return channelInputRef{ID: p.ChannelID}, p.ChannelID > 0
	case *tg.InputPeerChat:
		return channelInputRef{ID: p.ChatID}, p.ChatID > 0
	default:
		return channelInputRef{}, false
	}
}

func emptyPremiumBoostsStatus() *tg.PremiumBoostsStatus {
	return &tg.PremiumBoostsStatus{
		Level:              0,
		CurrentLevelBoosts: 0,
		Boosts:             0,
		BoostURL:           "",
	}
}

func emptyPremiumBoostsList() *tg.PremiumBoostsList {
	return &tg.PremiumBoostsList{
		Count:  0,
		Boosts: []tg.Boost{},
		Users:  []tg.UserClass{},
	}
}

func emptyPremiumMyBoosts() *tg.PremiumMyBoosts {
	return &tg.PremiumMyBoosts{
		MyBoosts: []tg.MyBoost{},
		Chats:    []tg.ChatClass{},
		Users:    []tg.UserClass{},
	}
}
