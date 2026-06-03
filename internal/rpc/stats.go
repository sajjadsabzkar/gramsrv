package rpc

import (
	"context"
	"fmt"

	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

const (
	maxStatsPublicForwardsLimit = 100
	maxStatsOffsetLength        = 128
	maxStatsGraphTokenLength    = 128
)

func (r *Router) registerStats(d *tg.ServerDispatcher) {
	d.OnStatsGetBroadcastStats(r.onStatsGetBroadcastStats)
	d.OnStatsGetMegagroupStats(r.onStatsGetMegagroupStats)
	d.OnStatsGetMessageStats(r.onStatsGetMessageStats)
	d.OnStatsGetMessagePublicForwards(r.onStatsGetMessagePublicForwards)
	d.OnStatsLoadAsyncGraph(r.onStatsLoadAsyncGraph)
	d.OnStatsGetStoryStats(r.onStatsGetStoryStats)
	d.OnStatsGetStoryPublicForwards(r.onStatsGetStoryPublicForwards)
	d.OnStatsGetPollStats(r.onStatsGetPollStats)
}

func (r *Router) onStatsGetBroadcastStats(ctx context.Context, req *tg.StatsGetBroadcastStatsRequest) (*tg.StatsBroadcastStats, error) {
	view, err := r.statsChannelView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	if !view.Channel.Broadcast {
		return nil, tgerr400("BROADCAST_REQUIRED")
	}
	return r.emptyBroadcastStats(), nil
}

func (r *Router) onStatsGetMegagroupStats(ctx context.Context, req *tg.StatsGetMegagroupStatsRequest) (*tg.StatsMegagroupStats, error) {
	view, err := r.statsChannelView(ctx, req.Channel)
	if err != nil {
		return nil, err
	}
	if !view.Channel.Megagroup {
		return nil, tgerr400("MEGAGROUP_REQUIRED")
	}
	return r.emptyMegagroupStats(), nil
}

func (r *Router) onStatsGetMessageStats(ctx context.Context, req *tg.StatsGetMessageStatsRequest) (*tg.StatsMessageStats, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if _, err := r.statsChannelView(ctx, req.Channel); err != nil {
		return nil, err
	}
	return &tg.StatsMessageStats{
		ViewsGraph:              r.emptyStatsGraph("Views"),
		ReactionsByEmotionGraph: r.emptyStatsGraph("Reactions"),
	}, nil
}

func (r *Router) onStatsGetMessagePublicForwards(ctx context.Context, req *tg.StatsGetMessagePublicForwardsRequest) (*tg.StatsPublicForwards, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if req.Limit < 0 || req.Limit > maxStatsPublicForwardsLimit || len(req.Offset) > maxStatsOffsetLength {
		return nil, limitInvalidErr()
	}
	if _, err := r.statsChannelView(ctx, req.Channel); err != nil {
		return nil, err
	}
	return emptyStatsPublicForwards(), nil
}

func (r *Router) onStatsLoadAsyncGraph(_ context.Context, req *tg.StatsLoadAsyncGraphRequest) (tg.StatsGraphClass, error) {
	if len(req.Token) > maxStatsGraphTokenLength {
		return &tg.StatsGraphError{Error: "GRAPH_INVALID_RELOAD"}, nil
	}
	return &tg.StatsGraphError{Error: "GRAPH_INVALID_RELOAD"}, nil
}

func (r *Router) onStatsGetStoryStats(ctx context.Context, req *tg.StatsGetStoryStatsRequest) (*tg.StatsStoryStats, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := r.validateStatsPeer(ctx, req.Peer); err != nil {
		return nil, err
	}
	return &tg.StatsStoryStats{
		ViewsGraph:              r.emptyStatsGraph("Views"),
		ReactionsByEmotionGraph: r.emptyStatsGraph("Reactions"),
	}, nil
}

func (r *Router) onStatsGetStoryPublicForwards(ctx context.Context, req *tg.StatsGetStoryPublicForwardsRequest) (*tg.StatsPublicForwards, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if req.Limit < 0 || req.Limit > maxStatsPublicForwardsLimit || len(req.Offset) > maxStatsOffsetLength {
		return nil, limitInvalidErr()
	}
	if err := r.validateStatsPeer(ctx, req.Peer); err != nil {
		return nil, err
	}
	return emptyStatsPublicForwards(), nil
}

func (r *Router) onStatsGetPollStats(ctx context.Context, req *tg.StatsGetPollStatsRequest) (*tg.StatsPollStats, error) {
	if req.MsgID <= 0 || req.MsgID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if err := r.validateStatsPeer(ctx, req.Peer); err != nil {
		return nil, err
	}
	return &tg.StatsPollStats{VotesGraph: r.emptyStatsGraph("Votes")}, nil
}

func (r *Router) statsChannelView(ctx context.Context, input tg.InputChannelClass) (domain.ChannelView, error) {
	_, view, err := r.channelView(ctx, input)
	if err != nil {
		return domain.ChannelView{}, err
	}
	if view.Self.Role != domain.ChannelRoleCreator && view.Self.Role != domain.ChannelRoleAdmin {
		return domain.ChannelView{}, tgerr400("CHAT_ADMIN_REQUIRED")
	}
	return view, nil
}

func (r *Router) validateStatsPeer(ctx context.Context, peer tg.InputPeerClass) error {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return internalErr()
	}
	_, err = r.checkedDomainPeerFromInputPeer(ctx, userID, peer)
	return err
}

func (r *Router) emptyBroadcastStats() *tg.StatsBroadcastStats {
	return &tg.StatsBroadcastStats{
		Period:                       r.statsDateRange(),
		Followers:                    tg.StatsAbsValueAndPrev{},
		ViewsPerPost:                 tg.StatsAbsValueAndPrev{},
		SharesPerPost:                tg.StatsAbsValueAndPrev{},
		ReactionsPerPost:             tg.StatsAbsValueAndPrev{},
		ViewsPerStory:                tg.StatsAbsValueAndPrev{},
		SharesPerStory:               tg.StatsAbsValueAndPrev{},
		ReactionsPerStory:            tg.StatsAbsValueAndPrev{},
		EnabledNotifications:         tg.StatsPercentValue{},
		GrowthGraph:                  r.emptyStatsGraph("Growth"),
		FollowersGraph:               r.emptyStatsGraph("Followers"),
		MuteGraph:                    r.emptyStatsGraph("Muted"),
		TopHoursGraph:                r.emptyStatsGraph("Hours"),
		InteractionsGraph:            r.emptyStatsGraph("Interactions"),
		IvInteractionsGraph:          r.emptyStatsGraph("Instant Views"),
		ViewsBySourceGraph:           r.emptyStatsGraph("Views"),
		NewFollowersBySourceGraph:    r.emptyStatsGraph("Followers"),
		LanguagesGraph:               r.emptyStatsGraph("Languages"),
		ReactionsByEmotionGraph:      r.emptyStatsGraph("Reactions"),
		StoryInteractionsGraph:       r.emptyStatsGraph("Stories"),
		StoryReactionsByEmotionGraph: r.emptyStatsGraph("Story Reactions"),
	}
}

func (r *Router) emptyMegagroupStats() *tg.StatsMegagroupStats {
	return &tg.StatsMegagroupStats{
		Period:                  r.statsDateRange(),
		Members:                 tg.StatsAbsValueAndPrev{},
		Messages:                tg.StatsAbsValueAndPrev{},
		Viewers:                 tg.StatsAbsValueAndPrev{},
		Posters:                 tg.StatsAbsValueAndPrev{},
		GrowthGraph:             r.emptyStatsGraph("Growth"),
		MembersGraph:            r.emptyStatsGraph("Members"),
		NewMembersBySourceGraph: r.emptyStatsGraph("Members"),
		LanguagesGraph:          r.emptyStatsGraph("Languages"),
		MessagesGraph:           r.emptyStatsGraph("Messages"),
		ActionsGraph:            r.emptyStatsGraph("Actions"),
		TopHoursGraph:           r.emptyStatsGraph("Hours"),
		WeekdaysGraph:           r.emptyStatsGraph("Weekdays"),
		TopPosters:              []tg.StatsGroupTopPoster{},
		TopAdmins:               []tg.StatsGroupTopAdmin{},
		TopInviters:             []tg.StatsGroupTopInviter{},
	}
}

func (r *Router) statsDateRange() tg.StatsDateRangeDays {
	now := int(r.clock.Now().Unix())
	return tg.StatsDateRangeDays{MinDate: now - 86400, MaxDate: now}
}

func (r *Router) emptyStatsGraph(label string) *tg.StatsGraph {
	nowMillis := r.clock.Now().UnixMilli()
	prevMillis := nowMillis - 86400000
	data := fmt.Sprintf(
		`{"columns":[["x",%d,%d],["y0",0,0]],"types":{"x":"x","y0":"line"},"names":{"y0":%q},"colors":{"y0":"blue#4a90e2"}}`,
		prevMillis,
		nowMillis,
		label,
	)
	return &tg.StatsGraph{JSON: tg.DataJSON{Data: data}}
}

func emptyStatsPublicForwards() *tg.StatsPublicForwards {
	return &tg.StatsPublicForwards{
		Count:    0,
		Forwards: []tg.PublicForwardClass{},
		Chats:    []tg.ChatClass{},
		Users:    []tg.UserClass{},
	}
}
