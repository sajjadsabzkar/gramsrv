package rpc

import (
	"context"
	"time"

	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

const updatesTooLongNudgeDelay = 300 * time.Millisecond

// registerUpdates 注册 updates.* RPC handler。
func (r *Router) registerUpdates(d *tg.ServerDispatcher) {
	d.OnUpdatesGetState(r.onUpdatesGetState)
	d.OnUpdatesGetDifference(r.onUpdatesGetDifference)
}

// onUpdatesGetState 处理 updates.getState（第一阶段返回零状态）。
func (r *Router) onUpdatesGetState(ctx context.Context) (*tg.UpdatesState, error) {
	id, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if r.deps.Updates == nil {
		r.markSessionReceivesUpdates(ctx, userID)
		return &tg.UpdatesState{Date: int(r.clock.Now().Unix())}, nil
	}
	st, err := r.deps.Updates.GetState(ctx, id, userID)
	if err != nil {
		return nil, internalErr()
	}
	current, err := r.deps.Updates.CurrentState(ctx, userID)
	if err != nil {
		return nil, internalErr()
	}
	r.markSessionReceivesUpdates(ctx, userID)
	if current.Pts > st.Pts {
		r.scheduleCurrentSessionDifferenceNudge(ctx)
	}
	return ptr(tgUpdateState(st)), nil
}

func (r *Router) onUpdatesGetDifference(ctx context.Context, req *tg.UpdatesGetDifferenceRequest) (tg.UpdatesDifferenceClass, error) {
	id, _ := AuthKeyIDFrom(ctx)
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if r.deps.Updates == nil {
		now := int(r.clock.Now().Unix())
		r.markSessionReceivesUpdates(ctx, userID)
		return &tg.UpdatesDifferenceEmpty{Date: now}, nil
	}
	st, err := r.deps.Updates.GetDifference(ctx, id, userID, domain.UpdateState{
		Pts:  req.Pts,
		Qts:  req.Qts,
		Date: req.Date,
	})
	if err != nil {
		return nil, internalErr()
	}
	r.markSessionReceivesUpdates(ctx, userID)
	if len(st.Events) == 0 {
		return &tg.UpdatesDifferenceEmpty{Date: st.State.Date, Seq: st.State.Seq}, nil
	}
	st.Events = r.enrichUpdateEvents(ctx, userID, st.Events)
	return tgUpdatesDifference(st), nil
}

func (r *Router) markSessionReceivesUpdates(ctx context.Context, userID int64) {
	if r.deps.Sessions == nil {
		return
	}
	r.syncSessionChannelMemberships(ctx, userID)
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return
	}
	if scoped, ok := r.scopedSessions(); ok {
		if rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx); ok {
			scoped.SetReceivesUpdatesForAuthKey(rawAuthKeyID, sessionID, true)
			return
		}
	}
	r.deps.Sessions.SetReceivesUpdates(sessionID, true)
}

func (r *Router) scheduleCurrentSessionDifferenceNudge(ctx context.Context) {
	pushCtx := context.Background()
	if sessionID, ok := SessionIDFrom(ctx); ok {
		pushCtx = WithSessionID(pushCtx, sessionID)
	}
	if rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx); ok {
		pushCtx = WithRawAuthKeyID(pushCtx, rawAuthKeyID)
	}
	if authKeyID, ok := AuthKeyIDFrom(ctx); ok {
		pushCtx = WithAuthKeyID(pushCtx, authKeyID)
	}
	time.AfterFunc(updatesTooLongNudgeDelay, func() {
		r.pushCurrentSessionMessage(pushCtx, "push updatesTooLong after getState", &tg.UpdatesTooLong{})
	})
}

func ptr[T any](v T) *T { return &v }
