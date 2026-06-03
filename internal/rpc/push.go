package rpc

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
	"go.uber.org/zap"
)

func (r *Router) pushUserMessage(ctx context.Context, userID int64, logMessage string, msg bin.Encoder) int {
	if r.deps.Sessions == nil || userID == 0 || msg == nil {
		return 0
	}
	sessionID, _ := SessionIDFrom(ctx)
	if timeout := r.cfg.OutboundPushTimeout; timeout > 0 {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		if scoped, ok := r.deps.Sessions.(ScopedBestEffortSessionBinder); ok {
			if sent, err := scoped.PushToUserExceptAuthKeySessionBestEffort(ctx, userID, authKeyID, sessionID, proto.MessageFromServer, msg, timeout); err != nil {
				r.log.Debug(logMessage, zap.Int64("user_id", userID), zap.Int("sent", sent), zap.Duration("timeout", timeout), zap.Error(err))
				return sent
			} else {
				return sent
			}
		}
		if bestEffort, ok := r.deps.Sessions.(BestEffortSessionBinder); ok {
			if sent, err := bestEffort.PushToUserExceptSessionBestEffort(ctx, userID, sessionID, proto.MessageFromServer, msg, timeout); err != nil {
				r.log.Debug(logMessage, zap.Int64("user_id", userID), zap.Int("sent", sent), zap.Duration("timeout", timeout), zap.Error(err))
				return sent
			} else {
				return sent
			}
		}
	}
	if scoped, ok := r.scopedSessions(); ok {
		authKeyID, _ := AuthKeyIDFrom(ctx)
		if sent, err := scoped.PushToUserExceptAuthKeySession(ctx, userID, authKeyID, sessionID, proto.MessageFromServer, msg); err != nil {
			r.log.Debug(logMessage, zap.Int64("user_id", userID), zap.Int("sent", sent), zap.Error(err))
			return sent
		} else {
			return sent
		}
	}
	if sent, err := r.deps.Sessions.PushToUserExceptSession(ctx, userID, sessionID, proto.MessageFromServer, msg); err != nil {
		r.log.Debug(logMessage, zap.Int64("user_id", userID), zap.Int("sent", sent), zap.Error(err))
		return sent
	} else {
		return sent
	}
}

func (r *Router) pushCurrentSessionMessage(ctx context.Context, logMessage string, msg bin.Encoder) {
	if r.deps.Sessions == nil || msg == nil {
		return
	}
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return
	}
	if scoped, ok := r.scopedSessions(); ok {
		rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx)
		if !ok {
			return
		}
		if err := scoped.PushToSessionForAuthKey(ctx, rawAuthKeyID, sessionID, proto.MessageFromServer, msg); err != nil {
			r.log.Debug(logMessage, zap.Int64("session_id", sessionID), zap.Error(err))
		}
		return
	}
	if err := r.deps.Sessions.PushToSession(ctx, sessionID, proto.MessageFromServer, msg); err != nil {
		r.log.Debug(logMessage, zap.Int64("session_id", sessionID), zap.Error(err))
	}
}
