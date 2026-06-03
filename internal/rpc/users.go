package rpc

import (
	"context"
	"errors"

	"github.com/gotd/td/tg"

	"telesrv/internal/app/users"
	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

const maxSavedMusicLimit = 100

// registerUsers 注册 users.* RPC handler。
func (r *Router) registerUsers(d *tg.ServerDispatcher) {
	d.OnUsersGetUsers(r.onUsersGetUsers)
	d.OnUsersGetFullUser(r.onUsersGetFullUser)
	d.OnUsersGetSavedMusic(r.onUsersGetSavedMusic)
	d.OnUsersGetSavedMusicByID(r.onUsersGetSavedMusicByID)
}

// onUsersGetUsers 处理 users.getUsers：支持 self 和已知 user peer（含 777000 官方账号）。
func (r *Router) onUsersGetUsers(ctx context.Context, ids []tg.InputUserClass) ([]tg.UserClass, error) {
	currentUserID, authorized, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	out := make([]tg.UserClass, 0, len(ids))
	for _, in := range ids {
		if r.deps.Users == nil {
			continue
		}
		switch v := in.(type) {
		case *tg.InputUserSelf:
			if !authorized {
				continue
			}
			u, err := r.deps.Users.Self(ctx, currentUserID)
			if err != nil {
				if errors.Is(err, users.ErrNotAuthorized) {
					continue // 未登录：getUsers 尽力而为，跳过 self
				}
				return nil, internalErr()
			}
			out = append(out, r.tgSelfUser(u))
		case *tg.InputUser:
			if !authorized {
				continue
			}
			u, found, err := r.deps.Users.ByID(ctx, currentUserID, v.UserID)
			if err != nil {
				if errors.Is(err, users.ErrNotAuthorized) {
					continue
				}
				return nil, internalErr()
			}
			if !found || (v.AccessHash != 0 && v.AccessHash != u.AccessHash) {
				continue
			}
			out = append(out, r.tgUser(u))
		}
	}
	return out, nil
}

func (r *Router) onUsersGetFullUser(ctx context.Context, id tg.InputUserClass) (*tg.UsersUserFull, error) {
	if r.deps.Users == nil {
		return emptyUserFull(), nil
	}
	currentUserID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	u, found, err := r.userFromInput(ctx, currentUserID, id)
	if err != nil {
		if errors.Is(err, users.ErrNotAuthorized) {
			return emptyUserFull(), nil
		}
		return nil, internalErr()
	}
	if !found {
		return emptyUserFull(), nil
	}
	user := r.tgUser(u)
	if _, ok := id.(*tg.InputUserSelf); ok {
		user = r.tgSelfUser(u)
	}
	full := tg.UserFull{
		ID:             u.ID,
		About:          u.About,
		Settings:       tg.PeerSettings{},
		NotifySettings: *tdesktop.NotifySettings(),
	}
	if r.deps.Channels != nil && u.ID != currentUserID {
		common, err := r.deps.Channels.CommonChannels(ctx, currentUserID, domain.CommonChannelsRequest{
			UserID:       currentUserID,
			TargetUserID: u.ID,
			Limit:        1,
			CountOnly:    true,
		})
		if err != nil {
			return nil, internalErr()
		}
		full.CommonChatsCount = common.Count
	}
	return &tg.UsersUserFull{
		FullUser: full,
		Users:    []tg.UserClass{user},
	}, nil
}

func (r *Router) onUsersGetSavedMusic(ctx context.Context, req *tg.UsersGetSavedMusicRequest) (tg.UsersSavedMusicClass, error) {
	if req == nil || req.Offset < 0 || req.Limit < 0 || req.Limit > maxSavedMusicLimit {
		return nil, limitInvalidErr()
	}
	if err := r.validateInputUser(ctx, req.ID); err != nil {
		return nil, err
	}
	return &tg.UsersSavedMusic{
		Count:     0,
		Documents: []tg.DocumentClass{},
	}, nil
}

func (r *Router) onUsersGetSavedMusicByID(ctx context.Context, req *tg.UsersGetSavedMusicByIDRequest) (tg.UsersSavedMusicClass, error) {
	if req == nil || len(req.Documents) > maxSavedMusicLimit {
		return nil, limitInvalidErr()
	}
	if err := r.validateInputUser(ctx, req.ID); err != nil {
		return nil, err
	}
	return &tg.UsersSavedMusic{
		Count:     0,
		Documents: []tg.DocumentClass{},
	}, nil
}

func emptyUserFull() *tg.UsersUserFull {
	return &tg.UsersUserFull{
		FullUser: tg.UserFull{
			Settings:       tg.PeerSettings{},
			NotifySettings: *tdesktop.NotifySettings(),
		},
	}
}

func (r *Router) userFromInput(ctx context.Context, currentUserID int64, id tg.InputUserClass) (domain.User, bool, error) {
	switch v := id.(type) {
	case *tg.InputUserSelf:
		u, err := r.deps.Users.Self(ctx, currentUserID)
		return u, err == nil, err
	case *tg.InputUser:
		u, found, err := r.deps.Users.ByID(ctx, currentUserID, v.UserID)
		if err != nil || !found {
			return domain.User{}, found, err
		}
		if v.AccessHash != 0 && v.AccessHash != u.AccessHash {
			return domain.User{}, false, nil
		}
		return u, true, nil
	default:
		return domain.User{}, false, nil
	}
}

func (r *Router) validateInputUser(ctx context.Context, id tg.InputUserClass) error {
	if r.deps.Users == nil {
		return nil
	}
	currentUserID, _, err := r.currentUserID(ctx)
	if err != nil {
		return internalErr()
	}
	_, found, err := r.userFromInput(ctx, currentUserID, id)
	if err != nil {
		if errors.Is(err, users.ErrNotAuthorized) {
			return userIDInvalidErr()
		}
		return internalErr()
	}
	if !found {
		return userIDInvalidErr()
	}
	return nil
}
