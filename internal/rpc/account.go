package rpc

import (
	"context"
	"errors"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
	"telesrv/internal/domain"
)

// registerAccount 注册 account.* RPC handler。
func (r *Router) registerAccount(d *tg.ServerDispatcher) {
	d.OnAccountCheckUsername(r.onAccountCheckUsername)
	d.OnAccountUpdateProfile(r.onAccountUpdateProfile)
	d.OnAccountUpdateUsername(r.onAccountUpdateUsername)
	d.OnAccountGetPassword(func(ctx context.Context) (*tg.AccountPassword, error) {
		if r.deps.Account == nil {
			return tgPassword(domain.PasswordSettings{SecureRandom: []byte("telesrv-tdesktop-dev-secure-rand")}), nil
		}
		userID, _, err := r.currentUserID(ctx)
		if err != nil {
			return nil, internalErr()
		}
		settings, err := r.deps.Account.GetPassword(ctx, userID)
		if err != nil {
			return nil, internalErr()
		}
		return tgPassword(settings), nil
	})
	d.OnAccountGetNotifySettings(func(ctx context.Context, peer tg.InputNotifyPeerClass) (*tg.PeerNotifySettings, error) {
		return tdesktop.NotifySettings(), nil
	})
	d.OnAccountUpdateNotifySettings(func(ctx context.Context, req *tg.AccountUpdateNotifySettingsRequest) (bool, error) {
		return true, nil
	})
	d.OnAccountGetPrivacy(func(ctx context.Context, key tg.InputPrivacyKeyClass) (*tg.AccountPrivacyRules, error) {
		return tdesktop.PrivacyRules(key), nil
	})
	d.OnAccountGetAuthorizations(func(ctx context.Context) (*tg.AccountAuthorizations, error) {
		return tdesktop.Authorizations(), nil
	})
	d.OnAccountGetDefaultEmojiStatuses(func(ctx context.Context, hash int64) (tg.AccountEmojiStatusesClass, error) {
		return tdesktop.DefaultEmojiStatuses(), nil
	})
	d.OnAccountGetCollectibleEmojiStatuses(func(ctx context.Context, hash int64) (tg.AccountEmojiStatusesClass, error) {
		return tdesktop.CollectibleEmojiStatuses(), nil
	})
	d.OnAccountGetDefaultGroupPhotoEmojis(func(ctx context.Context, hash int64) (tg.EmojiListClass, error) {
		return tdesktop.DefaultGroupPhotoEmojis(), nil
	})
	d.OnAccountGetConnectedBots(func(ctx context.Context) (*tg.AccountConnectedBots, error) {
		return tdesktop.ConnectedBots(), nil
	})
	d.OnAccountGetReactionsNotifySettings(r.onAccountGetReactionsNotifySettings)
	d.OnAccountSetReactionsNotifySettings(r.onAccountSetReactionsNotifySettings)
	d.OnAccountGetContactSignUpNotification(func(ctx context.Context) (bool, error) {
		return false, nil
	})
	d.OnAccountGetThemes(func(ctx context.Context, req *tg.AccountGetThemesRequest) (tg.AccountThemesClass, error) {
		return tdesktop.AccountThemes(), nil
	})
	d.OnAccountGetContentSettings(func(ctx context.Context) (*tg.AccountContentSettings, error) {
		return tdesktop.ContentSettings(), nil
	})
	d.OnAccountGetGlobalPrivacySettings(func(ctx context.Context) (*tg.GlobalPrivacySettings, error) {
		return tdesktop.GlobalPrivacySettings(), nil
	})
	d.OnAccountGetPasskeys(func(ctx context.Context) (*tg.AccountPasskeys, error) {
		return tdesktop.Passkeys(), nil
	})
	d.OnAccountGetSavedMusicIDs(func(ctx context.Context, hash int64) (tg.AccountSavedMusicIDsClass, error) {
		if _, _, err := r.currentUserID(ctx); err != nil {
			return nil, internalErr()
		}
		return &tg.AccountSavedMusicIDs{IDs: []int64{}}, nil
	})
	d.OnAccountGetAccountTTL(r.onAccountGetAccountTTL)
	d.OnAccountUpdateStatus(r.onAccountUpdateStatus)
}

func (r *Router) onAccountGetAccountTTL(ctx context.Context) (*tg.AccountDaysTTL, error) {
	if _, _, err := r.currentUserID(ctx); err != nil {
		return nil, internalErr()
	}
	return &tg.AccountDaysTTL{Days: 365}, nil
}

func (r *Router) onAccountUpdateStatus(ctx context.Context, offline bool) (bool, error) {
	userID, authorized, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	if !authorized || userID == 0 {
		return true, nil
	}
	status := r.setPresenceFromContext(ctx, userID, offline)
	r.pushUserStatus(ctx, userID, status)
	return true, nil
}

type accountReactionSettingsService interface {
	GetReactionSettings(ctx context.Context, userID int64) (domain.AccountReactionSettings, error)
	SetReactionsNotifySettings(ctx context.Context, userID int64, settings domain.ReactionsNotifySettings) (domain.AccountReactionSettings, error)
}

func (r *Router) onAccountGetReactionsNotifySettings(ctx context.Context) (*tg.ReactionsNotifySettings, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if svc, ok := r.deps.Account.(accountReactionSettingsService); ok {
		settings, err := svc.GetReactionSettings(ctx, userID)
		if err != nil {
			return nil, internalErr()
		}
		return tgReactionsNotifySettings(settings.Notify), nil
	}
	return tgReactionsNotifySettings(domain.DefaultAccountReactionSettings().Notify), nil
}

func (r *Router) onAccountSetReactionsNotifySettings(ctx context.Context, settings tg.ReactionsNotifySettings) (*tg.ReactionsNotifySettings, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	notify := domainReactionsNotifySettings(settings)
	if svc, ok := r.deps.Account.(accountReactionSettingsService); ok {
		next, err := svc.SetReactionsNotifySettings(ctx, userID, notify)
		if err != nil {
			return nil, internalErr()
		}
		return tgReactionsNotifySettings(next.Notify), nil
	}
	return tgReactionsNotifySettings(notify), nil
}

func domainReactionsNotifySettings(settings tg.ReactionsNotifySettings) domain.ReactionsNotifySettings {
	return domain.ReactionsNotifySettings{
		MessagesFrom:  domainReactionNotifyFrom(settings.GetMessagesNotifyFrom),
		StoriesFrom:   domainReactionNotifyFrom(settings.GetStoriesNotifyFrom),
		PollVotesFrom: domainReactionNotifyFrom(settings.GetPollVotesNotifyFrom),
		ShowPreviews:  settings.ShowPreviews,
	}
}

func domainReactionNotifyFrom(get func() (tg.ReactionNotificationsFromClass, bool)) domain.ReactionNotifyFrom {
	if get == nil {
		return domain.ReactionNotifyFromNone
	}
	value, ok := get()
	if !ok || value == nil {
		return domain.ReactionNotifyFromNone
	}
	switch value.(type) {
	case *tg.ReactionNotificationsFromAll:
		return domain.ReactionNotifyFromAll
	case *tg.ReactionNotificationsFromContacts:
		return domain.ReactionNotifyFromContacts
	default:
		return domain.ReactionNotifyFromNone
	}
}

func tgReactionsNotifySettings(settings domain.ReactionsNotifySettings) *tg.ReactionsNotifySettings {
	out := &tg.ReactionsNotifySettings{
		Sound:        &tg.NotificationSoundDefault{},
		ShowPreviews: settings.ShowPreviews,
	}
	if value := tgReactionNotifyFrom(settings.MessagesFrom); value != nil {
		out.SetMessagesNotifyFrom(value)
	}
	if value := tgReactionNotifyFrom(settings.StoriesFrom); value != nil {
		out.SetStoriesNotifyFrom(value)
	}
	if value := tgReactionNotifyFrom(settings.PollVotesFrom); value != nil {
		out.SetPollVotesNotifyFrom(value)
	}
	return out
}

func tgReactionNotifyFrom(value domain.ReactionNotifyFrom) tg.ReactionNotificationsFromClass {
	switch value {
	case domain.ReactionNotifyFromAll:
		return &tg.ReactionNotificationsFromAll{}
	case domain.ReactionNotifyFromContacts:
		return &tg.ReactionNotificationsFromContacts{}
	default:
		return nil
	}
}

func (r *Router) onAccountUpdateProfile(ctx context.Context, req *tg.AccountUpdateProfileRequest) (tg.UserClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	svc, ok := r.deps.Users.(UserIdentityService)
	if !ok {
		return nil, internalErr()
	}
	firstName, hasFirstName := req.GetFirstName()
	lastName, hasLastName := req.GetLastName()
	about, hasAbout := req.GetAbout()
	u, err := svc.UpdateProfile(ctx, userID, domain.UserProfileUpdate{
		FirstName:    firstName,
		HasFirstName: hasFirstName,
		LastName:     lastName,
		HasLastName:  hasLastName,
		About:        about,
		HasAbout:     hasAbout,
	})
	if err != nil {
		return nil, profileErr(err)
	}
	r.pushUsernameUpdate(ctx, u)
	return r.tgSelfUser(u), nil
}

func (r *Router) onAccountCheckUsername(ctx context.Context, username string) (bool, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return false, internalErr()
	}
	svc, ok := r.deps.Users.(UserIdentityService)
	if !ok {
		return false, internalErr()
	}
	okUsername, err := svc.CheckUsername(ctx, userID, username)
	if err != nil {
		return false, usernameErr(err)
	}
	return okUsername, nil
}

func (r *Router) onAccountUpdateUsername(ctx context.Context, username string) (tg.UserClass, error) {
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	svc, ok := r.deps.Users.(UserIdentityService)
	if !ok {
		return nil, internalErr()
	}
	u, err := svc.UpdateUsername(ctx, userID, username)
	if err != nil {
		return nil, usernameErr(err)
	}
	r.pushUsernameUpdate(ctx, u)
	return r.tgSelfUser(u), nil
}

func (r *Router) pushUsernameUpdate(ctx context.Context, u domain.User) {
	if u.ID == 0 {
		return
	}
	r.pushUserUpdates(ctx, u.ID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateUserName{
			UserID:    u.ID,
			FirstName: u.FirstName,
			LastName:  u.LastName,
			Usernames: tgUsernames(u.Username),
		}},
		Users: []tg.UserClass{r.tgSelfUser(u)},
		Date:  int(r.clock.Now().Unix()),
	})
}

func usernameErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrUsernameInvalid):
		return usernameInvalidErr()
	case errors.Is(err, domain.ErrUsernameOccupied):
		return usernameOccupiedErr()
	case errors.Is(err, domain.ErrUsernameNotOccupied):
		return usernameNotOccupiedErr()
	default:
		return internalErr()
	}
}

func profileErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrFirstNameInvalid):
		return firstNameInvalidErr()
	case errors.Is(err, domain.ErrAboutTooLong):
		return aboutTooLongErr()
	default:
		return internalErr()
	}
}
