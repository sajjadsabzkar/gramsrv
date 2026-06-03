package account

import (
	"context"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

var defaultSecureRandom = []byte("telesrv-tdesktop-dev-secure-rand")

// Service 提供账号安全配置查询。
type Service struct {
	passwords store.PasswordStore
	reactions store.AccountReactionSettingsStore
}

// ServiceOption 调整 account 服务依赖。
type ServiceOption func(*Service)

// WithReactionSettings 注入账号级 reaction 设置持久化。
func WithReactionSettings(reactions store.AccountReactionSettingsStore) ServiceOption {
	return func(s *Service) {
		s.reactions = reactions
	}
}

// NewService 创建 account 服务。
func NewService(passwords store.PasswordStore, opts ...ServiceOption) *Service {
	s := &Service{passwords: passwords}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetPassword 返回当前账号 2FA 配置。未登录或无记录时返回持久化策略的默认 no-password 配置。
func (s *Service) GetPassword(ctx context.Context, userID int64) (domain.PasswordSettings, error) {
	if s == nil || s.passwords == nil || userID == 0 {
		return defaultPasswordSettings(), nil
	}
	settings, found, err := s.passwords.GetByUser(ctx, userID)
	if err != nil {
		return domain.PasswordSettings{}, err
	}
	if !found {
		return defaultPasswordSettings(), nil
	}
	if len(settings.SecureRandom) == 0 {
		settings.SecureRandom = append([]byte(nil), defaultSecureRandom...)
	}
	return settings, nil
}

func defaultPasswordSettings() domain.PasswordSettings {
	return domain.PasswordSettings{SecureRandom: append([]byte(nil), defaultSecureRandom...)}
}

// GetReactionSettings returns account-level reaction preferences.
func (s *Service) GetReactionSettings(ctx context.Context, userID int64) (domain.AccountReactionSettings, error) {
	if s == nil || s.reactions == nil || userID == 0 {
		return domain.DefaultAccountReactionSettings(), nil
	}
	settings, found, err := s.reactions.GetReactionSettings(ctx, userID)
	if err != nil {
		return domain.AccountReactionSettings{}, err
	}
	if !found {
		return domain.DefaultAccountReactionSettings(), nil
	}
	return normalizeReactionSettings(settings), nil
}

// SetReactionsNotifySettings stores reaction notification preferences.
func (s *Service) SetReactionsNotifySettings(ctx context.Context, userID int64, notify domain.ReactionsNotifySettings) (domain.AccountReactionSettings, error) {
	settings, err := s.GetReactionSettings(ctx, userID)
	if err != nil {
		return domain.AccountReactionSettings{}, err
	}
	settings.Notify = normalizeNotifySettings(notify)
	return s.saveReactionSettings(ctx, userID, settings)
}

// SetDefaultReaction stores the account default quick reaction.
func (s *Service) SetDefaultReaction(ctx context.Context, userID int64, reaction domain.MessageReaction) (domain.AccountReactionSettings, error) {
	settings, err := s.GetReactionSettings(ctx, userID)
	if err != nil {
		return domain.AccountReactionSettings{}, err
	}
	if reaction.Type == "" || reaction.Emoticon == "" {
		reaction = domain.DefaultAccountReactionSettings().DefaultReaction
	}
	settings.DefaultReaction = reaction
	return s.saveReactionSettings(ctx, userID, settings)
}

// SetPaidReactionPrivacy stores the account default paid reaction privacy.
func (s *Service) SetPaidReactionPrivacy(ctx context.Context, userID int64, privacy domain.PaidReactionPrivacy) (domain.AccountReactionSettings, error) {
	settings, err := s.GetReactionSettings(ctx, userID)
	if err != nil {
		return domain.AccountReactionSettings{}, err
	}
	settings.PaidPrivacy = normalizePaidPrivacy(privacy)
	return s.saveReactionSettings(ctx, userID, settings)
}

func (s *Service) saveReactionSettings(ctx context.Context, userID int64, settings domain.AccountReactionSettings) (domain.AccountReactionSettings, error) {
	settings = normalizeReactionSettings(settings)
	if s == nil || s.reactions == nil || userID == 0 {
		return settings, nil
	}
	return settings, s.reactions.SaveReactionSettings(ctx, userID, settings)
}

func normalizeReactionSettings(settings domain.AccountReactionSettings) domain.AccountReactionSettings {
	defaults := domain.DefaultAccountReactionSettings()
	settings.Notify = normalizeNotifySettings(settings.Notify)
	if settings.DefaultReaction.Type == "" || settings.DefaultReaction.Emoticon == "" {
		settings.DefaultReaction = defaults.DefaultReaction
	}
	settings.PaidPrivacy = normalizePaidPrivacy(settings.PaidPrivacy)
	return settings
}

func normalizeNotifySettings(settings domain.ReactionsNotifySettings) domain.ReactionsNotifySettings {
	if !validNotifyFrom(settings.MessagesFrom) {
		settings.MessagesFrom = domain.ReactionNotifyFromContacts
	}
	if !validNotifyFrom(settings.StoriesFrom) {
		settings.StoriesFrom = domain.ReactionNotifyFromContacts
	}
	if !validNotifyFrom(settings.PollVotesFrom) {
		settings.PollVotesFrom = domain.ReactionNotifyFromContacts
	}
	return settings
}

func validNotifyFrom(value domain.ReactionNotifyFrom) bool {
	switch value {
	case domain.ReactionNotifyFromNone, domain.ReactionNotifyFromContacts, domain.ReactionNotifyFromAll:
		return true
	default:
		return false
	}
}

func normalizePaidPrivacy(privacy domain.PaidReactionPrivacy) domain.PaidReactionPrivacy {
	switch privacy.Kind {
	case domain.PaidReactionPrivacyAnonymous:
		return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyAnonymous}
	case domain.PaidReactionPrivacyPeer:
		if privacy.Peer != nil && privacy.Peer.ID != 0 {
			peer := *privacy.Peer
			return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyPeer, Peer: &peer}
		}
	}
	return domain.PaidReactionPrivacy{Kind: domain.PaidReactionPrivacyDefault}
}
