package help

import (
	"context"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const tdesktopClient = "tdesktop"
const tdesktopDefaultAppConfig = `{"chat_read_mark_expire_period":604800,"chat_read_mark_size_threshold":50,"pm_read_date_expire_period":604800,"quote_length_max":1024,"telegram_antispam_group_size_min":200,"telegram_antispam_user_id":"5434988373","reactions_default":{"_":"reactionEmoji","emoticon":"👍"},"reactions_uniq_max":11,"reactions_user_max_default":1,"reactions_in_chat_max":3}`

// Service 提供客户端启动配置与国家区号目录。
type Service struct {
	appConfigs store.AppConfigStore
	countries  store.CountryStore
}

// NewService 创建 help 服务。
func NewService(appConfigs store.AppConfigStore, countries store.CountryStore) *Service {
	return &Service{appConfigs: appConfigs, countries: countries}
}

// GetAppConfig 返回 TDesktop app config，hash 命中时返回 notModified。
func (s *Service) GetAppConfig(ctx context.Context, hash int) (domain.AppConfig, bool, error) {
	if s == nil || s.appConfigs == nil {
		cfg := domain.AppConfig{Client: tdesktopClient, Hash: 5, JSON: []byte(tdesktopDefaultAppConfig)}
		return cfg, hash == cfg.Hash, nil
	}
	cfg, found, err := s.appConfigs.GetAppConfig(ctx, tdesktopClient)
	if err != nil {
		return domain.AppConfig{}, false, err
	}
	if !found {
		cfg = domain.AppConfig{Client: tdesktopClient, Hash: 5, JSON: []byte(tdesktopDefaultAppConfig)}
	}
	return cfg, hash != 0 && hash == cfg.Hash, nil
}

// GetCountries 返回国家区号目录，hash 命中时返回 notModified。
func (s *Service) GetCountries(ctx context.Context, langCode string, hash int) (domain.CountriesList, bool, error) {
	if s == nil || s.countries == nil {
		list := defaultCountries()
		return list, hash != 0 && hash == list.Hash, nil
	}
	list, err := s.countries.ListCountries(ctx, langCode)
	if err != nil {
		return domain.CountriesList{}, false, err
	}
	if len(list.Countries) == 0 {
		list = defaultCountries()
	}
	return list, hash != 0 && hash == list.Hash, nil
}

func defaultCountries() domain.CountriesList {
	return domain.CountriesList{
		Hash: 1,
		Countries: []domain.Country{
			{
				ISO2:        "US",
				DefaultName: "United States",
				CountryCodes: []domain.CountryCode{
					{CountryCode: "1", Prefixes: []string{"1"}},
				},
			},
			{
				ISO2:        "CN",
				DefaultName: "China",
				CountryCodes: []domain.CountryCode{
					{CountryCode: "86", Prefixes: []string{"86"}},
				},
			},
		},
	}
}
