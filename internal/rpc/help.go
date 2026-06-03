package rpc

import (
	"context"

	"github.com/gotd/td/tg"

	"telesrv/internal/compat/tdesktop"
)

// registerHelp 注册 help.* RPC handler（DC 配置、最近 DC）。
func (r *Router) registerHelp(d *tg.ServerDispatcher) {
	d.OnHelpGetConfig(func(ctx context.Context) (*tg.Config, error) {
		return tdesktop.BuildConfig(r.cfg.DC, r.cfg.IP, r.cfg.Port, r.clock.Now()), nil
	})
	d.OnHelpGetNearestDC(func(ctx context.Context) (*tg.NearestDC, error) {
		return tdesktop.NearestDC(r.cfg.DC), nil
	})
	d.OnHelpGetAppConfig(func(ctx context.Context, hash int) (tg.HelpAppConfigClass, error) {
		if r.deps.Help == nil {
			return tdesktop.AppConfig(hash), nil
		}
		cfg, notModified, err := r.deps.Help.GetAppConfig(ctx, hash)
		if err != nil {
			return nil, internalErr()
		}
		if notModified {
			return &tg.HelpAppConfigNotModified{}, nil
		}
		return &tg.HelpAppConfig{Hash: cfg.Hash, Config: tgJSONValue(cfg.JSON)}, nil
	})
	d.OnHelpGetCountriesList(func(ctx context.Context, req *tg.HelpGetCountriesListRequest) (tg.HelpCountriesListClass, error) {
		if r.deps.Help == nil {
			return tdesktop.CountriesList(req.Hash), nil
		}
		list, notModified, err := r.deps.Help.GetCountries(ctx, req.LangCode, req.Hash)
		if err != nil {
			return nil, internalErr()
		}
		if notModified {
			return &tg.HelpCountriesListNotModified{}, nil
		}
		return tgCountriesList(list), nil
	})
	d.OnHelpGetTimezonesList(func(ctx context.Context, hash int) (tg.HelpTimezonesListClass, error) {
		return tdesktop.TimezonesList(hash), nil
	})
	d.OnHelpGetPeerColors(func(ctx context.Context, hash int) (tg.HelpPeerColorsClass, error) {
		return tdesktop.PeerColors(), nil
	})
	d.OnHelpGetPeerProfileColors(func(ctx context.Context, hash int) (tg.HelpPeerColorsClass, error) {
		return tdesktop.PeerColors(), nil
	})
	d.OnHelpGetPromoData(func(ctx context.Context) (tg.HelpPromoDataClass, error) {
		return tdesktop.PromoData(r.clock.Now()), nil
	})
	d.OnHelpGetTermsOfServiceUpdate(func(ctx context.Context) (tg.HelpTermsOfServiceUpdateClass, error) {
		return tdesktop.TermsOfServiceUpdate(r.clock.Now()), nil
	})
	d.OnHelpGetPremiumPromo(func(ctx context.Context) (*tg.HelpPremiumPromo, error) {
		return tdesktop.PremiumPromo(), nil
	})
}
