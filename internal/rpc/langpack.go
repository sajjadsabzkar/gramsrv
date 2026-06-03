package rpc

import (
	"context"

	"github.com/gotd/td/tg"
)

// registerLangpack 注册 langpack.* RPC handler。
func (r *Router) registerLangpack(d *tg.ServerDispatcher) {
	d.OnLangpackGetLangPack(func(ctx context.Context, req *tg.LangpackGetLangPackRequest) (*tg.LangPackDifference, error) {
		if r.deps.LangPack == nil {
			return &tg.LangPackDifference{LangCode: req.LangCode}, nil
		}
		pack, err := r.deps.LangPack.GetLangPack(ctx, req.LangPack, req.LangCode)
		if err != nil {
			return nil, internalErr()
		}
		return tgLangPackDifference(pack), nil
	})
	d.OnLangpackGetDifference(func(ctx context.Context, req *tg.LangpackGetDifferenceRequest) (*tg.LangPackDifference, error) {
		if r.deps.LangPack == nil {
			return &tg.LangPackDifference{LangCode: req.LangCode, FromVersion: req.FromVersion}, nil
		}
		pack, err := r.deps.LangPack.GetDifference(ctx, req.LangPack, req.LangCode, req.FromVersion)
		if err != nil {
			return nil, internalErr()
		}
		return tgLangPackDifference(pack), nil
	})
	d.OnLangpackGetStrings(func(ctx context.Context, req *tg.LangpackGetStringsRequest) ([]tg.LangPackStringClass, error) {
		if r.deps.LangPack == nil {
			return nil, nil
		}
		pack, err := r.deps.LangPack.GetStrings(ctx, req.LangPack, req.LangCode, req.Keys)
		if err != nil {
			return nil, internalErr()
		}
		return tgLangPackStrings(pack.Strings), nil
	})
}
