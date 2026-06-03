package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// LangPackStore 用 PostgreSQL 实现 store.LangPackStore。
type LangPackStore struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

// NewLangPackStore 基于 pgx 连接池（或事务）创建 LangPackStore。
func NewLangPackStore(db sqlcgen.DBTX) *LangPackStore {
	return &LangPackStore{db: db, q: sqlcgen.New(db)}
}

func (s *LangPackStore) GetPack(ctx context.Context, langPack, langCode string, fromVersion int) (domain.LangPack, error) {
	meta, found, err := s.meta(ctx, langPack, langCode)
	if err != nil || !found {
		return meta, err
	}
	meta.FromVersion = fromVersion
	if meta.Version <= fromVersion {
		return meta, nil
	}
	rows, err := s.q.ListLangPackStrings(ctx, sqlcgen.ListLangPackStringsParams{
		LangPack: langPack,
		LangCode: langCode,
	})
	if err != nil {
		return domain.LangPack{}, fmt.Errorf("list lang pack strings: %w", err)
	}
	meta.Strings = make([]domain.LangPackString, 0, len(rows))
	for _, row := range rows {
		meta.Strings = append(meta.Strings, langPackStringFromListRow(row))
	}
	return meta, nil
}

func (s *LangPackStore) GetStrings(ctx context.Context, langPack, langCode string, keys []string) (domain.LangPack, error) {
	meta, found, err := s.meta(ctx, langPack, langCode)
	if err != nil || !found {
		return meta, err
	}
	if len(keys) == 0 {
		return s.GetPack(ctx, langPack, langCode, 0)
	}
	rows, err := s.q.GetLangPackStringsByKeys(ctx, sqlcgen.GetLangPackStringsByKeysParams{
		LangPack: langPack,
		LangCode: langCode,
		Keys:     keys,
	})
	if err != nil {
		return domain.LangPack{}, fmt.Errorf("get lang pack strings: %w", err)
	}
	meta.Strings = make([]domain.LangPackString, 0, len(rows))
	for _, row := range rows {
		meta.Strings = append(meta.Strings, langPackStringFromKeysRow(row))
	}
	return meta, nil
}

func (s *LangPackStore) UpsertPack(ctx context.Context, pack domain.LangPack) error {
	if txer, ok := s.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err := txer.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin lang pack upsert: %w", err)
		}
		q := s.q.WithTx(tx)
		if err := upsertPackWith(ctx, q, pack); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit lang pack upsert: %w", err)
		}
		return nil
	}
	return upsertPackWith(ctx, s.q, pack)
}

func upsertPackWith(ctx context.Context, q *sqlcgen.Queries, pack domain.LangPack) error {
	if err := q.UpsertLangPackMeta(ctx, sqlcgen.UpsertLangPackMetaParams{
		LangPack:     pack.LangPack,
		LangCode:     pack.LangCode,
		Version:      int32(pack.Version),
		StringsCount: int32(len(pack.Strings)),
	}); err != nil {
		return fmt.Errorf("upsert lang pack meta: %w", err)
	}
	for _, item := range pack.Strings {
		if err := q.UpsertLangPackString(ctx, sqlcgen.UpsertLangPackStringParams{
			LangPack:   pack.LangPack,
			LangCode:   pack.LangCode,
			Key:        item.Key,
			Version:    int32(pack.Version),
			Pluralized: item.Pluralized,
			Value:      item.Value,
			ZeroValue:  item.ZeroValue,
			OneValue:   item.OneValue,
			TwoValue:   item.TwoValue,
			FewValue:   item.FewValue,
			ManyValue:  item.ManyValue,
			OtherValue: item.OtherValue,
			Deleted:    item.Deleted,
		}); err != nil {
			return fmt.Errorf("upsert lang pack string %q: %w", item.Key, err)
		}
	}
	return nil
}

func (s *LangPackStore) meta(ctx context.Context, langPack, langCode string) (domain.LangPack, bool, error) {
	row, err := s.q.GetLangPackMeta(ctx, sqlcgen.GetLangPackMetaParams{
		LangPack: langPack,
		LangCode: langCode,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.LangPack{LangPack: langPack, LangCode: langCode}, false, nil
		}
		return domain.LangPack{}, false, fmt.Errorf("get lang pack meta: %w", err)
	}
	return domain.LangPack{
		LangPack: row.LangPack,
		LangCode: row.LangCode,
		Version:  int(row.Version),
	}, true, nil
}

func langPackStringFromListRow(row sqlcgen.ListLangPackStringsRow) domain.LangPackString {
	return domain.LangPackString{
		Key:        row.Key,
		Value:      row.Value,
		Pluralized: row.Pluralized,
		ZeroValue:  row.ZeroValue,
		OneValue:   row.OneValue,
		TwoValue:   row.TwoValue,
		FewValue:   row.FewValue,
		ManyValue:  row.ManyValue,
		OtherValue: row.OtherValue,
		Deleted:    row.Deleted,
	}
}

func langPackStringFromKeysRow(row sqlcgen.GetLangPackStringsByKeysRow) domain.LangPackString {
	return domain.LangPackString{
		Key:        row.Key,
		Value:      row.Value,
		Pluralized: row.Pluralized,
		ZeroValue:  row.ZeroValue,
		OneValue:   row.OneValue,
		TwoValue:   row.TwoValue,
		FewValue:   row.FewValue,
		ManyValue:  row.ManyValue,
		OtherValue: row.OtherValue,
		Deleted:    row.Deleted,
	}
}
