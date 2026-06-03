// Package postgres 用 PostgreSQL 实现持久化存储接口（第一阶段：AuthKeyStore）。
//
// 查询代码由 sqlc 生成于 ./sqlcgen（见 telesrv/sqlc.yaml）；本包在其上实现 store 接口。
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // 注册 pgx5:// migrate driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/deploy"
)

const defaultMinConns = 16

// PoolOption 调整 pgxpool 连接池配置。
type PoolOption func(*pgxpool.Config)

// WithMaxConns 设置连接池最大连接数；<=0 时保持 pgx 默认。
// 同时把 MinConns 预热到 min(maxConns, 16)，降低 TDesktop 启动风暴下的冷连接尾延迟突刺。
func WithMaxConns(n int) PoolOption {
	return func(cfg *pgxpool.Config) {
		if n <= 0 {
			return
		}
		cfg.MaxConns = int32(n)
		minConns := int32(defaultMinConns)
		if int32(n) < minConns {
			minConns = int32(n)
		}
		cfg.MinConns = minConns
	}
}

// WithMinConns 设置启动时预热的最小连接数；<=0 保持既有配置。
func WithMinConns(n int) PoolOption {
	return func(cfg *pgxpool.Config) {
		if n <= 0 {
			return
		}
		minConns := int32(n)
		if cfg.MaxConns > 0 && minConns > cfg.MaxConns {
			minConns = cfg.MaxConns
		}
		cfg.MinConns = minConns
	}
}

// Open 建立 pgxpool 连接池并 ping 验证。
func Open(ctx context.Context, dsn string, opts ...PoolOption) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool parse config: %w", err)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool new: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	if err := warmMinConns(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func warmMinConns(ctx context.Context, pool *pgxpool.Pool) error {
	target := pool.Config().MinConns
	if target <= 0 {
		return nil
	}
	conns := make([]*pgxpool.Conn, 0, target)
	defer func() {
		for _, conn := range conns {
			conn.Release()
		}
	}()
	for int32(len(conns)) < target {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("prewarm pg connection %d/%d: %w", len(conns)+1, target, err)
		}
		conns = append(conns, conn)
	}
	return nil
}

// Migrate 用嵌入的迁移脚本将数据库迁移到最新版本。幂等：已最新时返回 nil。
func Migrate(dsn string) error {
	src, err := iofs.New(deploy.Migrations, "migrations")
	if err != nil {
		return fmt.Errorf("iofs source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5DSN(dsn))
	if err != nil {
		return fmt.Errorf("migrate new: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// toPgx5DSN 把 pgxpool 用的 postgres:// DSN 转成 golang-migrate pgx5 driver 所需的 pgx5:// scheme。
func toPgx5DSN(dsn string) string {
	if s, ok := strings.CutPrefix(dsn, "postgres://"); ok {
		return "pgx5://" + s
	}
	if s, ok := strings.CutPrefix(dsn, "postgresql://"); ok {
		return "pgx5://" + s
	}
	return dsn
}
