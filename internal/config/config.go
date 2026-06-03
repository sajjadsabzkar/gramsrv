// Package config 负责 telesrv 运行配置的加载与校验。
package config

import (
	"os"
	"strconv"
	"time"
)

// Config 是 telesrv 的运行配置。
type Config struct {
	// ListenAddr 是 MTProto TCP 监听地址。
	// 需与 TDesktop patch 指向的自建 DC 地址/端口一致（记录于 docs/tdesktop-patch-notes.md）。
	ListenAddr string
	// AdvertiseIP 是写入 help.getConfig DCOptions 的对外可达 IP（客户端据此连接本 DC）。
	AdvertiseIP string
	// RSAKeyPath 是 server RSA 私钥的 PEM 路径；不存在时自动生成。
	RSAKeyPath string
	// DC 是本 server 的 DC ID。
	DC int

	// PostgresDSN 是业务数据（auth_key / user / authorization 等）持久化的 PostgreSQL 连接串。
	// 依赖由 deploy/docker-compose.yml 启动；职责划分见 docs/persistence-layer.md。
	PostgresDSN string
	// PostgresMaxConns 是 pgxpool 最大连接数。<=0 用 pgx 默认（max(4, NumCPU)，生产偏小）。
	// 需覆盖发送事务 + outbox worker 并发 + RPC 读，过小会在高并发下排队（表现为尾延迟突刺）。
	PostgresMaxConns int
	// PostgresMinConns 是启动时预热的 pgxpool 连接数，降低 TDesktop 冷启动并发 RPC 的建连等待。
	PostgresMinConns int
	// RedisAddr 是高频易失态（验证码、限流计数、update 队列）的 Redis 地址。
	RedisAddr string
	// RedisPassword 是 Redis 密码；开发默认空。
	RedisPassword string
	// RedisDB 是 Redis 逻辑库编号。
	RedisDB int

	// DevAuthCode 是开发固定验证码；生产短信/风控不在当前范围内。
	DevAuthCode string
	// LangPackSeedDir 是 TDesktop 语言包 .strings 种子目录。
	LangPackSeedDir string
	// BlobDir 是本地磁盘 blob backend 根目录（媒体文件字节内容）。
	BlobDir string
	// StickerSeedDir 是 reaction / sticker 资源种子目录（导入到 documents/sticker_sets + blob）。
	StickerSeedDir string
	// StickerSeedMaxSets 限制导入的常规贴纸集数量（避免启动时导入过多包），<=0 表示不限。
	StickerSeedMaxSets int

	// OutboxWorkers 是并发 claim 的 outbox worker 数。worker 间用 FOR UPDATE SKIP LOCKED 互不重叠。
	// 开发默认偏保守，避免 TDesktop 启动风暴下多个 worker 同扫分区父表触发 PG lock/shared-memory 压力；
	// 压测或生产可按硬件用 TELESRV_OUTBOX_WORKERS 调高。
	OutboxWorkers int
	// OutboxBatch 是 transactional outbox worker 每次 claim 的最大条数。
	// 调大提升吞吐、增大单批 PG/推送压力；调小降低延迟抖动。配套压测见 docs/message-module.md。
	OutboxBatch int
	// OutboxInterval 是 outbox worker 两次 claim 之间的轮询间隔。
	OutboxInterval time.Duration
	// OutboxLeaseTimeout 是 'dispatching' 行被判定为租约过期、允许其它 worker 重新 claim 的时长。
	// 取值需大于单批投递耗时，否则会重复推送；过大则 worker 崩溃后积压恢复变慢。
	OutboxLeaseTimeout time.Duration
	// OutboundPushTimeout 是 best-effort updates 推送等待 outbound 队列接受的最长时间。
	OutboundPushTimeout time.Duration
	// UpdateEventRetention 是 durable update log 保留期；只清理已被水位/state 覆盖的事件。
	UpdateEventRetention time.Duration
	// RetentionInterval 是 retention worker 的运行间隔。
	RetentionInterval time.Duration
	// RetentionBatch 是单次 retention 最多删除的行数。
	RetentionBatch int
}

// Load 从环境变量读取配置并填充默认值。第一阶段不做严格校验。
func Load() (Config, error) {
	cfg := Config{
		ListenAddr:  envOr("TELESRV_LISTEN", "0.0.0.0:2398"),
		AdvertiseIP: envOr("TELESRV_ADVERTISE_IP", "127.0.0.1"),
		RSAKeyPath:  envOr("TELESRV_RSA_KEY", "data/server_rsa.pem"),
		DC:          envIntOr("TELESRV_DC", 2),

		PostgresDSN:      envOr("TELESRV_POSTGRES_DSN", "postgres://telesrv:telesrv@localhost:5432/telesrv?sslmode=disable"),
		PostgresMaxConns: envIntOr("TELESRV_POSTGRES_MAX_CONNS", 50),
		PostgresMinConns: envIntOr("TELESRV_POSTGRES_MIN_CONNS", 16),
		RedisAddr:        envOr("TELESRV_REDIS_ADDR", "localhost:6399"),
		RedisPassword:    envOr("TELESRV_REDIS_PASSWORD", ""),
		RedisDB:          envIntOr("TELESRV_REDIS_DB", 0),

		DevAuthCode:        envOr("TELESRV_DEV_AUTH_CODE", "12345"),
		LangPackSeedDir:    envOr("TELESRV_LANGPACK_SEED_DIR", "data/langpack"),
		BlobDir:            envOr("TELESRV_BLOB_DIR", "data/blobs"),
		StickerSeedDir:     envOr("TELESRV_STICKER_SEED_DIR", "data/sticker-seed"),
		StickerSeedMaxSets: envIntOr("TELESRV_STICKER_SEED_MAX_SETS", 40),

		OutboxWorkers:        envIntOr("TELESRV_OUTBOX_WORKERS", 2),
		OutboxBatch:          envIntOr("TELESRV_OUTBOX_BATCH", 100),
		OutboxInterval:       envDurationOr("TELESRV_OUTBOX_INTERVAL", 200*time.Millisecond),
		OutboxLeaseTimeout:   envDurationOr("TELESRV_OUTBOX_LEASE_TIMEOUT", 30*time.Second),
		OutboundPushTimeout:  envDurationOr("TELESRV_OUTBOUND_PUSH_TIMEOUT", 200*time.Millisecond),
		UpdateEventRetention: envDurationOr("TELESRV_UPDATE_EVENT_RETENTION", 168*time.Hour),
		RetentionInterval:    envDurationOr("TELESRV_RETENTION_INTERVAL", time.Hour),
		RetentionBatch:       envIntOr("TELESRV_RETENTION_BATCH", 10000),
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envDurationOr 读取 time.ParseDuration 格式（如 "200ms"、"30s"）的时长配置；解析失败回退默认值。
func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
