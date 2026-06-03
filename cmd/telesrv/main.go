// Command telesrv 是基于 gotd/td 的 Telegram-like server（第一兼容目标：Telegram Desktop）。
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"go.uber.org/zap"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"

	"telesrv/internal/app/account"
	"telesrv/internal/app/auth"
	channelapp "telesrv/internal/app/channels"
	"telesrv/internal/app/contacts"
	"telesrv/internal/app/dialogs"
	filesapp "telesrv/internal/app/files"
	"telesrv/internal/app/help"
	"telesrv/internal/app/langpack"
	"telesrv/internal/app/maintenance"
	messageapp "telesrv/internal/app/messages"
	"telesrv/internal/app/updates"
	"telesrv/internal/app/users"
	"telesrv/internal/config"
	"telesrv/internal/mtprotoedge"
	"telesrv/internal/rpc"
	"telesrv/internal/store/postgres"
	"telesrv/internal/store/redisstore"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger:", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	if err := run(logger); err != nil {
		logger.Error("telesrv 退出", zap.Error(err))
		os.Exit(1)
	}
}

func run(logger *zap.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	rsaKey, err := mtprotoedge.LoadOrGenerateRSAKey(cfg.RSAKeyPath)
	if err != nil {
		return fmt.Errorf("server rsa key: %w", err)
	}
	fingerprint := exchange.PrivateKey{RSA: rsaKey}.Fingerprint()

	_, portStr, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("parse listen addr %q: %w", cfg.ListenAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("parse listen port %q: %w", portStr, err)
	}

	// tg.Layer 来自 gotd/td v0.144.0（应为 225），需与目标 TDesktop 基线对齐。
	logger.Info("telesrv 启动",
		zap.String("listen", cfg.ListenAddr),
		zap.Int("dc", cfg.DC),
		zap.String("advertise", net.JoinHostPort(cfg.AdvertiseIP, portStr)),
		zap.Int("tl_layer", tg.Layer),
		zap.String("rsa_key", cfg.RSAKeyPath),
		zap.Int64("rsa_fingerprint", fingerprint),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 持久化依赖：先迁移 schema，再建立连接。auth_key 落 PostgreSQL、session 落 Redis。
	// 依赖由 deploy/docker-compose.yml 启动；连不上则启动失败（开发期须先 docker compose up）。
	if err := postgres.Migrate(cfg.PostgresDSN); err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	pool, err := postgres.Open(ctx, cfg.PostgresDSN,
		postgres.WithMaxConns(cfg.PostgresMaxConns),
		postgres.WithMinConns(cfg.PostgresMinConns),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	rdb, err := redisstore.Open(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()
	logger.Info("持久化依赖就绪", zap.String("redis", cfg.RedisAddr))

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %q: %w", cfg.ListenAddr, err)
	}

	authKeyStore := postgres.NewAuthKeyStore(pool)
	userStore := postgres.NewUserStore(pool)
	authzStore := postgres.NewAuthorizationStore(pool)
	updateStateStore := postgres.NewUpdateStateStore(pool)
	updateEventStore := postgres.NewUpdateEventStore(pool)
	dispatchOutboxStore := postgres.NewDispatchOutboxStore(pool, postgres.WithLeaseTimeout(cfg.OutboxLeaseTimeout))
	ptsAllocator := redisstore.NewPtsAllocator(rdb, updateEventStore)
	boxIDAllocator := redisstore.NewBoxIDAllocator(rdb, postgres.NewMessageBoxCounterSource(pool))
	channelIDAllocator := redisstore.NewChannelIDAllocator(rdb, postgres.NewChannelIDCounterSource(pool))
	channelPtsAllocator := redisstore.NewChannelPtsAllocator(rdb, postgres.NewChannelPtsCounterSource(pool))
	channelMessageIDAllocator := redisstore.NewChannelMessageIDAllocator(rdb, postgres.NewChannelMessageIDCounterSource(pool))
	contactStore := postgres.NewContactStore(pool)
	dialogStore := postgres.NewDialogStore(pool)
	messageStore := postgres.NewMessageStore(pool, postgres.WithMessageAllocators(boxIDAllocator, ptsAllocator))
	channelStore := postgres.NewChannelStore(pool, postgres.WithChannelAllocators(channelIDAllocator, channelPtsAllocator, channelMessageIDAllocator))
	mediaStore := postgres.NewMediaStore(pool)
	blobBackend, err := filesapp.NewLocalFS(cfg.BlobDir)
	if err != nil {
		return fmt.Errorf("init blob backend: %w", err)
	}
	filesService := filesapp.NewService(mediaStore, blobBackend, cfg.DC)
	if stats, err := filesService.SeedMedia(ctx, cfg.StickerSeedDir, cfg.StickerSeedMaxSets); err != nil {
		return fmt.Errorf("seed media: %w", err)
	} else if !stats.Skipped {
		logger.Info("媒体种子导入完成",
			zap.String("dir", cfg.StickerSeedDir),
			zap.Int("reactions", stats.Reactions),
			zap.Int("sticker_sets", stats.StickerSets),
			zap.Int("documents", stats.Documents),
			zap.Int("blobs", stats.Blobs),
		)
	}
	if stats, err := filesService.WarmCaches(ctx); err != nil {
		logger.Warn("媒体资源缓存预热失败", zap.Error(err))
	} else if stats.StickerSets > 0 || stats.Documents > 0 || stats.Blobs > 0 {
		logger.Info("媒体资源缓存预热完成",
			zap.Int("sticker_sets", stats.StickerSets),
			zap.Int("documents", stats.Documents),
			zap.Int("blobs", stats.Blobs),
		)
	}
	langPackStore := postgres.NewLangPackStore(pool)
	passwordStore := postgres.NewPasswordStore(pool)
	helpStore := postgres.NewHelpStore(pool)
	tempAuthKeyStore := postgres.NewTempAuthKeyBindingStore(pool)
	sessionStore := redisstore.NewSessionStore(rdb, redisstore.DefaultSessionTTL)
	codeStore := redisstore.NewCodeStore(rdb)
	rateLimiter := redisstore.NewRateLimiter(rdb)
	activeSessions := mtprotoedge.NewSessionManager(logger.Named("mtprotoedge").Named("sessions"))
	go maintenance.NewRetentionWorker(dispatchOutboxStore, logger.Named("maintenance").Named("retention"),
		cfg.UpdateEventRetention,
		cfg.RetentionInterval,
		cfg.RetentionBatch,
	).Run(ctx)
	go rpc.NewOutboxDispatcher(updateEventStore, dispatchOutboxStore, activeSessions, logger.Named("rpc").Named("outbox"),
		rpc.WithOutboxWorkers(cfg.OutboxWorkers),
		rpc.WithOutboxBatch(cfg.OutboxBatch),
		rpc.WithOutboxInterval(cfg.OutboxInterval),
		rpc.WithOutboxPushTimeout(cfg.OutboundPushTimeout),
	).Run(ctx)
	langPackService := langpack.NewService(langPackStore)
	if seeded, err := langPackService.SeedDirectory(ctx, cfg.LangPackSeedDir); err != nil {
		return fmt.Errorf("seed langpack: %w", err)
	} else if seeded > 0 {
		logger.Info("语言包种子导入完成", zap.String("dir", cfg.LangPackSeedDir), zap.Int("strings", seeded))
	}

	router := rpc.New(rpc.Config{
		DC:                  cfg.DC,
		IP:                  cfg.AdvertiseIP,
		Port:                port,
		OutboundPushTimeout: cfg.OutboundPushTimeout,
	}, rpc.Deps{
		Auth:     auth.NewService(userStore, authzStore, codeStore, authKeyStore, tempAuthKeyStore, cfg.DevAuthCode, auth.WithLoginMessages(messageStore, dialogStore)),
		Account:  account.NewService(passwordStore, account.WithReactionSettings(passwordStore)),
		Help:     help.NewService(helpStore, helpStore),
		Users:    users.NewService(userStore, users.WithPhotoProvider(mediaStore)),
		Updates:  updates.NewService(updateStateStore, updateEventStore, updates.WithPtsAllocator(ptsAllocator)),
		Contacts: contacts.NewService(contactStore, userStore),
		Dialogs:  dialogs.NewService(dialogStore, channelStore),
		Messages: messageapp.NewService(messageStore, dialogStore),
		Channels: channelapp.NewService(channelStore),
		Files:    filesService,
		LangPack: langPackService,
		Sessions: activeSessions,
		Limiter:  rateLimiter,
	}, logger.Named("rpc"), clock.System)
	activeSessions.SetLifecycleObserver(router)

	srv := mtprotoedge.New(mtprotoedge.Options{
		Logger:         logger.Named("mtprotoedge"),
		DC:             cfg.DC,
		RSAKey:         rsaKey,
		RPC:            router,
		AuthKeys:       authKeyStore,
		Sessions:       sessionStore,
		ActiveSessions: activeSessions,
		ObfuscatedTCP:  true,
	})
	return srv.Serve(ctx, ln)
}
