package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"wire-server/pkg/config"
	"wire-server/pkg/db"
	"wire-server/pkg/eventbus"
	loggerpkg "wire-server/pkg/logger"
	"wire-server/pkg/proto/chatpb"
)

func main() {
	cfg := config.LoadChatConfig()
	log, err := loggerpkg.NewLogger(cfg.AppEnv, "chat-service")
	if err != nil {
		panic(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	primaryPool, err := db.NewPrimaryPool(ctx, cfg.PrimaryDBURL)
	if err != nil {
		log.Fatal("primary db connect failed", zap.Error(err))
	}
	defer primaryPool.Close()

	replicaPool, err := db.NewReplicaPool(ctx, cfg.ReplicaDBURL)
	if err != nil {
		log.Fatal("replica db connect failed", zap.Error(err))
	}
	defer replicaPool.Close()

	migrationsPath := os.Getenv("MIGRATIONS_PATH")
	if migrationsPath == "" {
		migrationsPath = "infra/migrations"
	}
	if err := db.RunMigrations(cfg.PrimaryDBURL, migrationsPath); err != nil {
		log.Fatal("migrations failed", zap.Error(err))
	}

	redisAddr := firstAddr(cfg.RedisClusterAddrs)
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Username: strings.TrimSpace(os.Getenv("REDIS_USERNAME")),
		Password: strings.TrimSpace(os.Getenv("REDIS_PASSWORD")),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("redis connect failed", zap.Error(err))
	}
	defer rdb.Close()

	bus := eventbus.NewEventBus(eventbus.Config{
		Driver: cfg.EventBusDriver,
		Client: rdb,
		Logger: log,
	})

	chatSvc := newChatServer(primaryPool, replicaPool, rdb, bus, log)
	chatSvc.startStatusConsumers(ctx)

	grpcPort := os.Getenv("CHAT_SERVICE_PORT")
	if grpcPort == "" {
		grpcPort = "8111"
	}
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatal("grpc listen failed", zap.Error(err))
	}

	grpcServer := grpc.NewServer()
	chatpb.RegisterChatServiceServer(grpcServer, chatSvc)

	healthPort := os.Getenv("PORT")
	if healthPort == "" {
		healthPort = "8110"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpServer := &http.Server{
		Addr:              ":" + healthPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("chat grpc server listening", zap.String("addr", ":"+grpcPort))
		if err := grpcServer.Serve(grpcLis); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error("grpc server failed", zap.Error(err))
			stop()
		}
	}()

	go func() {
		log.Info("chat health server listening", zap.String("addr", ":"+healthPort))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", zap.Error(err))
			// DO NOT stop entire service just because health check port is occupied
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Info("chat-service stopped")
}

func firstAddr(raw string) string {
	parts := strings.Split(raw, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			return p
		}
	}
	return "127.0.0.1:6379"
}
