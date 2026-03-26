package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"wire-server/pkg/config"
	"wire-server/pkg/db"
	"wire-server/pkg/eventbus"
	loggerpkg "wire-server/pkg/logger"
	"wire-server/pkg/redis"
	"wire-server/services/api-gateway/internal/clients"
	gatewayinternal "wire-server/services/api-gateway/internal/gateway"
)

func main() {
	cfg := config.LoadGatewayConfig()
	log, err := loggerpkg.NewLogger(cfg.AppEnv, "api-gateway")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisAddrs := parseAddrs(cfg.RedisClusterAddrs)
	redisClient, err := redis.NewClusterClient(ctx, redisAddrs, log)
	if err != nil {
		log.Fatal("redis connect failed", zap.Error(err))
	}
	eventBus := eventbus.NewEventBus(eventbus.Config{Driver: cfg.EventBusDriver, Client: redisClient.Cmdable(), Logger: log})

	dbPool, err := db.NewPrimaryPool(ctx, cfg.SupabaseDBURL)
	if err != nil {
		log.Fatal("db pool", zap.Error(err))
	}
	defer dbPool.Close()

	mtlsCreds, err := loadMTLSCredentials()
	if err != nil {
		log.Fatal("mtls creds", zap.Error(err))
	}

	grpcClients, err := clients.New(ctx, cfg, mtlsCreds, log)
	if err != nil {
		log.Fatal("grpc clients", zap.Error(err))
	}

	server := &gatewayinternal.Server{
		Config:      cfg,
		Logger:      log,
		EventBus:    eventBus,
		Redis:       redisClient,
		DB:          dbPool,
		GRPCClients: grpcClients,
	}

	hub, err := gatewayinternal.NewHub(log, redisClient, eventBus, server.UserExists)
	if err != nil {
		log.Fatal("new hub", zap.Error(err))
	}
	server.Hub = hub

	router := server.Routes()

	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := hub.Run(ctx); err != nil && err != context.Canceled {
			log.Error("hub stopped", zap.Error(err))
		}
	}()

	go func() {
		log.Info("http server starting", zap.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", zap.Error(err))
			cancel()
		}
	}()

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	<-signalCtx.Done()
	stop()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = hub.Shutdown()
	wg.Wait()
	log.Info("api-gateway stopped")
}

func parseAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func loadMTLSCredentials() (credentials.TransportCredentials, error) {
	certPEM := os.Getenv("MTLS_CLIENT_CERT")
	keyPEM := os.Getenv("MTLS_CLIENT_KEY")
	caPEM := os.Getenv("MTLS_CA_CERT")
	if certPEM == "" || keyPEM == "" || caPEM == "" {
		// Single-EC2/local mode fallback.
		return insecure.NewCredentials(), nil
	}
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, fmt.Errorf("failed to append ca cert")
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS12,
	}), nil
}
