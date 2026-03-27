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

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"wire-server/internal/presence"
	"wire-server/pkg/proto/presencepb"
)

type presenceServer struct {
	presencepb.UnimplementedPresenceServiceServer
	tracker *presence.Tracker
	client  *goredis.Client
}

func (s *presenceServer) GetPresence(ctx context.Context, req *presencepb.GetPresenceRequest) (*presencepb.GetPresenceResponse, error) {
	userID := strings.TrimSpace(req.GetUserId())
	if userID == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	online, err := s.tracker.IsOnline(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "presence check failed: %v", err)
	}
	lastSeen := timestamppb.New(time.Now().UTC())
	if raw, err := s.client.Get(ctx, "presence:user:"+userID+":last_seen").Result(); err == nil {
		if ts, parseErr := time.Parse(time.RFC3339Nano, raw); parseErr == nil {
			lastSeen = timestamppb.New(ts)
		}
	}
	statusV := "offline"
	if online {
		statusV = "online"
	}
	return &presencepb.GetPresenceResponse{
		Presence: &presencepb.PresenceRecord{
			UserId:   userID,
			Status:   statusV,
			LastSeen: lastSeen,
		},
	}, nil
}

func (s *presenceServer) GetBulkPresence(ctx context.Context, req *presencepb.GetBulkPresenceRequest) (*presencepb.GetBulkPresenceResponse, error) {
	out := make([]*presencepb.PresenceRecord, 0, len(req.GetUserIds()))
	for _, userID := range req.GetUserIds() {
		resp, err := s.GetPresence(ctx, &presencepb.GetPresenceRequest{UserId: userID})
		if err != nil {
			continue
		}
		out = append(out, resp.GetPresence())
	}
	return &presencepb.GetBulkPresenceResponse{Records: out}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisAddr := envOr("REDIS_CLUSTER_ADDRS", "127.0.0.1:6379")
	first := strings.TrimSpace(strings.Split(redisAddr, ",")[0])
	client := goredis.NewClient(&goredis.Options{Addr: first})
	if err := client.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	defer client.Close()

	srv := &presenceServer{
		tracker: presence.New(client),
		client:  client,
	}

	grpcSrv := grpc.NewServer()
	presencepb.RegisterPresenceServiceServer(grpcSrv, srv)

	grpcPort := envOr("PRESENCE_SERVICE_PORT", "8411")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		panic(err)
	}

	httpPort := envOr("PORT", "8410")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpSrv := &http.Server{Addr: ":" + httpPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
			stop()
		}
	}()
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
		}
	}()

	<-ctx.Done()
	grpcSrv.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func envOr(k, d string) string {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	return v
}
