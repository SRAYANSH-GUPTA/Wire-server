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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"wire-server/pkg/config"
	"wire-server/pkg/db"
	"wire-server/pkg/proto/userpb"
)

type userServer struct {
	userpb.UnimplementedUserServiceServer
	db *pgxpool.Pool
}

func (s *userServer) GetUser(ctx context.Context, req *userpb.GetUserRequest) (*userpb.GetUserResponse, error) {
	phone := strings.TrimSpace(req.GetUserPhone())
	if phone == "" {
		return nil, status.Error(codes.InvalidArgument, "user_phone required")
	}
	var userPhone string
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		SELECT phone, created_at
		FROM users
		WHERE phone = $1
	`, phone).Scan(&userPhone, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "db failed: %v", err)
	}
	return &userpb.GetUserResponse{
		User: &userpb.User{
			UserPhone:   userPhone,
			DisplayName: "",
			AvatarUrl:   "",
			CreatedAt:   timestamppb.New(createdAt),
		},
	}, nil
}

func (s *userServer) GetContacts(_ context.Context, req *userpb.GetContactsRequest) (*userpb.GetContactsResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	return &userpb.GetContactsResponse{
		Contacts:   []*userpb.Contact{},
		NextCursor: "",
	}, nil
}

func (s *userServer) BlockUser(_ context.Context, req *userpb.BlockUserRequest) (*userpb.BlockUserResponse, error) {
	if strings.TrimSpace(req.GetUserPhone()) == "" || strings.TrimSpace(req.GetBlockUserPhone()) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_phone and block_user_phone required")
	}
	return &userpb.BlockUserResponse{Success: true}, nil
}

func main() {
	cfg := config.LoadUserConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPrimaryPool(ctx, cfg.ProfileDBURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	grpcSrv := grpc.NewServer()
	userpb.RegisterUserServiceServer(grpcSrv, &userServer{db: pool})

	grpcPort := envOr("USER_SERVICE_PORT", "8611")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		panic(err)
	}

	httpPort := envOr("PORT", "8610")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	httpSrv := &http.Server{Addr: ":" + httpPort, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = grpcSrv.Serve(lis) }()
	go func() { _ = httpSrv.ListenAndServe() }()

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
