package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"wire-server/internal/media"
	"wire-server/pkg/db"
	"wire-server/pkg/db/sqlc"
	"wire-server/pkg/proto/mediapb"
)

type mediaServer struct {
	mediapb.UnimplementedMediaServiceServer
	svc  *media.Service
	pool *pgxpool.Pool
}

func (s *mediaServer) RequestUploadURL(ctx context.Context, req *mediapb.RequestUploadURLRequest) (*mediapb.RequestUploadURLResponse, error) {
	resp, err := s.svc.PresignUpload(ctx, media.PresignRequest{
		UserPhone:   req.GetOwnerPhone(),
		ContentType: req.GetContentType(),
		SizeBytes:   req.GetSize(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "presign failed: %v", err)
	}
	return &mediapb.RequestUploadURLResponse{
		Url:       resp.URL,
		Headers:   resp.Headers,
		ExpiresAt: timestamppb.New(resp.ExpiresAt),
	}, nil
}

func (s *mediaServer) GetMediaItem(ctx context.Context, req *mediapb.GetMediaItemRequest) (*mediapb.GetMediaItemResponse, error) {
	var id, bucket, objectKey, statusV, ownerPhone string
	var createdAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, bucket, object_key, status, created_at, owner_phone
		FROM media_objects
		WHERE id = $1
	`, req.GetMediaId()).Scan(&id, &bucket, &objectKey, &statusV, &createdAt, &ownerPhone)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "media item not found")
	}
	return &mediapb.GetMediaItemResponse{
		Item: &mediapb.MediaItem{
			MediaId:    id,
			OwnerPhone: ownerPhone,
			Bucket:     bucket,
			ObjectKey:  objectKey,
			Status:     statusV,
			CreatedAt:  timestamppb.New(createdAt),
		},
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_PRIMARY_URL")
	if dbURL == "" {
		dbURL = os.Getenv("SUPABASE_DB_URL")
	}
	if dbURL == "" {
		panic("DATABASE_PRIMARY_URL or SUPABASE_DB_URL required")
	}

	pool, err := db.NewPrimaryPool(ctx, dbURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	repo := sqlc.New(pool)
	svc := media.New(
		envOr("S3_BUCKET", "local-bucket"),
		envOr("S3_REGION", "ap-northeast-1"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		repo,
	)

	grpcSrv := grpc.NewServer()
	mediapb.RegisterMediaServiceServer(grpcSrv, &mediaServer{svc: svc, pool: pool})

	grpcPort := envOr("MEDIA_SERVICE_PORT", "8311")
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		panic(err)
	}

	httpPort := envOr("PORT", "8310")
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
			// DO NOT stop entire service just because health check port is occupied
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
