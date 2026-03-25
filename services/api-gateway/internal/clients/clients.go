package clients

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"wire-server/pkg/config"
	"wire-server/pkg/proto/callpb"
	"wire-server/pkg/proto/chatpb"
	mediapb "wire-server/pkg/proto/mediapb"
	"wire-server/pkg/proto/presencepb"
	"wire-server/pkg/proto/userpb"
)

type Clients struct {
	Chat     chatpb.ChatServiceClient
	Call     callpb.CallServiceClient
	Media    mediapb.MediaServiceClient
	Presence presencepb.PresenceServiceClient
	User     userpb.UserServiceClient
	logger   *zap.Logger
}

func New(ctx context.Context, cfg config.GatewayConfig, creds credentials.TransportCredentials, log *zap.Logger) (*Clients, error) {
	dials := map[string]*grpc.ClientConn{}
	targets := map[string]string{
		"chat":     cfg.ChatServiceAddr,
		"call":     cfg.CallServiceAddr,
		"media":    cfg.MediaServiceAddr,
		"presence": cfg.PresenceServiceAddr,
		"user":     cfg.UserServiceAddr,
	}
	for name, addr := range targets {
		conn, err := dialWithCircuit(ctx, name, addr, creds)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", name, err)
		}
		dials[name] = conn
	}
	return &Clients{
		Chat:     chatpb.NewChatServiceClient(dials["chat"]),
		Call:     callpb.NewCallServiceClient(dials["call"]),
		Media:    mediapb.NewMediaServiceClient(dials["media"]),
		Presence: presencepb.NewPresenceServiceClient(dials["presence"]),
		User:     userpb.NewUserServiceClient(dials["user"]),
		logger:   log,
	}, nil
}

func dialWithCircuit(ctx context.Context, name, addr string, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: 2,
		Interval:    10 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	})

	var conn *grpc.ClientConn
	operation := func() error {
		var err error
		_, err = cb.Execute(func() (interface{}, error) {
			conn, err = grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(creds))
			return nil, err
		})
		return err
	}
	return conn, backoff.Retry(operation, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
}
