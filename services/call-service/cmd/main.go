package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"wire-server/internal/call"
	"wire-server/pkg/config"
	"wire-server/pkg/proto/callpb"
)

type callServer struct {
	callpb.UnimplementedCallServiceServer
	svc     *call.Service
	mu      sync.RWMutex
	callIDs map[string]time.Time
}

func newCallServer(svc *call.Service) *callServer {
	return &callServer{
		svc:     svc,
		callIDs: make(map[string]time.Time),
	}
}

func (s *callServer) InitiateCall(_ context.Context, req *callpb.InitiateCallRequest) (*callpb.InitiateCallResponse, error) {
	callID := "call_" + randHex(8)
	started := time.Now().UTC()
	s.mu.Lock()
	s.callIDs[callID] = started
	s.mu.Unlock()
	return &callpb.InitiateCallResponse{
		CallId:    callID,
		StartedAt: timestamppb.New(started),
	}, nil
}

func (s *callServer) JoinCall(_ context.Context, req *callpb.JoinCallRequest) (*callpb.JoinCallResponse, error) {
	s.mu.RLock()
	_, ok := s.callIDs[req.GetCallId()]
	s.mu.RUnlock()
	if !ok {
		return nil, errors.New("call not found")
	}
	expires := time.Now().UTC().Add(30 * time.Minute)
	return &callpb.JoinCallResponse{
		Token:     "join_" + randHex(12),
		ExpiresAt: timestamppb.New(expires),
	}, nil
}

func (s *callServer) EndCall(_ context.Context, req *callpb.EndCallRequest) (*callpb.EndCallResponse, error) {
	s.mu.Lock()
	delete(s.callIDs, req.GetCallId())
	s.mu.Unlock()
	return &callpb.EndCallResponse{Success: true}, nil
}

func (s *callServer) GetTurnCredentials(_ context.Context, req *callpb.GetTurnCredentialsRequest) (*callpb.GetTurnCredentialsResponse, error) {
	username, password, exp := s.svc.TURNCredential(req.GetUserPhone(), 1*time.Hour)
	return &callpb.GetTurnCredentialsResponse{
		Username:  username,
		Password:  password,
		ExpiresAt: timestamppb.New(exp),
	}, nil
}

func main() {
	cfg := config.LoadCallConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	callSvc := call.New(cfg.TurnSecret)
	grpcSrv := grpc.NewServer()
	callpb.RegisterCallServiceServer(grpcSrv, newCallServer(callSvc))

	grpcPort := os.Getenv("CALL_SERVICE_PORT")
	if grpcPort == "" {
		grpcPort = "8211"
	}
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		panic(err)
	}

	httpPort := os.Getenv("PORT")
	if httpPort == "" {
		httpPort = "8210"
	}
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

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
