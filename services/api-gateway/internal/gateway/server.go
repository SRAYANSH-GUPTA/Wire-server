package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gobwas/ws"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"wire-server/pkg/config"
	"wire-server/pkg/eventbus"
	"wire-server/pkg/middleware"
	"wire-server/pkg/proto/callpb"
	"wire-server/pkg/proto/chatpb"
	"wire-server/pkg/proto/mediapb"
	"wire-server/pkg/proto/presencepb"
	"wire-server/pkg/proto/userpb"
	"wire-server/pkg/redis"
	"wire-server/services/api-gateway/internal/clients"
)

type Server struct {
	Config      config.GatewayConfig
	Logger      *zap.Logger
	Hub         *Hub
	EventBus    eventbus.EventBus
	Redis       *redis.Client
	GRPCClients *clients.Clients
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.TracingMiddleware("api-gateway"))
	r.Use(middleware.MetricsMiddleware())
	r.Use(middleware.RateLimitMiddleware(s.Redis, 200, time.Minute))

	r.Get("/health", s.health)
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Post("/auth/refresh", s.refreshToken)
	r.With(middleware.AuthMiddleware(s.Config.JWTSecret)).Post("/ws/connect", s.wsConnect)

	r.Route("/api/v1", func(api chi.Router) {
		api.Use(middleware.AuthMiddleware(s.Config.JWTSecret))
		api.Post("/messages", s.createMessage)
		api.Get("/messages/{id}", s.getMessage)
		api.Post("/media/upload", s.requestUpload)
		api.Post("/calls/start", s.startCall)
		api.Get("/users/{id}", s.getUser)
		api.Get("/presence/{id}", s.getPresence)
	})

	return r
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "api-gateway"})
}

func (s *Server) refreshToken(w http.ResponseWriter, r *http.Request) {
	target := fmt.Sprintf("%s/auth/v1/token?grant_type=refresh_token", strings.TrimRight(s.Config.SupabaseURL, "/"))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target, r.Body)
	if err != nil {
		http.Error(w, "proxy failed", http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "upstream failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) wsConnect(w http.ResponseWriter, r *http.Request) {
	claims, ok := middleware.ClaimsFromContext(r.Context())
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	conn, err := ws.Upgrade(r, w)
	if err != nil {
		http.Error(w, "upgrade failed", http.StatusBadRequest)
		return
	}
	c, err := NewConnection(conn, claims, s.Redis, s.EventBus, s.Logger)
	if err != nil {
		http.Error(w, "connection failed", http.StatusInternalServerError)
		return
	}
	s.Hub.Register(c)
}

func (s *Server) createMessage(w http.ResponseWriter, r *http.Request) {
	var body chatpb.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Chat.SendMessage(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.Chat.GetHistory(r.Context(), &chatpb.GetHistoryRequest{
		ConversationId: id,
		Limit:          1,
	})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) requestUpload(w http.ResponseWriter, r *http.Request) {
	var body mediapb.RequestUploadURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Media.RequestUploadURL(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) startCall(w http.ResponseWriter, r *http.Request) {
	var body callpb.InitiateCallRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Call.InitiateCall(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getUser(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.User.GetUser(r.Context(), &userpb.GetUserRequest{UserId: id})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getPresence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.Presence.GetPresence(r.Context(), &presencepb.GetPresenceRequest{UserId: id})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}
