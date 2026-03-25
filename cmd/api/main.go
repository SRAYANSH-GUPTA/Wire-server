package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-playground/validator/v10"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"wire-server/internal/auth"
	"wire-server/internal/call"
	"wire-server/internal/chat"
	"wire-server/internal/media"
	"wire-server/internal/notification"
	"wire-server/internal/presence"
	"wire-server/pkg/config"
	"wire-server/pkg/db"
	"wire-server/pkg/db/sqlc"
	"wire-server/pkg/eventbus"
	"wire-server/pkg/logger"
	redisclient "wire-server/pkg/redis"
	"wire-server/pkg/ws"
)

type app struct {
	cfg      config.Config
	log      *zap.Logger
	runCtx   context.Context
	db       *pgxpool.Pool
	redis    *goredis.Client
	authMW   *auth.Middleware
	hub      *ws.Hub
	chat     *chat.Service
	presence *presence.Tracker
	call     *call.Service
	media    *media.Service
	notify   *notification.WorkerPool
	bus      eventbus.Bus
	repo     *sqlc.Queries
	validate *validator.Validate
	upgrader websocket.Upgrader
	requests atomic.Uint64
}

func main() {
	cfg := config.Load()
	log, err := logger.New(cfg.AppEnv)
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("database init failed", zap.Error(err))
	}
	if err := db.RunMigrations(cfg.DatabaseURL, "infra/migrations"); err != nil {
		log.Fatal("migrations failed", zap.Error(err))
	}

	redisC, err := redisclient.New(ctx, cfg.RedisURL)
	if err != nil {
		log.Fatal("redis init failed", zap.Error(err))
	}
	repo := sqlc.New(pool)
	bus := eventbus.NewRedisStreams(redisC.Client)
	presenceTracker := presence.New(redisC.Client)
	authMW := auth.NewMiddleware(cfg.JWTSecret)
	callSvc := call.New(cfg.TurnSecret)
	mediaSvc := media.New(cfg.S3Bucket, cfg.S3Region, cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, repo)
	notifyPool := notification.New(cfg.FCMServerKey, 4)

	srv := &app{
		cfg:      cfg,
		log:      log,
		runCtx:   runCtx,
		db:       pool,
		redis:    redisC.Client,
		authMW:   authMW,
		presence: presenceTracker,
		call:     callSvc,
		media:    mediaSvc,
		notify:   notifyPool,
		bus:      bus,
		repo:     repo,
		validate: validator.New(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     strictOrigin(cfg.CORSAllowedOrigins),
		},
	}

	srv.hub = ws.NewHub(log, srv.handleDisconnect)
	srv.chat = chat.New(repo, srv.hub, bus, presenceTracker, redisC.Client)

	go srv.hub.Run(runCtx)
	notifyPool.Start(ctx)

	router := srv.routes()
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("http server starting", zap.String("port", cfg.Port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("http server failed", zap.Error(err))
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	log.Info("shutdown initiated")
	_ = server.Shutdown(shutdownCtx)
	drainDeadline := time.NewTimer(cfg.WebSocketDrainTimeout)
drainLoop:
	for {
		if srv.hub.ActiveConnections() == 0 {
			break
		}
		select {
		case <-drainDeadline.C:
			break drainLoop
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !drainDeadline.Stop() {
		select {
		case <-drainDeadline.C:
		default:
		}
	}
	cancelRun()
	notifyPool.Stop()
	_ = srv.db.Close()
	_ = srv.redis.Close()
	_ = log.Sync()
	log.Info("shutdown complete")
}

func (a *app) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(a.httpLogger)
	r.Get("/health", a.health)
	r.Get("/metrics", a.metrics)
	r.Get("/ws/connect", a.wsConnect)

	r.Group(func(pr chi.Router) {
		pr.Use(a.authMW.Wrap)
		pr.Post("/v1/chat/send", a.httpSendMessage)
		pr.Post("/v1/chat/read", a.httpMarkRead)
		pr.Post("/v1/media/presign", a.httpPresign)
		pr.Get("/v1/call/turn", a.httpTURN)
		pr.Post("/v1/presence/online", a.httpPresenceOnline)
		pr.Post("/v1/presence/offline", a.httpPresenceOffline)
	})

	return strictCORS(a.cfg.CORSAllowedOrigins, r)
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (a *app) metrics(w http.ResponseWriter, r *http.Request) {
	active := a.hub.ActiveConnections()
	rate, errorsTotal := ws.MetricsSnapshot(active)
	payload := fmt.Sprintf(
		"# TYPE active_connections gauge\nactive_connections %d\n# TYPE messages_per_second gauge\nmessages_per_second %.6f\n# TYPE ws_errors_total counter\nws_errors_total %d\n",
		active, rate, errorsTotal,
	)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(payload))
}

type sendMessageRequest struct {
	ConversationID string   `json:"conversation_id" validate:"omitempty"`
	RecipientIDs   []string `json:"recipient_ids" validate:"omitempty"`
	Body           string   `json:"body" validate:"required_without=MediaURL"`
	MediaURL       *string  `json:"media_url" validate:"omitempty,url"`
}

type readMessageRequest struct {
	MessageID string `json:"message_id" validate:"required"`
}

type presignRequest struct {
	ObjectKey   string `json:"object_key" validate:"omitempty"`
	ContentType string `json:"content_type" validate:"required"`
	SizeBytes   int64  `json:"size_bytes" validate:"gt=0"`
}

func (a *app) httpSendMessage(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	var req sendMessageRequest
	if err := decodeAndValidate(a.validate, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "MSG_001", err.Error(), traceID(r))
		return
	}
	msg, err := a.chat.SendMessage(r.Context(), chat.SendRequest{
		ConversationID: req.ConversationID,
		RecipientIDs:   req.RecipientIDs,
		SenderID:       claims.UserID,
		Body:           req.Body,
		MediaURL:       req.MediaURL,
		TraceID:        traceID(r),
	})
	if err != nil {
		a.log.Error("send message failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "MSG_002", "failed to send message", traceID(r))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"message_id": msg.ID, "conversation_id": msg.ConversationID})
}

func (a *app) httpMarkRead(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	var req readMessageRequest
	if err := decodeAndValidate(a.validate, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "MSG_003", err.Error(), traceID(r))
		return
	}
	if err := a.chat.MarkRead(r.Context(), chat.ReadRequest{MessageID: req.MessageID, UserID: claims.UserID, TraceID: traceID(r)}); err != nil {
		writeError(w, http.StatusInternalServerError, "MSG_004", "failed to mark message read", traceID(r))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (a *app) httpPresign(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	var req presignRequest
	if err := decodeAndValidate(a.validate, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "MEDIA_001", err.Error(), traceID(r))
		return
	}
	resp, err := a.media.PresignUpload(r.Context(), media.PresignRequest{
		UserID:      claims.UserID,
		ObjectKey:   req.ObjectKey,
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
	})
	if err != nil {
		a.log.Error("presign failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "MEDIA_002", "failed to presign upload", traceID(r))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *app) httpTURN(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	username, password, expiresAt := a.call.TURNCredential(claims.UserID, 10*time.Minute)
	writeJSON(w, http.StatusOK, map[string]any{
		"username":   username,
		"password":   password,
		"expires_at": expiresAt.UTC().Format(time.RFC3339),
	})
}

func (a *app) httpPresenceOnline(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	if err := a.presence.SetOnline(r.Context(), claims.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, "PRES_001", "failed to set presence", traceID(r))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "online"})
}

func (a *app) httpPresenceOffline(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "AUTH_003", "missing auth context", traceID(r))
		return
	}
	if err := a.presence.SetOffline(r.Context(), claims.UserID); err != nil {
		writeError(w, http.StatusInternalServerError, "PRES_002", "failed to set presence", traceID(r))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "offline"})
}

func (a *app) wsConnect(w http.ResponseWriter, r *http.Request) {
	traceID := traceID(r)
	token := r.URL.Query().Get("access_token")
	if token == "" {
		token = authHeaderBearer(r.Header.Get("Authorization"))
	}
	claims, err := auth.ParseToken(token, a.cfg.JWTSecret)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "AUTH_004", "invalid websocket token", traceID)
		return
	}
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		a.log.Error("websocket upgrade failed", zap.Error(err))
		return
	}
	ctx := logger.WithContext(auth.WithContext(a.runCtx, claims), traceID, claims.UserID)
	_ = a.presence.SetOnline(ctx, claims.UserID)
	socket := ws.NewConn(conn, a.hub, claims.UserID, traceID, a.log, a.wsHandler)
	socket.Register()
	go socket.Run(ctx)
	_ = a.chat.DrainPending(ctx, claims.UserID)
}

func (a *app) wsHandler(ctx context.Context, userID string, frame ws.Frame) error {
	switch frame.Type {
	case "chat.message.send":
		body, _ := frame.Payload["body"].(string)
		conversationID, _ := frame.Payload["conversation_id"].(string)
		recipientIDs := stringSlice(frame.Payload["recipient_ids"])
		mediaURL := optionalString(frame.Payload["media_url"])
		_, err := a.chat.SendMessage(ctx, chat.SendRequest{
			ConversationID: conversationID,
			RecipientIDs:   recipientIDs,
			SenderID:       userID,
			Body:           body,
			MediaURL:       mediaURL,
			TraceID:        frame.TraceID,
		})
		return err
	case "chat.message.read":
		messageID, _ := frame.Payload["message_id"].(string)
		return a.chat.MarkRead(ctx, chat.ReadRequest{MessageID: messageID, UserID: userID, TraceID: frame.TraceID})
	case "call.offer", "call.answer", "call.candidate":
		toUserID, _ := frame.Payload["to_user_id"].(string)
		if toUserID != "" {
			payload, err := ws.MarshalFrame(ws.Frame{
				Type:    frame.Type,
				TraceID: frame.TraceID,
				UserID:  userID,
				Payload: frame.Payload,
			})
			if err == nil {
				a.hub.Broadcast(ws.Outbound{Targets: []string{toUserID}, Data: payload})
			}
		}
		_ = a.bus.Publish(ctx, "events.calls", eventbus.Event{
			Type:   "call.started",
			Stream: "events.calls",
			ID:     callID(frame.Payload),
			Metadata: map[string]string{
				"trace_id": frame.TraceID,
				"from":     userID,
				"to":       optionalStringValue(frame.Payload, "to_user_id"),
			},
		})
		return nil
	default:
		return nil
	}
}

func (a *app) handleDisconnect(ctx context.Context, userID string) error {
	if err := a.presence.TouchLastSeen(ctx, userID); err != nil {
		a.log.Warn("touch last seen failed", zap.Error(err), zap.String("user_id", userID))
	}
	if err := a.repo.UpdateUserPresence(ctx, sqlc.UpdateUserPresenceParams{
		UserID:   userID,
		Status:   "offline",
		LastSeen: ptrTime(time.Now().UTC()),
	}); err != nil {
		a.log.Warn("update presence failed", zap.Error(err), zap.String("user_id", userID))
	}
	_ = a.bus.Publish(ctx, "events.presence", eventbus.Event{
		Type:     "user.offline",
		Stream:   "events.presence",
		ID:       userID,
		Metadata: map[string]string{"user_id": userID},
	})
	return nil
}

func (a *app) httpLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		trace := traceID(r)
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r.WithContext(logger.WithContext(r.Context(), trace, "")))
		fields := []zap.Field{
			zap.String("trace_id", trace),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", rw.status),
			zap.Duration("latency", time.Since(start)),
		}
		if claims, err := auth.AuthFromHeader(a.cfg.JWTSecret, r.Header.Get("Authorization")); err == nil {
			fields = append(fields, zap.String("user_id", claims.UserID))
		}
		a.log.Info("http_request", fields...)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message, trace string) {
	writeJSON(w, status, map[string]any{
		"code":     code,
		"message":  message,
		"trace_id": trace,
	})
}

func decodeAndValidate(v *validator.Validate, r *http.Request, out any) error {
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	if err := v.Struct(out); err != nil {
		return err
	}
	return nil
}

func traceID(r *http.Request) string {
	if t := strings.TrimSpace(r.Header.Get("X-Request-ID")); t != "" {
		return t
	}
	return fmt.Sprintf("req_%d", time.Now().UTC().UnixNano())
}

func authHeaderBearer(header string) string {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}

func strictOrigin(allowed []string) func(*http.Request) bool {
	allowedSet := map[string]struct{}{}
	for _, origin := range allowed {
		allowedSet[origin] = struct{}{}
	}
	return func(r *http.Request) bool {
		if len(allowedSet) == 0 {
			return true
		}
		origin := r.Header.Get("Origin")
		_, ok := allowedSet[origin]
		return ok
	}
}

func strictCORS(allowed []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			for _, allowedOrigin := range allowed {
				if origin == allowedOrigin {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
					w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
					w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
					break
				}
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func stringSlice(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func optionalString(value any) *string {
	if s, ok := value.(string); ok && s != "" {
		return &s
	}
	return nil
}

func optionalStringValue(payload map[string]any, key string) string {
	if s, ok := payload[key].(string); ok {
		return s
	}
	return ""
}

func callID(payload map[string]any) string {
	if v, ok := payload["call_id"].(string); ok && v != "" {
		return v
	}
	return fmt.Sprintf("call_%d", time.Now().UTC().UnixNano())
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
