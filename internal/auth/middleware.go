package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"wire-server/pkg/logger"
)

type Middleware struct {
	secret string
}

func NewMiddleware(secret string) *Middleware {
	return &Middleware{secret: secret}
}

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := requestTraceID(r)
		token := bearerToken(r.Header.Get("Authorization"))
		if token == "" {
			writeError(w, http.StatusUnauthorized, "AUTH_001", "missing bearer token", traceID)
			return
		}
		claims, err := ParseToken(token, m.secret)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "AUTH_002", "invalid or expired token", traceID)
			return
		}
		ctx := WithContext(r.Context(), claims)
		next.ServeHTTP(w, r.WithContext(logger.WithContext(ctx, claims.UserID, claims.UserID)))
	})
}

func bearerToken(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	TraceID string `json:"trace_id,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message, traceID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Code: code, Message: message, TraceID: traceID})
}

func AuthFromHeader(secret, header string) (Claims, error) {
	token := bearerToken(header)
	if token == "" {
		return Claims{}, errors.New("missing bearer token")
	}
	claims, err := ParseToken(token, secret)
	if err != nil {
		return Claims{}, fmt.Errorf("auth.AuthFromHeader: %w", err)
	}
	return claims, nil
}

func requestTraceID(r *http.Request) string {
	if traceID := strings.TrimSpace(r.Header.Get("X-Request-ID")); traceID != "" {
		return traceID
	}
	return fmt.Sprintf("req_%d", time.Now().UTC().UnixNano())
}
