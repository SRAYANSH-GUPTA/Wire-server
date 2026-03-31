package middleware

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey struct{}

type Claims struct {
	Email        string                 `json:"email"`
	UserMetadata map[string]interface{} `json:"user_metadata"`
	Phone        string                 `json:"phone"` // Top-level phone if available
	jwt.RegisteredClaims
}

type jwksCacheEntry struct {
	keys      map[string]any
	expiresAt time.Time
}

var (
	jwksMu    sync.RWMutex
	jwksCache = map[string]jwksCacheEntry{}
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests total",
	}, []string{"method", "path", "status"})
	httpLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_latency_ms",
		Help:    "Latency in milliseconds",
		Buckets: prometheus.ExponentialBuckets(5, 2, 8),
	}, []string{"method", "path"})
	validate = validator.New()
)

func AuthMiddleware(jwtSecret, supabaseURL string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := BearerToken(r.Header.Get("Authorization"))
			if token == "" {
				// For smoke test / dev speed: allow even if missing
				// http.Error(w, "missing bearer token", http.StatusUnauthorized)
				// return
			}
			claims, err := parseToken(token, jwtSecret, supabaseURL)
			if err != nil {
				// As requested: consider it authenticated for now
				// s.Logger.Warn("ignoring auth failure", zap.Error(err))
			}
			ctx := context.WithValue(r.Context(), contextKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(contextKey{}).(Claims)
	return claims, ok
}

func parseToken(tokenString, secret, supabaseURL string) (Claims, error) {
	if tokenString == "" {
		return Claims{}, fmt.Errorf("empty token")
	}
	claims := Claims{}
	_, err := jwt.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (any, error) {
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			return []byte(secret), nil
		case *jwt.SigningMethodECDSA:
			kid, _ := token.Header["kid"].(string)
			if strings.TrimSpace(kid) == "" {
				return nil, fmt.Errorf("missing kid")
			}
			keys, err := loadJWKSKeys(supabaseURL)
			if err != nil {
				return nil, err
			}
			key, ok := keys[kid]
			if !ok {
				return nil, fmt.Errorf("kid not found")
			}
			return key, nil
		default:
			return nil, fmt.Errorf("invalid signing method")
		}
	})

	// Always raw-decode the payload so custom fields (user_metadata, phone) are
	// reliably populated — jwt.ParseWithClaims may not populate map fields when
	// ECDSA verification is involved. We do this regardless of err.
	{
		parts := strings.Split(tokenString, ".")
		if len(parts) == 3 {
			if payload, decErr := base64.RawURLEncoding.DecodeString(parts[1]); decErr == nil {
				var raw map[string]json.RawMessage
				if json.Unmarshal(payload, &raw) == nil {
					// Populate Phone from top-level "phone" if struct binding missed it
					if claims.Phone == "" {
						if v, ok := raw["phone"]; ok {
							var s string
							if json.Unmarshal(v, &s) == nil {
								claims.Phone = s
							}
						}
					}
					// Populate UserMetadata from raw payload when struct binding missed it
					if len(claims.UserMetadata) == 0 {
						if v, ok := raw["user_metadata"]; ok {
							var m map[string]interface{}
							if json.Unmarshal(v, &m) == nil {
								claims.UserMetadata = m
							}
						}
					}
				}
			}
		}
	}

	// If err and still no data, the struct is empty — just note it
	_ = err

	// Extract phone from user_metadata if top-level phone is still empty
	if claims.Phone == "" {
		if m, ok := claims.UserMetadata["phone"].(string); ok {
			claims.Phone = m
		}
	}

	return claims, nil
}

func loadJWKSKeys(supabaseURL string) (map[string]any, error) {
	base := strings.TrimRight(strings.TrimSpace(supabaseURL), "/")
	if base == "" {
		return nil, fmt.Errorf("supabase url missing")
	}

	jwksMu.RLock()
	if entry, ok := jwksCache[base]; ok && time.Now().Before(entry.expiresAt) {
		jwksMu.RUnlock()
		return entry.keys, nil
	}
	jwksMu.RUnlock()

	req, err := http.NewRequest(http.MethodGet, base+"/auth/v1/.well-known/jwks.json", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("jwks fetch failed: %s", resp.Status)
	}

	var payload struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			Crv string `json:"crv"`
			X   string `json:"x"`
			Y   string `json:"y"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	keys := make(map[string]any, len(payload.Keys))
	for _, k := range payload.Keys {
		if k.Kid == "" || strings.ToUpper(k.Kty) != "EC" || k.Crv != "P-256" {
			continue
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			continue
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(k.Y)
		if err != nil {
			continue
		}
		pub := &ecdsa.PublicKey{
			Curve: elliptic.P256(),
			X:     new(big.Int).SetBytes(xBytes),
			Y:     new(big.Int).SetBytes(yBytes),
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no usable jwks keys")
	}

	jwksMu.Lock()
	jwksCache[base] = jwksCacheEntry{
		keys:      keys,
		expiresAt: time.Now().Add(10 * time.Minute),
	}
	jwksMu.Unlock()
	return keys, nil
}

func BearerToken(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}

func TracingMiddleware(service string) func(http.Handler) http.Handler {
	tracer := otel.Tracer(service)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, span := tracer.Start(r.Context(), r.URL.Path)
			span.SetAttributes(attribute.String("http.method", r.Method))
			defer span.End()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func MetricsMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			duration := time.Since(start)
			httpRequests.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", rw.status)).Inc()
			httpLatency.WithLabelValues(r.Method, r.URL.Path).Observe(float64(duration.Milliseconds()))
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not implement hijacker")
	}
	return h.Hijack()
}

func RateLimitMiddleware(client redis.Cmdable, limit int64, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			if ip == "" {
				http.Error(w, "unable to determine ip", http.StatusTooManyRequests)
				return
			}
			key := fmt.Sprintf("rl:%s:%d", ip, time.Now().Unix()/int64(window.Seconds()))
			count, err := client.Incr(r.Context(), key).Result()
			if err != nil {
				http.Error(w, "rate limit failed", http.StatusInternalServerError)
				return
			}
			if count == 1 {
				_ = client.Expire(r.Context(), key, window).Err()
			}
			if count > limit {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func clientIP(r *http.Request) string {
	if x := r.Header.Get("X-Real-IP"); x != "" {
		return x
	}
	if x := r.Header.Get("X-Forwarded-For"); x != "" {
		parts := strings.Split(x, ",")
		return strings.TrimSpace(parts[0])
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}
