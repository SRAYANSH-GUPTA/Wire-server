module wire-server

go 1.22

require (
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/go-chi/chi/v5 v5.2.3
	github.com/go-playground/validator/v10 v10.27.0
	github.com/gobwas/ws v4.0.3
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/golang-migrate/migrate/v4 v4.18.3
	github.com/jackc/pgx/v5 v5.7.6
	github.com/redis/go-redis/v9 v9.12.1
	github.com/sony/gobreaker v0.5.0
	github.com/spf13/viper v1.21.0
	go.opentelemetry.io/otel v1.38.0
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.59.0
	google.golang.org/protobuf v1.36.10
	golang.org/x/sys v0.12.0
)
