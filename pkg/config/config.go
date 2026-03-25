package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	AppEnv                string   `mapstructure:"APP_ENV" validate:"required,oneof=development production"`
	Port                  string   `mapstructure:"PORT" validate:"required"`
	JWTSecret             string   `mapstructure:"JWT_SECRET" validate:"required"`
	DatabaseURL           string   `mapstructure:"DATABASE_URL" validate:"required"`
	RedisURL              string   `mapstructure:"REDIS_URL" validate:"required"`
	S3Bucket              string   `mapstructure:"S3_BUCKET" validate:"required"`
	S3Region              string   `mapstructure:"S3_REGION" validate:"required"`
	AWSAccessKeyID        string   `mapstructure:"AWS_ACCESS_KEY_ID" validate:"required"`
	AWSSecretAccessKey    string   `mapstructure:"AWS_SECRET_ACCESS_KEY" validate:"required"`
	TurnSecret            string   `mapstructure:"TURN_SECRET" validate:"required"`
	FCMServerKey          string   `mapstructure:"FCM_SERVER_KEY" validate:"required"`
	CORSAllowedOrigins    []string `mapstructure:"CORS_ALLOWED_ORIGINS"`
	ShutdownTimeout       time.Duration
	WebSocketDrainTimeout time.Duration
}

func Load() Config {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetDefault("APP_ENV", "development")
	v.SetDefault("PORT", "8080")
	v.SetDefault("CORS_ALLOWED_ORIGINS", "http://localhost:3000")
	v.SetDefault("SHUTDOWN_TIMEOUT", "15s")
	v.SetDefault("WS_DRAIN_TIMEOUT", "5s")

	cfg := Config{
		AppEnv:                v.GetString("APP_ENV"),
		Port:                  v.GetString("PORT"),
		JWTSecret:             v.GetString("JWT_SECRET"),
		DatabaseURL:           v.GetString("DATABASE_URL"),
		RedisURL:              v.GetString("REDIS_URL"),
		S3Bucket:              v.GetString("S3_BUCKET"),
		S3Region:              v.GetString("S3_REGION"),
		AWSAccessKeyID:        v.GetString("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey:    v.GetString("AWS_SECRET_ACCESS_KEY"),
		TurnSecret:            v.GetString("TURN_SECRET"),
		FCMServerKey:          v.GetString("FCM_SERVER_KEY"),
		CORSAllowedOrigins:    splitCSV(v.GetString("CORS_ALLOWED_ORIGINS")),
		ShutdownTimeout:       v.GetDuration("SHUTDOWN_TIMEOUT"),
		WebSocketDrainTimeout: v.GetDuration("WS_DRAIN_TIMEOUT"),
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	if cfg.WebSocketDrainTimeout == 0 {
		cfg.WebSocketDrainTimeout = 5 * time.Second
	}
	if err := validator.New().Struct(cfg); err != nil {
		panic(fmt.Errorf("config.Load: %w", err))
	}
	return cfg
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
