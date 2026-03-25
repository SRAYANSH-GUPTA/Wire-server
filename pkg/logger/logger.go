package logger

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey struct{}

type Fields struct {
	TraceID string
	UserID  string
}

func New(env string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if strings.EqualFold(env, "development") {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.TimeKey = "ts"
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncoderConfig.TimeKey = "ts"
		cfg.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	}
	return cfg.Build()
}

func WithContext(ctx context.Context, traceID, userID string) context.Context {
	current, _ := ctx.Value(contextKey{}).(Fields)
	if traceID != "" {
		current.TraceID = traceID
	}
	if userID != "" {
		current.UserID = userID
	}
	return context.WithValue(ctx, contextKey{}, current)
}

func FieldsFromContext(ctx context.Context) Fields {
	fields, _ := ctx.Value(contextKey{}).(Fields)
	return fields
}

func WithFields(logger *zap.Logger, ctx context.Context) *zap.Logger {
	fields := FieldsFromContext(ctx)
	attrs := make([]zap.Field, 0, 2)
	if fields.TraceID != "" {
		attrs = append(attrs, zap.String("trace_id", fields.TraceID))
	}
	if fields.UserID != "" {
		attrs = append(attrs, zap.String("user_id", fields.UserID))
	}
	return logger.With(attrs...)
}
