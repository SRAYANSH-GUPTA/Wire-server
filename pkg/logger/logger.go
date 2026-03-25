package logger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey struct{}

type Fields struct {
	TraceID     string
	SpanID      string
	UserID      string
	ServiceName string
	LatencyMS   int64
}

func NewLogger(env, serviceName string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if strings.EqualFold(env, "development") {
		cfg = zap.NewDevelopmentConfig()
	}
	cfg.Encoding = "json"
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	if cfg.InitialFields == nil {
		cfg.InitialFields = map[string]interface{}{}
	}
	cfg.InitialFields["service_name"] = serviceName
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	return cfg.Build()
}

func WithContext(ctx context.Context, traceID, spanID, userID, serviceName string, latency time.Duration) context.Context {
	current, _ := ctx.Value(contextKey{}).(Fields)
	if traceID != "" {
		current.TraceID = traceID
	}
	if spanID != "" {
		current.SpanID = spanID
	}
	if userID != "" {
		current.UserID = userID
	}
	if serviceName != "" {
		current.ServiceName = serviceName
	}
	if latency > 0 {
		current.LatencyMS = latency.Milliseconds()
	}
	return context.WithValue(ctx, contextKey{}, current)
}

func FieldsFromContext(ctx context.Context) Fields {
	fields, _ := ctx.Value(contextKey{}).(Fields)
	return fields
}

func FieldsForLogging(ctx context.Context, extra ...zap.Field) []zap.Field {
	fields := FieldsFromContext(ctx)
	zapFields := make([]zap.Field, 0, 5+len(extra))
	if fields.TraceID != "" {
		zapFields = append(zapFields, zap.String("trace_id", fields.TraceID))
	}
	if fields.SpanID != "" {
		zapFields = append(zapFields, zap.String("span_id", fields.SpanID))
	}
	if fields.ServiceName != "" {
		zapFields = append(zapFields, zap.String("service_name", fields.ServiceName))
	}
	if fields.UserID != "" {
		zapFields = append(zapFields, zap.String("user_id", fields.UserID))
	}
	if fields.LatencyMS > 0 {
		zapFields = append(zapFields, zap.Int64("latency_ms", fields.LatencyMS))
	}
	zapFields = append(zapFields, extra...)
	return zapFields
}

func ContextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

type loggerKey struct{}

func FromContext(ctx context.Context) (*zap.Logger, error) {
	logger, ok := ctx.Value(loggerKey{}).(*zap.Logger)
	if !ok || logger == nil {
		return nil, fmt.Errorf("logger not found in context")
	}
	return logger, nil
}
