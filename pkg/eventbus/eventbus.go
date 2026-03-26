package eventbus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type EventBus interface {
	Publish(ctx context.Context, stream string, msg proto.Message) error
	Subscribe(ctx context.Context, stream, group string, fn func(ctx context.Context, payload []byte)) error
	Ack(ctx context.Context, stream, group, id string) error
}

type Config struct {
	Driver       string
	Logger       *zap.Logger
	Client       redis.Cmdable
	KafkaBrokers []string
}

func NewEventBus(cfg Config) EventBus {
	if strings.EqualFold(cfg.Driver, "kafka") {
		return NewKafkaEventBus(cfg.KafkaBrokers, cfg.Logger)
	}
	if cfg.Client == nil {
		panic("eventbus redis client is required")
	}
	return &RedisStreams{
		client: cfg.Client,
		logger: cfg.Logger,
	}
}

type RedisStreams struct {
	client redis.Cmdable
	logger *zap.Logger
}

func (r *RedisStreams) Publish(ctx context.Context, stream string, msg proto.Message) error {
	start := time.Now()
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("eventbus publish marshal: %w", err)
	}
	if _, err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"payload": payload,
		},
	}).Result(); err != nil {
		return fmt.Errorf("eventbus publish xadd: %w", err)
	}
	if r.logger != nil {
		r.logger.Debug("eventbus publish", zap.String("stream", stream), zap.Int64("latency_ms", time.Since(start).Milliseconds()))
	}
	return nil
}

func (r *RedisStreams) Subscribe(ctx context.Context, stream, group string, fn func(ctx context.Context, payload []byte)) error {
	if _, err := r.client.XGroupCreateMkStream(ctx, stream, group, "0").Result(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("eventbus subscribe group: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			entries, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: fmt.Sprintf("%s-%d", group, time.Now().UnixNano()),
				Streams:  []string{stream, ">"},
				Count:    16,
				Block:    5 * time.Second,
			}).Result()
			if err != nil {
				if err == context.Canceled || ctx.Err() != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			for _, entry := range entries {
				for _, msg := range entry.Messages {
					if raw, ok := msg.Values["payload"].(string); ok {
						fn(ctx, []byte(raw))
					}
					_ = r.Ack(ctx, stream, group, msg.ID)
				}
			}
		}
	}()
	return nil
}

func (r *RedisStreams) Ack(ctx context.Context, stream, group, id string) error {
	start := time.Now()
	if err := r.client.XAck(ctx, stream, group, id).Err(); err != nil {
		return fmt.Errorf("eventbus ack: %w", err)
	}
	if r.logger != nil {
		r.logger.Debug("eventbus ack", zap.String("stream", stream), zap.Int64("latency_ms", time.Since(start).Milliseconds()))
	}
	return nil
}
