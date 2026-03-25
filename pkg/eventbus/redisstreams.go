package eventbus

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type RedisStreams struct {
	client *goredis.Client
}

func NewRedisStreams(client *goredis.Client) *RedisStreams {
	return &RedisStreams{client: client}
}

func (r *RedisStreams) Publish(ctx context.Context, stream string, event Event) error {
	fields := map[string]any{
		"type":    event.Type,
		"id":      event.ID,
		"payload": string(event.Payload),
	}
	for k, v := range event.Metadata {
		fields["meta."+k] = v
	}
	if err := r.client.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: fields, MaxLenApprox: 10000}).Err(); err != nil {
		return fmt.Errorf("eventbus.Publish: %w", err)
	}
	return nil
}

func (r *RedisStreams) Subscribe(ctx context.Context, stream, group string) (<-chan Event, error) {
	if err := r.client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("eventbus.Subscribe create group: %w", err)
	}
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		consumer := fmt.Sprintf("%s-%d", group, time.Now().UnixNano())
		for {
			entries, err := r.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Streams:  []string{stream, ">"},
				Count:    32,
				Block:    5 * time.Second,
			}).Result()
			if err != nil {
				if err == context.Canceled || ctx.Err() != nil {
					return
				}
				continue
			}
			for _, entry := range entries {
				for _, msg := range entry.Messages {
					ev := Event{Stream: stream, ID: msg.ID, Metadata: map[string]string{}}
					if t, ok := msg.Values["type"].(string); ok {
						ev.Type = t
					}
					if payload, ok := msg.Values["payload"].(string); ok {
						ev.Payload = []byte(payload)
					}
					for k, v := range msg.Values {
						val, ok := v.(string)
						if !ok || len(k) < 5 || k[:5] != "meta." {
							continue
						}
						ev.Metadata[k[5:]] = val
					}
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
					_ = r.client.XAck(ctx, stream, group, msg.ID).Err()
				}
			}
		}
	}()
	return out, nil
}
