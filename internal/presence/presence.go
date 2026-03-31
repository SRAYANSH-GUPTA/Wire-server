package presence

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Tracker struct {
	client *goredis.Client
	prefix string
	ttl    time.Duration
}

func New(client *goredis.Client) *Tracker {
	return &Tracker{
		client: client,
		prefix: "presence:user:",
		ttl:    75 * time.Second,
	}
}

func (t *Tracker) SetOnline(ctx context.Context, userPhone string) error {
	if err := t.client.Set(ctx, t.prefix+userPhone, "online", t.ttl).Err(); err != nil {
		return fmt.Errorf("presence.SetOnline: %w", err)
	}
	return nil
}

func (t *Tracker) SetOffline(ctx context.Context, userPhone string) error {
	if err := t.client.Set(ctx, t.prefix+userPhone, "offline", t.ttl).Err(); err != nil {
		return fmt.Errorf("presence.SetOffline: %w", err)
	}
	return nil
}

func (t *Tracker) TouchLastSeen(ctx context.Context, userPhone string) error {
	if err := t.client.Set(ctx, t.prefix+userPhone+":last_seen", time.Now().UTC().Format(time.RFC3339Nano), 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("presence.TouchLastSeen: %w", err)
	}
	return nil
}

func (t *Tracker) IsOnline(ctx context.Context, userPhone string) (bool, error) {
	value, err := t.client.Get(ctx, t.prefix+userPhone).Result()
	if err == goredis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("presence.IsOnline: %w", err)
	}
	return value == "online", nil
}
