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

func (t *Tracker) SetOnline(ctx context.Context, userID string) error {
	if err := t.client.Set(ctx, t.prefix+userID, "online", t.ttl).Err(); err != nil {
		return fmt.Errorf("presence.SetOnline: %w", err)
	}
	return nil
}

func (t *Tracker) SetOffline(ctx context.Context, userID string) error {
	if err := t.client.Set(ctx, t.prefix+userID, "offline", t.ttl).Err(); err != nil {
		return fmt.Errorf("presence.SetOffline: %w", err)
	}
	return nil
}

func (t *Tracker) TouchLastSeen(ctx context.Context, userID string) error {
	if err := t.client.Set(ctx, t.prefix+userID+":last_seen", time.Now().UTC().Format(time.RFC3339Nano), 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("presence.TouchLastSeen: %w", err)
	}
	return nil
}

func (t *Tracker) IsOnline(ctx context.Context, userID string) (bool, error) {
	value, err := t.client.Get(ctx, t.prefix+userID).Result()
	if err == goredis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("presence.IsOnline: %w", err)
	}
	return value == "online", nil
}
