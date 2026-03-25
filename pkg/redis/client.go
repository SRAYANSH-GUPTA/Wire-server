package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Client struct {
	*goredis.Client
}

func New(ctx context.Context, rawURL string) (*Client, error) {
	opts, err := goredis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("redis.New parse url: %w", err)
	}
	opts.PoolSize = 25
	opts.MinIdleConns = 2
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 3 * time.Second
	opts.WriteTimeout = 3 * time.Second
	client := goredis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis.New ping: %w", err)
	}
	return &Client{Client: client}, nil
}
