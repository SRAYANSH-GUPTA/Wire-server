package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Client struct {
	client goredis.UniversalClient
	logger *zap.Logger
}

func NewClusterClient(ctx context.Context, addrs []string, logger *zap.Logger) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("redis: addrs must not be empty")
	}
	opts := &goredis.UniversalOptions{
		Addrs:    addrs,
		Username: strings.TrimSpace(os.Getenv("REDIS_USERNAME")),
		Password: strings.TrimSpace(os.Getenv("REDIS_PASSWORD")),
	}
	client := goredis.NewUniversalClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis.NewClusterClient ping: %w", err)
	}
	logger.Info("redis connected", zap.String("addrs", strings.Join(addrs, ",")))
	return &Client{client: client, logger: logger}, nil
}

func (c *Client) Cmdable() goredis.Cmdable {
	return c.client
}

func (c *Client) Incr(ctx context.Context, key string) *goredis.IntCmd {
	return c.client.Incr(ctx, key)
}

func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	return c.client.Expire(ctx, key, expiration)
}

func (c *Client) log(op string, start time.Time) {
	if c.logger == nil {
		return
	}
	c.logger.Debug("redis", zap.String("operation", op), zap.Int64("latency_ms", time.Since(start).Milliseconds()))
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	start := time.Now()
	defer c.log("get", start)
	return c.client.Get(ctx, key).Result()
}

func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	start := time.Now()
	defer c.log("set", start)
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *Client) HSet(ctx context.Context, key string, values ...any) error {
	start := time.Now()
	defer c.log("hset", start)
	return c.client.HSet(ctx, key, values...).Err()
}

func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	start := time.Now()
	defer c.log("hget", start)
	return c.client.HGet(ctx, key, field).Result()
}

func (c *Client) LPush(ctx context.Context, key string, values ...any) error {
	start := time.Now()
	defer c.log("lpush", start)
	return c.client.LPush(ctx, key, values...).Err()
}

func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	t := time.Now()
	defer c.log("lrange", t)
	return c.client.LRange(ctx, key, start, stop).Result()
}

func (c *Client) Publish(ctx context.Context, channel string, message any) error {
	start := time.Now()
	defer c.log("publish", start)
	return c.client.Publish(ctx, channel, message).Err()
}

func (c *Client) Subscribe(ctx context.Context, channels ...string) (*goredis.PubSub, error) {
	start := time.Now()
	defer c.log("subscribe", start)
	return c.client.Subscribe(ctx, channels...), nil
}

func (c *Client) XAdd(ctx context.Context, stream string, values map[string]any) (string, error) {
	start := time.Now()
	defer c.log("xadd", start)
	return c.client.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Result()
}

func (c *Client) XReadGroup(ctx context.Context, args *goredis.XReadGroupArgs) ([]goredis.XStream, error) {
	start := time.Now()
	defer c.log("xreadgroup", start)
	return c.client.XReadGroup(ctx, args).Result()
}

func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) error {
	start := time.Now()
	defer c.log("xack", start)
	return c.client.XAck(ctx, stream, group, ids...).Err()
}
