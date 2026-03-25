package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Client struct {
	*goredis.ClusterClient
	logger *zap.Logger
}

func NewClusterClient(ctx context.Context, addrs []string, logger *zap.Logger) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("redis: addrs must not be empty")
	}
	opts := &goredis.ClusterOptions{
		Addrs: addrs,
	}
	client := goredis.NewClusterClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis.NewClusterClient ping: %w", err)
	}
	logger.Info("redis cluster connected", zap.String("addrs", strings.Join(addrs, ",")))
	return &Client{ClusterClient: client, logger: logger}, nil
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
	return c.ClusterClient.Get(ctx, key).Result()
}

func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	start := time.Now()
	defer c.log("set", start)
	return c.ClusterClient.Set(ctx, key, value, ttl).Err()
}

func (c *Client) HSet(ctx context.Context, key string, values ...any) error {
	start := time.Now()
	defer c.log("hset", start)
	return c.ClusterClient.HSet(ctx, key, values...).Err()
}

func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	start := time.Now()
	defer c.log("hget", start)
	return c.ClusterClient.HGet(ctx, key, field).Result()
}

func (c *Client) LPush(ctx context.Context, key string, values ...any) error {
	start := time.Now()
	defer c.log("lpush", start)
	return c.ClusterClient.LPush(ctx, key, values...).Err()
}

func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	t := time.Now()
	defer c.log("lrange", t)
	return c.ClusterClient.LRange(ctx, key, start, stop).Result()
}

func (c *Client) Publish(ctx context.Context, channel string, message any) error {
	start := time.Now()
	defer c.log("publish", start)
	return c.ClusterClient.Publish(ctx, channel, message).Err()
}

func (c *Client) Subscribe(ctx context.Context, channels ...string) (*goredis.PubSub, error) {
	start := time.Now()
	defer c.log("subscribe", start)
	return c.ClusterClient.Subscribe(ctx, channels...), nil
}

func (c *Client) XAdd(ctx context.Context, stream string, values map[string]any) (string, error) {
	start := time.Now()
	defer c.log("xadd", start)
	return c.ClusterClient.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Result()
}

func (c *Client) XReadGroup(ctx context.Context, args *goredis.XReadGroupArgs) ([]goredis.XStream, error) {
	start := time.Now()
	defer c.log("xreadgroup", start)
	return c.ClusterClient.XReadGroup(ctx, args).Result()
}

func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) error {
	start := time.Now()
	defer c.log("xack", start)
	return c.ClusterClient.XAck(ctx, stream, group, ids...).Err()
}
