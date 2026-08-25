package redisstore

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type scriptClient interface {
	ScriptLoad(context.Context, string) (string, error)
	EvalSHA(context.Context, string, []string, ...any) (any, error)
}

type Client struct {
	client *redis.Client
}

func NewClient(client *redis.Client) *Client {
	return &Client{client: client}
}

func (c *Client) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.client.ScriptLoad(ctx, script).Result()
}

func (c *Client) EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error) {
	return c.client.EvalSha(ctx, sha, keys, args...).Result()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.client.Close()
}
