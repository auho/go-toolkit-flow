package goredis

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
	"github.com/go-redis/redis/v8"
)

func NewDialectGoRedisV8(ctx context.Context, client *redis.Client) (dialect.Dialect, error) {
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return &v8{V8: &goredis.V8{Client: client}}, nil
}
