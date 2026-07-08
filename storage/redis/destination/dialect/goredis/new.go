package goredis

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
	"github.com/go-redis/redis/v8"
	v9redis "github.com/redis/go-redis/v9"
)

func NewDialectGoRedisV8(ctx context.Context, client *redis.Client) (dialect.Dialect, error) {
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &destDialect{client: &goredis.V8{Client: client}}, nil
}

func NewDialectGoRedisV9(ctx context.Context, client *v9redis.Client) (dialect.Dialect, error) {
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &destDialect{client: &goredis.V9{Client: client}}, nil
}
