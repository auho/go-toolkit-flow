package goredisv8

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	goredis "github.com/go-redis/redis/v8"
)

func NewDialectGoRedisV8(client *goredis.Client) (dialect.Dialect, error) {
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}
	return &goRedisV8{client: client}, nil
}
