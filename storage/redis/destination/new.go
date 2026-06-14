package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect/goredis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/format"
	"github.com/go-redis/redis/v8"
)

func NewHashesWithGoRedisV8(config BulkConfig, client *redis.Client) (*Bulk[storage.MapEntry], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk[storage.MapEntry](config, d, format.NewHashesFormat())
}

func NewListsWithGoRedisV8(config BulkConfig, client *redis.Client) (*Bulk[string], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk[string](config, d, format.NewListsFormat())
}

func NewSetsWithGoRedisV8(config BulkConfig, client *redis.Client) (*Bulk[string], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk[string](config, d, format.NewSetsFormat())
}

func NewSortedSetsWithGoRedisV8(config BulkConfig, client *redis.Client) (*Bulk[storage.ScoreMapEntry], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk[storage.ScoreMapEntry](config, d, format.NewSortedSetsFormat())
}

func newGoRedisV8(d time.Duration, client *redis.Client) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV8(ctx, client)
}
