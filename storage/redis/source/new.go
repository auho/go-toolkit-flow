package source

import (
	"context"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect/goredis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
	"github.com/go-redis/redis/v8"
)

func NewHashesWithGoRedisV8(config KeyConfig, client *redis.Client) (*Key[storage.MapOfStringsEntry], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newKey[storage.MapOfStringsEntry](config, d, format.NewHashesFormat())
}

func NewListsWithGoRedisV8(config KeyConfig, client *redis.Client) (*Key[string], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newKey[string](config, d, format.NewListsFormat())
}

func NewSetsWithGoRedisV8(config KeyConfig, client *redis.Client) (*Key[string], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newKey[string](config, d, format.NewSetsFormat())
}

func NewSortedSetsWithGoRedisV8(config KeyConfig, client *redis.Client) (*Key[storage.MapOfStringsEntry], error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newKey[storage.MapOfStringsEntry](config, d, format.NewSortedSetsFormat())
}

func NewScanWithGoRedisV8(config KeyConfig, client *redis.Client) (*ScanKey, error) {
	d, err := newGoRedisV8(config.GetTimeOutDuration(), client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newScanKey(config, d, format.NewScanFormat())
}

func newGoRedisV8(d time.Duration, client *redis.Client) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV8(ctx, client)
}
