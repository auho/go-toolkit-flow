package source

import (
	"context"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/format"
	"github.com/go-redis/redis/v8"
)

func NewHashesWithGoRedisV8(client *redis.Client, c KeyConfig) (*Iterator[storage.StringMapEntry], error) {
	return newIteratorWithGoRedisV8(format.NewHashesFormat(c.Key), client, c)
}

func NewListsWithGoRedisV8(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV8(format.NewListsFormat(c.Key), client, c)
}

func NewSetsWithGoRedisV8(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV8(format.NewSetsFormat(c.Key), client, c)
}

func NewSortedSetsWithGoRedisV8(client *redis.Client, c KeyConfig) (*Iterator[storage.StringMapEntry], error) {
	return newIteratorWithGoRedisV8(format.NewSortedSetsFormat(c.Key), client, c)
}

func NewScanWithGoRedisV8(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV8(format.NewScanFormat(c.Key), client, c)
}

func newIteratorWithGoRedisV8[E storage.Entry](f format.Format[E], client *redis.Client, c KeyConfig) (*Iterator[E], error) {
	d, err := newGoRedisV8(client, c.getTimeoutDuration())
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newIterator(f, d, c)
}

func newGoRedisV8(client *redis.Client, d time.Duration) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV8(ctx, client)
}
