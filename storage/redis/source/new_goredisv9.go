package source

import (
	"context"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/format"
	"github.com/redis/go-redis/v9"
)

func NewHashesWithGoRedisV9(client *redis.Client, c KeyConfig) (*Iterator[storage.StringMapEntry], error) {
	return newIteratorWithGoRedisV9(format.NewHashesFormat(c.Key), client, c)
}

func NewListsWithGoRedisV9(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV9(format.NewListsFormat(c.Key), client, c)
}

func NewSetsWithGoRedisV9(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV9(format.NewSetsFormat(c.Key), client, c)
}

func NewSortedSetsWithGoRedisV9(client *redis.Client, c KeyConfig) (*Iterator[storage.StringMapEntry], error) {
	return newIteratorWithGoRedisV9(format.NewSortedSetsFormat(c.Key), client, c)
}

func NewScanWithGoRedisV9(client *redis.Client, c KeyConfig) (*Iterator[string], error) {
	return newIteratorWithGoRedisV9(format.NewScanFormat(c.Key), client, c)
}

func newIteratorWithGoRedisV9[E storage.Entry](f format.Format[E], client *redis.Client, c KeyConfig) (*Iterator[E], error) {
	d, err := newGoRedisV9(client, c.getTimeoutDuration())
	if err != nil {
		return nil, fmt.Errorf("newGoRedisV9: %w", err)
	}

	return newIterator(f, d, c)
}

func newGoRedisV9(client *redis.Client, d time.Duration) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV9(ctx, client)
}
