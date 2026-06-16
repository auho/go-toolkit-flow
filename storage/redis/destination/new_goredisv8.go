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

func NewHashesWithGoRedisV8(client *redis.Client, c BulkConfig) (*Bulk[storage.MapEntry], error) {
	return newBulkWithGoRedisV8(format.NewHashesFormat(c.Key), client, c)
}

func NewListsWithGoRedisV8(client *redis.Client, c BulkConfig) (*Bulk[string], error) {
	return newBulkWithGoRedisV8(format.NewListsFormat(c.Key), client, c)
}

func NewSetsWithGoRedisV8(client *redis.Client, c BulkConfig) (*Bulk[string], error) {
	return newBulkWithGoRedisV8(format.NewSetsFormat(c.Key), client, c)
}

func NewSortedSetsWithGoRedisV8(client *redis.Client, c BulkConfig) (*Bulk[storage.ScoreMapEntry], error) {
	return newBulkWithGoRedisV8(format.NewSortedSetsFormat(c.Key), client, c)
}

func newBulkWithGoRedisV8[E storage.Entry](f format.Format[E], client *redis.Client, c BulkConfig) (*Bulk[E], error) {
	d, err := newGoRedisV8(client, c.getTimeoutDuration())
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk(f, d, c)
}

func newGoRedisV8(client *redis.Client, d time.Duration) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV8(ctx, client)
}
