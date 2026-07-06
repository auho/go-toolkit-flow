package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/format"
	"github.com/redis/go-redis/v9"
)

func NewHashesWithGoRedisV9(client *redis.Client, c BulkConfig) (*Bulk[storage.MapEntry], error) {
	return newBulkWithGoRedisV9(format.NewHashesFormat(c.Key), client, c)
}

func NewListsWithGoRedisV9(client *redis.Client, c BulkConfig) (*Bulk[string], error) {
	return newBulkWithGoRedisV9(format.NewListsFormat(c.Key), client, c)
}

func NewSetsWithGoRedisV9(client *redis.Client, c BulkConfig) (*Bulk[string], error) {
	return newBulkWithGoRedisV9(format.NewSetsFormat(c.Key), client, c)
}

func NewSortedSetsWithGoRedisV9(client *redis.Client, c BulkConfig) (*Bulk[storage.ScoreMapEntry], error) {
	return newBulkWithGoRedisV9(format.NewSortedSetsFormat(c.Key), client, c)
}

func newBulkWithGoRedisV9[E storage.Entry](f format.Format[E], client *redis.Client, c BulkConfig) (*Bulk[E], error) {
	d, err := newGoRedisV9(client, c.getTimeoutDuration())
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}

	return newBulk(f, d, c)
}

func newGoRedisV9(client *redis.Client, d time.Duration) (dialect.Dialect, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	return goredis.NewDialectGoRedisV9(ctx, client)
}
