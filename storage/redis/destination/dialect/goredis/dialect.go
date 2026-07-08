package goredis

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
	clientgoredis "github.com/auho/go-toolkit-flow/v3/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
)

var _ dialect.Dialect = (*destDialect)(nil)

// destDialect is a version-agnostic destination dialect implementation.
// It delegates all Redis operations to a RedisClient (V8 or V9 wrapper),
// so dialect logic is written once and works with either go-redis version.
type destDialect struct {
	client clientgoredis.DestClient
}

func (d *destDialect) DB() int {
	return d.client.DB()
}

func (d *destDialect) Close() error {
	return d.client.Close()
}

func (d *destDialect) Truncate(ctx context.Context, keyName string) (int64, error) {
	return d.client.Truncate(ctx, keyName)
}

func (d *destDialect) HashLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.HashLen(ctx, keyName)
}

func (d *destDialect) HashMSet(ctx context.Context, keyName string, entries storage.MapEntries) error {
	return d.client.HashMSet(ctx, keyName, entries)
}

func (d *destDialect) ListLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.ListLen(ctx, keyName)
}

func (d *destDialect) ListPush(ctx context.Context, keyName string, entries []string) error {
	return d.client.ListPush(ctx, keyName, entries)
}

func (d *destDialect) SetLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.SetLen(ctx, keyName)
}

func (d *destDialect) SetAdd(ctx context.Context, keyName string, entries []string) error {
	return d.client.SetAdd(ctx, keyName, entries)
}

func (d *destDialect) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.SortedSetLen(ctx, keyName)
}

func (d *destDialect) SortedSetAdd(ctx context.Context, keyName string, entries storage.ScoreMapEntries) error {
	return d.client.SortedSetAdd(ctx, keyName, entries)
}
