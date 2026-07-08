package goredis

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
	clientgoredis "github.com/auho/go-toolkit-flow/v3/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
)

var _ dialect.Dialect = (*sourceDialect)(nil)

// sourceDialect is a version-agnostic source dialect implementation.
// It delegates all Redis operations to a RedisClient (V8 or V9 wrapper),
// so dialect logic is written once and works with either go-redis version.
type sourceDialect struct {
	client clientgoredis.SourceClient
}

func (d *sourceDialect) DB() int {
	return d.client.DB()
}

func (d *sourceDialect) Close() error {
	return d.client.Close()
}

func (d *sourceDialect) HashLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.HashLen(ctx, keyName)
}

func (d *sourceDialect) HashScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.StringMapEntries, uint64, error) {
	keys, newCursor, err := d.client.HashScan(ctx, keyName, cursor, "", count)
	if err != nil {
		return nil, 0, err
	}

	return parseStringMapEntries(keys), newCursor, nil
}

func (d *sourceDialect) ListLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.ListLen(ctx, keyName)
}

func (d *sourceDialect) ListRange(ctx context.Context, keyName string, start, stop int64) ([]string, error) {
	return d.client.ListRange(ctx, keyName, start, stop)
}

func (d *sourceDialect) SetLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.SetLen(ctx, keyName)
}

func (d *sourceDialect) SetScan(ctx context.Context, keyName string, cursor uint64, count int64) ([]string, uint64, error) {
	return d.client.SetScan(ctx, keyName, cursor, "", count)
}

func (d *sourceDialect) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return d.client.SortedSetLen(ctx, keyName)
}

func (d *sourceDialect) SortedSetScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.StringMapEntries, uint64, error) {
	keys, newCursor, err := d.client.SortedSetScan(ctx, keyName, cursor, "", count)
	if err != nil {
		return nil, 0, err
	}

	return parseStringMapEntries(keys), newCursor, nil
}

func (d *sourceDialect) KeyScan(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	return d.client.KeyScan(ctx, pattern, cursor, count)
}

// parseStringMapEntries parses flat key-value string pairs into StringMapEntry slices.
func parseStringMapEntries(items []string) storage.StringMapEntries {
	entries := make(storage.StringMapEntries, 0, len(items)/2)
	for i := 0; i < len(items)-1; i += 2 {
		entries = append(entries, storage.StringMapEntry{items[i]: items[i+1]})
	}

	return entries
}
