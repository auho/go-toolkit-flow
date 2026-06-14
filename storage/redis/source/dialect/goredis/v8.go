package goredis

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ dialect.Dialect = (*v8)(nil)

type v8 struct {
	*goredis.V8
}

func (v *v8) HashScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	keys, newCursor, err := v.Client.HScan(ctx, keyName, cursor, "", count).Result()
	if err != nil {
		return nil, 0, err
	}

	return v.parseMapOfStringsEntries(keys), newCursor, nil
}

func (v *v8) ListRange(ctx context.Context, keyName string, start, stop int64) ([]string, error) {
	return v.Client.LRange(ctx, keyName, start, stop).Result()
}

func (v *v8) SetScan(ctx context.Context, keyName string, cursor uint64, count int64) ([]string, uint64, error) {
	return v.Client.SScan(ctx, keyName, cursor, "", count).Result()
}

func (v *v8) SortedSetScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	keys, newCursor, err := v.Client.ZScan(ctx, keyName, cursor, "", count).Result()
	if err != nil {
		return nil, 0, err
	}

	return v.parseMapOfStringsEntries(keys), newCursor, nil
}

func (v *v8) KeyScan(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	return v.Client.Scan(ctx, cursor, pattern, count).Result()
}

// parseMapOfStringsEntries parses flat key-value string pairs into MapOfStringsEntry slices.
func (v *v8) parseMapOfStringsEntries(items []string) storage.MapOfStringsEntries {
	entries := make(storage.MapOfStringsEntries, 0, len(items)/2)
	for i := 0; i < len(items)-1; i += 2 {
		entries = append(entries, storage.MapOfStringsEntry{items[i]: items[i+1]})
	}

	return entries
}
