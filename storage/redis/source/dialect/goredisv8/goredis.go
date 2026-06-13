package goredisv8

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	goredis "github.com/go-redis/redis/v8"
)

var _ dialect.Dialect = (*goRedisV8)(nil)

type goRedisV8 struct {
	client *goredis.Client
}

func (g *goRedisV8) DBName() string {
	return g.client.Options().Addr
}

func (g *goRedisV8) Close() error {
	return g.client.Close()
}

func (g *goRedisV8) KeyLen(keyName string, keyType redis.KeyType) (int64, error) {
	ctx := context.Background()
	switch keyType {
	case redis.KeyTypeHashes:
		return g.client.HLen(ctx, keyName).Result()
	case redis.KeyTypeLists:
		return g.client.LLen(ctx, keyName).Result()
	case redis.KeyTypeSets:
		return g.client.SCard(ctx, keyName).Result()
	case redis.KeyTypeSortedSets:
		return g.client.ZCard(ctx, keyName).Result()
	default:
		return 0, fmt.Errorf("unsupported key type: %s", keyType)
	}
}

func (g *goRedisV8) HashScan(keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	keys, newCursor, err := g.client.HScan(context.Background(), keyName, cursor, "", count).Result()
	if err != nil {
		return nil, 0, err
	}
	return parseMapOfStringsEntries(keys), newCursor, nil
}

func (g *goRedisV8) ListRange(keyName string, start, stop int64) ([]string, error) {
	return g.client.LRange(context.Background(), keyName, start, stop).Result()
}

func (g *goRedisV8) SetScan(keyName string, cursor uint64, count int64) ([]string, uint64, error) {
	return g.client.SScan(context.Background(), keyName, cursor, "", count).Result()
}

func (g *goRedisV8) SortedSetScan(keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	keys, newCursor, err := g.client.ZScan(context.Background(), keyName, cursor, "", count).Result()
	if err != nil {
		return nil, 0, err
	}
	return parseMapOfStringsEntries(keys), newCursor, nil
}

func (g *goRedisV8) KeyScan(pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	return g.client.Scan(context.Background(), cursor, pattern, count).Result()
}

// parseMapOfStringsEntries parses flat key-value string pairs into MapOfStringsEntry slices.
func parseMapOfStringsEntries(items []string) storage.MapOfStringsEntries {
	entries := make(storage.MapOfStringsEntries, 0, len(items)/2)
	for i := 0; i < len(items)-1; i += 2 {
		entries = append(entries, storage.MapOfStringsEntry{items[i]: items[i+1]})
	}
	return entries
}
