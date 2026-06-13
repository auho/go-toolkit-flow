package goredisv8

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
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

func (g *goRedisV8) Truncate(keyName string) (int64, error) {
	return g.client.Del(context.Background(), keyName).Result()
}

func (g *goRedisV8) HashSet(keyName string, entries storage.MapEntries) error {
	ctx := context.Background()
	pipe := g.client.Pipeline()
	for _, entry := range entries {
		flat := flattenMapEntry(entry)
		pipe.HMSet(ctx, keyName, flat...)
	}
	_, err := pipe.Exec(ctx)
	pipe.Close()
	return err
}

func (g *goRedisV8) ListPush(keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}
	_, err := g.client.LPush(context.Background(), keyName, anyEntries...).Result()
	return err
}

func (g *goRedisV8) SetAdd(keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}
	_, err := g.client.SAdd(context.Background(), keyName, anyEntries...).Result()
	return err
}

func (g *goRedisV8) SortedSetAdd(keyName string, entries storage.ScoreMapEntries) error {
	ctx := context.Background()
	pipe := g.client.Pipeline()
	for _, entry := range entries {
		for k, v := range entry {
			pipe.ZAdd(ctx, keyName, &goredis.Z{
				Score:  v,
				Member: k,
			})
		}
	}
	_, err := pipe.Exec(ctx)
	pipe.Close()
	return err
}

func flattenMapEntry(entry storage.MapEntry) []any {
	flat := make([]any, 0, len(entry)*2)
	for k, v := range entry {
		flat = append(flat, k, v)
	}
	return flat
}
