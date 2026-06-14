package goredis

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/client/goredis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/go-redis/redis/v8"
)

var _ dialect.Dialect = (*v8)(nil)

type v8 struct {
	*goredis.V8
}

func (v *v8) HashMSet(ctx context.Context, keyName string, entries storage.MapEntries) error {
	pipe := v.Client.Pipeline()
	for _, entry := range entries {
		flat := v.flattenMapEntry(entry)
		pipe.HMSet(ctx, keyName, flat...)
	}

	_, err := pipe.Exec(ctx)
	_ = pipe.Close()

	return err
}

func (v *v8) ListPush(ctx context.Context, keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}

	_, err := v.Client.LPush(ctx, keyName, anyEntries...).Result()
	return err
}

func (v *v8) SetAdd(ctx context.Context, keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}

	_, err := v.Client.SAdd(ctx, keyName, anyEntries...).Result()
	return err
}

func (v *v8) SortedSetAdd(ctx context.Context, keyName string, entries storage.ScoreMapEntries) error {
	pipe := v.Client.Pipeline()
	for _, entry := range entries {
		for k, v := range entry {
			pipe.ZAdd(ctx, keyName, &redis.Z{
				Score:  v,
				Member: k,
			})
		}
	}

	_, err := pipe.Exec(ctx)
	_ = pipe.Close()

	return err
}

func (v *v8) flattenMapEntry(entry storage.MapEntry) []any {
	flat := make([]any, 0, len(entry)*2)
	for k, v := range entry {
		flat = append(flat, k, v)
	}

	return flat
}
