// Package goredis provides a Redis client wrapper for go-redis v8.
package goredis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// V8 wraps a go-redis v8 client.
type V8 struct {
	// Client is the underlying go-redis client.
	// Exported because V8 is not exposed to external callers (returned as
	// dialect.Dialect via newGoRedisV8); Client is accessed directly by the
	// dialect package via struct embedding.
	Client *redis.Client
}

// DB returns the configured database index.
func (v *V8) DB() int {
	return v.Client.Options().DB
}

// Close closes the Redis connection.
func (v *V8) Close() error {
	return v.Client.Close()
}

// Truncate deletes all entries of the given key.
func (v *V8) Truncate(ctx context.Context, keyName string) (int64, error) {
	return v.Client.Del(ctx, keyName).Result()
}

// HashLen returns the number of fields in a hash.
func (v *V8) HashLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.HLen(ctx, keyName).Result()
}

// HashScan iterates hash fields with SCAN.
func (v *V8) HashScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return v.Client.HScan(ctx, keyName, cursor, match, count).Result()
}

// HashMSet batch-sets hash fields using a pipeline.
func (v *V8) HashMSet(ctx context.Context, keyName string, entries []map[string]any) error {
	pipe := v.Client.Pipeline()
	for _, entry := range entries {
		pipe.HMSet(ctx, keyName, flattenMapEntry(entry)...)
	}

	_, err := pipe.Exec(ctx)
	_ = pipe.Close()

	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// ListLen returns the length of a list.
func (v *V8) ListLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.LLen(ctx, keyName).Result()
}

// ListRange returns a range of list elements.
func (v *V8) ListRange(ctx context.Context, keyName string, start, stop int64) ([]string, error) {
	return v.Client.LRange(ctx, keyName, start, stop).Result()
}

// ListPush prepends values to a list.
func (v *V8) ListPush(ctx context.Context, keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}

	_, err := v.Client.LPush(ctx, keyName, anyEntries...).Result()
	if err != nil {
		return fmt.Errorf("LPush: %w", err)
	}

	return nil
}

// SetLen returns the number of members in a set.
func (v *V8) SetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.SCard(ctx, keyName).Result()
}

// SetScan iterates set members with SCAN.
func (v *V8) SetScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return v.Client.SScan(ctx, keyName, cursor, match, count).Result()
}

// SetAdd adds members to a set.
func (v *V8) SetAdd(ctx context.Context, keyName string, entries []string) error {
	anyEntries := make([]any, 0, len(entries))
	for _, e := range entries {
		anyEntries = append(anyEntries, e)
	}

	_, err := v.Client.SAdd(ctx, keyName, anyEntries...).Result()
	if err != nil {
		return fmt.Errorf("SAdd: %w", err)
	}

	return nil
}

// SortedSetLen returns the number of members in a sorted set.
func (v *V8) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.ZCard(ctx, keyName).Result()
}

// SortedSetScan iterates sorted-set members with SCAN.
func (v *V8) SortedSetScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return v.Client.ZScan(ctx, keyName, cursor, match, count).Result()
}

// SortedSetAdd batch-adds members to a sorted set using a pipeline.
func (v *V8) SortedSetAdd(ctx context.Context, keyName string, entries []map[any]float64) error {
	pipe := v.Client.Pipeline()
	for _, entry := range entries {
		for member, score := range entry {
			pipe.ZAdd(ctx, keyName, &redis.Z{
				Score:  score,
				Member: member,
			})
		}
	}

	_, err := pipe.Exec(ctx)
	_ = pipe.Close()

	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// KeyScan iterates keys matching a pattern with SCAN.
func (v *V8) KeyScan(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	return v.Client.Scan(ctx, cursor, pattern, count).Result()
}
