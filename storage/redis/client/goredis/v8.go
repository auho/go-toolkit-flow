// Package goredis provides a Redis client wrapper for go-redis v8.
package goredis

import (
	"context"

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

// ListLen returns the length of a list.
func (v *V8) ListLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.LLen(ctx, keyName).Result()
}

// SetLen returns the number of members in a set.
func (v *V8) SetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.SCard(ctx, keyName).Result()
}

// SortedSetLen returns the number of members in a sorted set.
func (v *V8) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.ZCard(ctx, keyName).Result()
}
