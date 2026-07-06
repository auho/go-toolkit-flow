// V9 wraps a go-redis v9 client. See doc.go for the multi-version design.
package goredis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// V9 wraps a go-redis v9 client.
type V9 struct {
	// Client is the underlying go-redis client.
	Client *redis.Client
}

// DB returns the configured database index.
func (v *V9) DB() int {
	return v.Client.Options().DB
}

// Close closes the Redis connection.
func (v *V9) Close() error {
	return v.Client.Close()
}

// Truncate deletes all entries of the given key.
func (v *V9) Truncate(ctx context.Context, keyName string) (int64, error) {
	return v.Client.Del(ctx, keyName).Result()
}

// HashLen returns the number of fields in a hash.
func (v *V9) HashLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.HLen(ctx, keyName).Result()
}

// ListLen returns the length of a list.
func (v *V9) ListLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.LLen(ctx, keyName).Result()
}

// SetLen returns the number of members in a set.
func (v *V9) SetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.SCard(ctx, keyName).Result()
}

// SortedSetLen returns the number of members in a sorted set.
func (v *V9) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.ZCard(ctx, keyName).Result()
}
