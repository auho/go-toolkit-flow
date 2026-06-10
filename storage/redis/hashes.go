package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

var _ KeyOperator = (*Hashes)(nil)

type Hashes struct {
	key
}

func (h Hashes) Type() KeyType {
	return KeyTypeHashes
}

func (h Hashes) Len(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.HLen(ctx, key).Result()
}
