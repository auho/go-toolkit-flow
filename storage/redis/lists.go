package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

var _ Keyer = (*Lists)(nil)

type Lists struct {
	key
}

func (l *Lists) Type() KeyType {
	return KeyTypeList
}

func (l *Lists) Len(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.LLen(ctx, key).Result()
}
