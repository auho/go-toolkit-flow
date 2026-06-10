package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

var _ Keyer = (*Sets)(nil)

type Sets struct {
	key
}

func (l *Sets) Type() KeyType {
	return KeyTypeList
}

func (l *Sets) Len(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.SCard(ctx, key).Result()
}
