package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

var _ KeyOperator = (*SortedSets)(nil)

type SortedSets struct {
	key
}

func (l *SortedSets) Type() KeyType {
	return KeyTypeSortedSets
}

func (l *SortedSets) Len(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.ZCard(ctx, key).Result()
}
