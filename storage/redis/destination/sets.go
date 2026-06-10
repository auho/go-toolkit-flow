package destination

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyWriter[string] = (*sets)(nil)

type sets struct {
	redis.Sets
	amount int64
}

func (h *sets) stateAmount() int64 {
	return h.amount
}

func NewSets(config Config) (*key[string], error) {
	return newKey[string](config, &sets{})
}

func (h *sets) accept(itemsChan <-chan []string, c *client.Redis, key string, pageSize int64) error {
	return acceptStringItems(itemsChan, key, pageSize, &h.amount, func(ctx context.Context, key string, entries []any) error {
		_, err := c.SAdd(ctx, key, entries...).Result()
		return err
	}, "sets accept sadd error")
}
