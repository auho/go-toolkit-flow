package destination

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyWriter[string] = (*lists)(nil)

type lists struct {
	redis.Lists
	amount int64
}

func (h *lists) stateAmount() int64 {
	return h.amount
}

func NewLists(config Config) (*key[string], error) {
	return newKey[string](config, &lists{})
}

func (h *lists) accept(itemsChan <-chan []string, c *client.Redis, key string, pageSize int64) error {
	return acceptStringItems(itemsChan, key, pageSize, &h.amount, func(ctx context.Context, key string, entries []any) error {
		_, err := c.LPush(ctx, key, entries...).Result()
		return err
	}, "lists accept lpush error")
}
