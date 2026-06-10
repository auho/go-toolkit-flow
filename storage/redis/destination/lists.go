package destination

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyer[string] = (*lists)(nil)

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
	ctx := context.Background()
	for items := range itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(pageSize) {
			end := i + int(pageSize)
			if end > l {
				end = l
			}

			entries := items[i:end]

			entriesAny := make([]any, 0, end-i)
			for _, entry := range entries {
				entriesAny = append(entriesAny, entry)
			}

			_, err := c.LPush(ctx, key, entriesAny...).Result()
			if err != nil {
				return fmt.Errorf("lists accept lpush error; %w", err)
			}
		}

		atomic.AddInt64(&h.amount, int64(l))
	}

	return nil
}
