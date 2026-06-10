package destination

import (
	"context"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyer[string] = (*sets)(nil)

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

func (h *sets) accept(itemsChan <-chan []string, c *client.Redis, key string, pageSize int64) {
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

			_, err := c.SAdd(ctx, key, entriesAny...).Result()
			if err != nil {
				panic(err)
			}
		}

		atomic.AddInt64(&h.amount, int64(l))
	}
}
