package destination

import (
	"context"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
	redis2 "github.com/go-redis/redis/v8"
)

var _ keyer[storage.ScoreMap] = (*sortedSets)(nil)

type sortedSets struct {
	redis.SortedSets
	amount int64
}

func (h *sortedSets) stateAmount() int64 {
	return h.amount
}

func NewSortedSets(config Config) (*key[storage.ScoreMap], error) {
	return newKey[storage.ScoreMap](config, &sortedSets{})
}

func (h *sortedSets) accept(itemsChan <-chan []storage.ScoreMap, c *client.Redis, key string, pageSize int64) {
	ctx := context.Background()
	pipe := c.Pipeline()

	for items := range itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(pageSize) {
			end := i + int(pageSize)
			if end > l {
				end = l
			}

			entries := items[i:end]
			for _, entry := range entries {
				for k, v := range entry {
					pipe.ZAdd(ctx, key, &redis2.Z{
						Score:  v,
						Member: k,
					})
				}
			}

			_, err := pipe.Exec(ctx)
			if err != nil {
				panic(err)
			}
		}

		atomic.AddInt64(&h.amount, int64(l))
	}

	_ = pipe.Close()
}
