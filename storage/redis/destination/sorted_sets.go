package destination

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
	redis2 "github.com/go-redis/redis/v8"
)

var _ keyWriter[storage.ScoreMapEntry] = (*sortedSets)(nil)

type sortedSets struct {
	redis.SortedSets
	amount int64
}

func (h *sortedSets) stateAmount() int64 {
	return h.amount
}

func NewSortedSets(config Config) (*key[storage.ScoreMapEntry], error) {
	return newKey[storage.ScoreMapEntry](config, &sortedSets{})
}

func (h *sortedSets) accept(itemsChan <-chan []storage.ScoreMapEntry, c *client.Redis, key string, pageSize int64) error {
	return acceptMapItems(itemsChan, c, key, pageSize, &h.amount, func(ctx context.Context, pipe redis2.Pipeliner, key string, entry storage.ScoreMapEntry) {
		for k, v := range entry {
			pipe.ZAdd(ctx, key, &redis2.Z{
				Score:  v,
				Member: k,
			})
		}
	}, "sorted sets accept exec error")
}
