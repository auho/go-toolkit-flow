package destination

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
	redis2 "github.com/go-redis/redis/v8"
)

var _ keyWriter[storage.MapEntry] = (*hashes)(nil)

type hashes struct {
	redis.Hashes
	amount int64
}

func (h *hashes) stateAmount() int64 {
	return h.amount
}

func NewHashes(config Config) (*key[storage.MapEntry], error) {
	return newKey[storage.MapEntry](config, &hashes{})
}

func (h *hashes) accept(itemsChan <-chan []storage.MapEntry, c *client.Redis, key string, pageSize int64) error {
	return acceptMapItems(itemsChan, c, key, pageSize, &h.amount, func(ctx context.Context, pipe redis2.Pipeliner, key string, entry storage.MapEntry) {
		for k, v := range entry {
			pipe.HMSet(ctx, key, k, v)
		}
	}, "hashes accept exec error")
}
