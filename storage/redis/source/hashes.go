package source

import (
	"context"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/tool"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyScanner[storage.MapOfStringsEntry] = (*hashesKey)(nil)

type hashesKey struct {
	storage.Storage
	amount int64
}

func NewHashes(config Config) (*key[storage.MapOfStringsEntry], error) {
	return newKey[storage.MapOfStringsEntry](config, &hashesKey{})
}

func (h *hashesKey) keyType() redis.KeyType {
	return redis.KeyTypeHashes
}

func (h *hashesKey) len(c *client.Redis, key string) (int64, error) {
	return c.HLen(context.Background(), key).Result()
}

func (h *hashesKey) scan(entriesChan chan<- storage.MapOfStringsEntries, c *client.Redis, key string, amount int64, count int64) {
	scanKeyValues(amount, count, &h.amount, entriesChan,
		func(cursor uint64) ([]string, uint64, error) {
			return c.HScan(context.Background(), key, cursor, "", count).Result()
		},
		"hscan",
		parseMapOfStringsEntries,
	)
}

func (h *hashesKey) duplicate(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}

func (h *hashesKey) stateAmount() int64 {
	return atomic.LoadInt64(&h.amount)
}
