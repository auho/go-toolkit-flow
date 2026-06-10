package source

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/tool"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyer[storage.MapOfStringsEntry] = (*hashesKey)(nil)

type hashesKey struct {
	storage.Storage
	amount int64
}

func NewHashes(config Config) (*key[storage.MapOfStringsEntry], error) {
	return newKey[storage.MapOfStringsEntry](config, &hashesKey{})
}

func (h *hashesKey) keyType() redis.KeyType {
	return redis.KeyTypeHash
}

func (h *hashesKey) len(c *client.Redis, key string) (int64, error) {
	return c.HLen(context.Background(), key).Result()
}

func (h *hashesKey) scan(entriesChan chan<- storage.MapOfStringsEntries, c *client.Redis, key string, amount int64, count int64) {
	var err error
	var items []string
	cursor := uint64(0)

	for {
		items, cursor, err = c.HScan(context.Background(), key, cursor, "", count).Result()
		if err != nil {
			h.LogFatal(err)
		}

		entries := make(storage.MapOfStringsEntries, 0, len(items)/2)

		for i := 0; i < len(items)/2; i++ {
			entries = append(entries, storage.MapOfStringsEntry{items[i]: items[i+1]})
		}

		h.amount += int64(len(entries))
		entriesChan <- entries

		if cursor == 0 {
			break
		}

		if h.amount >= amount {
			break
		}
	}
}

func (h *hashesKey) duplicate(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[tool.StringEntry](items)
}

func (h *hashesKey) stateAmount() int64 {
	return h.amount
}
