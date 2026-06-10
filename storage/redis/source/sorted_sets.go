package source

import (
	"context"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/tool"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyScanner[storage.MapOfStringsEntry] = (*sortedSetsKey)(nil)

type sortedSetsKey struct {
	storage.Storage
	amount int64
}

func NewSortedSets(config Config) (*key[storage.MapOfStringsEntry], error) {
	return newKey[storage.MapOfStringsEntry](config, &sortedSetsKey{})
}

func (s *sortedSetsKey) keyType() redis.KeyType {
	return redis.KeyTypeSortedSets
}

func (s *sortedSetsKey) len(c *client.Redis, key string) (int64, error) {
	return c.ZCard(context.Background(), key).Result()
}

func (s *sortedSetsKey) scan(entriesChan chan<- storage.MapOfStringsEntries, c *client.Redis, key string, amount int64, count int64) {
	scanKeyValues(amount, count, &s.amount, entriesChan,
		func(cursor uint64) ([]string, uint64, error) {
			return c.ZScan(context.Background(), key, cursor, "", count).Result()
		},
		"zscan",
		parseMapOfStringsEntries,
	)
}

func (s *sortedSetsKey) duplicate(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}

func (s *sortedSetsKey) stateAmount() int64 {
	return atomic.LoadInt64(&s.amount)
}
