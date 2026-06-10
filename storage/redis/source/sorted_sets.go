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
	var err error
	var items []string
	cursor := uint64(0)
	for {
		items, cursor, err = c.ZScan(context.Background(), key, cursor, "", count).Result()
		if err != nil {
			panic(fmt.Sprintf("zscan: %v", err))
		}

		entries := make(storage.MapOfStringsEntries, 0, len(items)/2)

		for i := 0; i < len(items)-1; i += 2 {
			entries = append(entries, storage.MapOfStringsEntry{items[i]: items[i+1]})
		}

		s.amount = atomic.AddInt64(&s.amount, int64(len(entries)))
		entriesChan <- entries

		if cursor == 0 {
			break
		}

		if atomic.LoadInt64(&s.amount) >= amount {
			break
		}
	}
}

func (s *sortedSetsKey) duplicate(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}

func (s *sortedSetsKey) stateAmount() int64 {
	return atomic.LoadInt64(&s.amount)
}
