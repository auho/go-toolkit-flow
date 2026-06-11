package source

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyScanner[string] = (*setsKey)(nil)

type setsKey struct {
	storage.Storage
	amount int64
}

func NewSets(config Config) (*key[string], error) {
	return newKey[string](config, &setsKey{})
}

func (s *setsKey) keyType() redis.KeyType {
	return redis.KeyTypeSets
}

func (s *setsKey) len(c *client.Redis, key string) (int64, error) {
	return c.SCard(context.Background(), key).Result()
}

func (s *setsKey) scan(entriesChan chan<- []string, c *client.Redis, key string, amount int64, count int64) {
	var err error
	var items []string
	cursor := uint64(0)

	for {
		items, cursor, err = c.SScan(context.Background(), key, cursor, "", count).Result()
		if err != nil {
			panic(fmt.Sprintf("sscan: %v", err))
		}

		if len(items) > 0 {
			atomic.AddInt64(&s.amount, int64(len(items)))
			entriesChan <- items
		}

		if cursor == 0 {
			break
		}

		if atomic.LoadInt64(&s.amount) >= amount {
			break
		}
	}
}

func (s *setsKey) duplicate(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)

	return newItems
}

func (s *setsKey) stateAmount() int64 {
	return atomic.LoadInt64(&s.amount)
}
