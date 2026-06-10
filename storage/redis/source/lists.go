package source

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ keyScanner[string] = (*listsKey)(nil)

type listsKey struct {
	storage.Storage
	amount int64
}

func NewLists(config Config) (*key[string], error) {
	return newKey[string](config, &listsKey{})
}

func (l *listsKey) keyType() redis.KeyType {
	return redis.KeyTypeList
}

func (l *listsKey) len(c *client.Redis, key string) (int64, error) {
	return c.LLen(context.Background(), key).Result()
}

func (l *listsKey) scan(entriesChan chan<- []string, c *client.Redis, key string, amount int64, pageSize int64) {
	start := int64(0)
	stop := start + pageSize - 1

	for {
		items, err := c.LRange(context.Background(), key, start, stop).Result()
		if err != nil {
			panic(fmt.Sprintf("lrange: %v", err))
		}

		if len(items) <= 0 {
			break
		}

		l.amount += int64(len(items))
		entriesChan <- items

		start = stop + 1
		stop = start + pageSize - 1

		if start >= amount {
			break
		}
	}
}

func (l *listsKey) duplicate(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)

	return newItems
}

func (l *listsKey) stateAmount() int64 {
	return l.amount
}
