package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

const KeyTypeList KeyType = "lists"
const KeyTypeSet KeyType = "sets"
const KeyTypeSortedSets KeyType = "sortedSets"
const KeyTypeHash KeyType = "hashes"

type KeyType string

type Rediser interface {
	GetClient() *client.Redis
}

type Keyer interface {
	Type() KeyType
	Len(ctx context.Context, c *client.Redis, key string) (int64, error)
	Truncate(ctx context.Context, c *client.Redis, key string) (int64, error)
}

type key struct {
}

func (k *key) Truncate(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.Del(ctx, key).Result()
}
