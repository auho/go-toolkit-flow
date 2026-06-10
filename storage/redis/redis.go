package redis

import (
	"context"

	"github.com/auho/go-toolkit/redis/client"
)

const (
	KeyTypeLists      KeyType = "lists"
	KeyTypeSets       KeyType = "sets"
	KeyTypeSortedSets KeyType = "sortedSets"
	KeyTypeHashes     KeyType = "hashes"
)

type KeyType string

type ClientProvider interface {
	GetClient() *client.Redis
}

type KeyOperator interface {
	Type() KeyType
	Len(ctx context.Context, c *client.Redis, key string) (int64, error)
	Truncate(ctx context.Context, c *client.Redis, key string) (int64, error)
}

type key struct {
}

func (k *key) Truncate(ctx context.Context, c *client.Redis, key string) (int64, error) {
	return c.Del(ctx, key).Result()
}
