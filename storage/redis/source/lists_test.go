package source

import (
	"context"
	"testing"

	"github.com/auho/go-toolkit/redis/client"
	"github.com/go-redis/redis/v8"
)

var _listsKey = "test:source:lists"

func _buildListsData(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&_redisOptions)
	c.Del(ctx, _listsKey)

	amount := _randAmount()
	pipe := c.Pipeline()
	for i := 0; i < amount; i++ {
		pipe.LPush(ctx, _listsKey, i)
		if i%99 == 0 {
			_, err := pipe.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			pipe = c.Pipeline()
		}
	}
}

func TestNewLists(t *testing.T) {
	_buildListsData(t)

	_testKey[string](
		t,
		_listsKey,
		NewLists,
		func(ctx context.Context, c *client.Redis) (int64, error) {
			return c.LLen(ctx, _listsKey).Result()
		})
}
