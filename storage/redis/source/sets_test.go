package source

import (
	"context"
	"testing"

	goredis "github.com/go-redis/redis/v8"
)

var _setsKey = "test:source:sets"

func _buildSetsData(t *testing.T) {
	ctx := context.Background()
	c := goredis.NewClient(&_redisOptions)
	c.Del(ctx, _setsKey)

	amount := _randAmount()
	pipe := c.Pipeline()
	for i := 0; i < amount; i++ {
		pipe.SAdd(ctx, _setsKey, i)
		if i%99 == 0 {
			_, err := pipe.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			pipe = c.Pipeline()
		}
	}
}

func TestNewSets(t *testing.T) {
	_buildSetsData(t)

	c := _newRedisClient()
	_testKey[string](
		t,
		_setsKey,
		NewSetsWithGoRedisV8,
		c,
		func(ctx context.Context, c *goredis.Client) (int64, error) {
			return c.SCard(ctx, _setsKey).Result()
		},
	)
}
