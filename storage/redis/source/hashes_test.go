package source

import (
	"context"
	"strconv"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	goredis "github.com/go-redis/redis/v8"
)

var _hashesKey = "test:source:hashes"

func _buildHashesData(t *testing.T) {
	ctx := context.Background()
	c := goredis.NewClient(&_redisOptions)
	c.Del(ctx, _hashesKey)

	amount := _randAmount()
	pipe := c.Pipeline()
	for i := 0; i < amount; i++ {
		pipe.HSet(ctx, _hashesKey, strconv.Itoa(i), i)
		if i%99 == 0 {
			_, err := pipe.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			pipe = c.Pipeline()
		}
	}
}

func TestNewHashes(t *testing.T) {
	_buildHashesData(t)

	c := _newRedisClient()
	_testKey[storage.MapOfStringsEntry](
		t,
		_hashesKey,
		NewHashesWithGoRedisV8,
		c,
		func(ctx context.Context, c *goredis.Client) (int64, error) {
			return c.HLen(ctx, _hashesKey).Result()
		},
	)
}
