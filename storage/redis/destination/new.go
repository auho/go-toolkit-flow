package destination

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	goredisv8 "github.com/auho/go-toolkit-flow/storage/redis/destination/dialect/goredisv8"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/format"
	goredis "github.com/go-redis/redis/v8"
)

func NewHashesWithGoRedisV8(config Config, client *goredis.Client) (*key[storage.MapEntry], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[storage.MapEntry](config, d, format.NewHashesFormat())
}

func NewListsWithGoRedisV8(config Config, client *goredis.Client) (*key[string], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[string](config, d, format.NewListsFormat())
}

func NewSetsWithGoRedisV8(config Config, client *goredis.Client) (*key[string], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[string](config, d, format.NewSetsFormat())
}

func NewSortedSetsWithGoRedisV8(config Config, client *goredis.Client) (*key[storage.ScoreMapEntry], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[storage.ScoreMapEntry](config, d, format.NewSortedSetsFormat())
}
