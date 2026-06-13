package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect/goredisv8"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
	goredis "github.com/go-redis/redis/v8"
)

func NewHashesWithGoRedisV8(config Config, client *goredis.Client) (*key[storage.MapOfStringsEntry], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[storage.MapOfStringsEntry](config, d, format.NewHashesFormat())
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

func NewSortedSetsWithGoRedisV8(config Config, client *goredis.Client) (*key[storage.MapOfStringsEntry], error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newKey[storage.MapOfStringsEntry](config, d, format.NewSortedSetsFormat())
}

func NewScanWithGoRedisV8(config Config, client *goredis.Client) (*scanKey, error) {
	d, err := goredisv8.NewDialectGoRedisV8(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create dialect: %w", err)
	}
	return newScanKey(config, d, format.NewScanFormat())
}
