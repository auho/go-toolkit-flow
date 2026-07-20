package redis

import (
	"fmt"

	testredis "github.com/auho/go-toolkit-testutil/redis"
	"github.com/go-redis/redis/v8"
)

// GetOptions returns Redis connection options.
// Callers must call testutil.LoadEnv() first to load .env.test, otherwise env var read will fatal.
func GetOptions() (redis.Options, error) {
	config, err := testredis.LoadConfig()
	if err != nil {
		return redis.Options{}, fmt.Errorf("redistest.LoadConfig: %w", err)
	}

	return redis.Options{
		Network:  "tcp",
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	}, nil
}
