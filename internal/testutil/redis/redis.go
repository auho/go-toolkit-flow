package redis

import (
	"log"
	"os"

	"github.com/go-redis/redis/v8"
)

// GetOptions returns Redis connection options.
// Callers must call testutil.LoadEnv() first to load .env.test, otherwise env var read will fatal.
func GetOptions() redis.Options {
	return redis.Options{
		Network:  "tcp",
		Addr:     mustGetEnv("TEST_REDIS_ADDR"),
		Password: mustGetEnv("TEST_REDIS_PASSWORD"),
	}
}

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env var %s not set, please configure Redis connection info", key)
	}
	return v
}
