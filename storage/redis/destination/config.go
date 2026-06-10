package destination

import (
	"github.com/go-redis/redis/v8"
)

type Config struct {
	IsTruncate  bool
	Concurrency int
	PageSize    int64
	Key         string
	Options     *redis.Options
}
