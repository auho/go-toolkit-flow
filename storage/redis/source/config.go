package source

import (
	"github.com/go-redis/redis/v8"
)

type Config struct {
	Concurrency int
	Amount      int64 // 取的总数量，不是精确值
	PageSize    int64
	Key         string
	Options     *redis.Options
}
