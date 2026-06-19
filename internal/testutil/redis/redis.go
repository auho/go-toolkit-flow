package redis

import "github.com/go-redis/redis/v8"

var Options = redis.Options{
	Network:  "tcp",
	Addr:     "localhost:6379",
	Password: "password",
}
