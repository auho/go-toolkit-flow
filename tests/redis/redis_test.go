package redis

import "github.com/go-redis/redis/v8"

var Options = redis.Options{
	Network:  "tcp",
	Addr:     "127.0.0.1:6379",
	Password: "password",
}
