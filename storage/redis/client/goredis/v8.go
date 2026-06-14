package goredis

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type V8 struct {
	Client *redis.Client
}

func (v *V8) DBName() string {
	return v.Client.Options().Addr
}

func (v *V8) Close() error {
	return v.Client.Close()
}

func (v *V8) Truncate(ctx context.Context, keyName string) (int64, error) {
	return v.Client.Del(ctx, keyName).Result()
}

func (v *V8) HashLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.HLen(ctx, keyName).Result()
}

func (v *V8) ListLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.LLen(ctx, keyName).Result()
}

func (v *V8) SetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.SCard(ctx, keyName).Result()
}

func (v *V8) SortedSetLen(ctx context.Context, keyName string) (int64, error) {
	return v.Client.ZCard(ctx, keyName).Result()
}
