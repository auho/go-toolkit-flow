package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage/redis/client"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct {
	keyFormat
}

func NewSetsFormat(key string) Format[string] {
	return &setsFormat{keyFormat{key: key}}
}

func (f *setsFormat) Type() string {
	return client.KeyTypeSet
}

func (f *setsFormat) Write(ctx context.Context, d dialect.Dialect, items []string) error {
	return d.SetAdd(ctx, f.key, items)
}

func (f *setsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.SetLen(ctx, f.key)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)

	return newItems
}
