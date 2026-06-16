package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/client"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct {
	keyFormat
}

func NewSetsFormat(key string) Format[string] {
	return &setsFormat{keyFormat: keyFormat{key: key}}
}

func (f *setsFormat) Type() string {
	return client.KeyTypeSet
}

func (f *setsFormat) ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) ([]string, uint64, error) {
	return d.SetScan(ctx, f.key, cursor, count)
}

func (f *setsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.SetLen(ctx, f.key)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
