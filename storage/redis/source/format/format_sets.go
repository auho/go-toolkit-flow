package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct{}

func NewSetsFormat() Format[string] {
	return &setsFormat{}
}

func (f *setsFormat) ScanByRange(ctx context.Context, d dialect.Dialect, keyName string, cursor uint64, count int64) ([]string, uint64, error) {
	return d.SetScan(ctx, keyName, cursor, count)
}

func (f *setsFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.SetLen(ctx, keyName)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
