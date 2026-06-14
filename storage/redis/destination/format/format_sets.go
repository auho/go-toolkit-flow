package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct{}

func NewSetsFormat() Format[string] {
	return &setsFormat{}
}

func (f *setsFormat) Write(ctx context.Context, d dialect.Dialect, keyName string, items []string) error {
	return d.SetAdd(ctx, keyName, items)
}

func (f *setsFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.SetLen(ctx, keyName)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)

	return newItems
}
