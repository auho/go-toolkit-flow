package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*hashesFormat)(nil)

type hashesFormat struct{}

func NewHashesFormat() Format[storage.MapEntry] {
	return &hashesFormat{}
}

func (f *hashesFormat) Write(ctx context.Context, d dialect.Dialect, keyName string, items storage.MapEntries) error {
	return d.HashMSet(ctx, keyName, items)
}

func (f *hashesFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.HashLen(ctx, keyName)
}

func (f *hashesFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
