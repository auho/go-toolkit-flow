package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*hashesFormat)(nil)

type hashesFormat struct{}

func NewHashesFormat() Format[storage.MapOfStringsEntry] {
	return &hashesFormat{}
}

func (f *hashesFormat) ScanByRange(ctx context.Context, d dialect.Dialect, keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	return d.HashScan(ctx, keyName, cursor, count)
}

func (f *hashesFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.HashLen(ctx, keyName)
}

func (f *hashesFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}
