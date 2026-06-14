package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct{}

func NewSortedSetsFormat() Format[storage.MapOfStringsEntry] {
	return &sortedSetsFormat{}
}

func (f *sortedSetsFormat) ScanByRange(ctx context.Context, d dialect.Dialect, keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	return d.SortedSetScan(ctx, keyName, cursor, count)
}

func (f *sortedSetsFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.SortedSetLen(ctx, keyName)
}

func (f *sortedSetsFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}
