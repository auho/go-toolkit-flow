package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/client"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct {
	keyFormat
}

func NewSortedSetsFormat(key string) Format[storage.MapOfStringsEntry] {
	return &sortedSetsFormat{keyFormat{key: key}}
}

func (f *sortedSetsFormat) Type() string {
	return client.KeyTypeSortedSet
}

func (f *sortedSetsFormat) ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error) {
	return d.SortedSetScan(ctx, f.key, cursor, count)
}

func (f *sortedSetsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.SortedSetLen(ctx, f.key)
}

func (f *sortedSetsFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}
