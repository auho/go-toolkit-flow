package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct{}

func NewSortedSetsFormat() Format[storage.MapOfStringsEntry] {
	return &sortedSetsFormat{}
}

func (f *sortedSetsFormat) ScanByRange(d dialect.Dialect, keyName string, cursor int64, count int64) (storage.MapOfStringsEntries, int64, error) {
	entries, newCursor, err := d.SortedSetScan(keyName, uint64(cursor), count)
	return entries, int64(newCursor), err
}

func (f *sortedSetsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeSortedSets)
}

func (f *sortedSetsFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}
