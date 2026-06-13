package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[storage.ScoreMapEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct{}

func NewSortedSetsFormat() Format[storage.ScoreMapEntry] {
	return &sortedSetsFormat{}
}

func (f *sortedSetsFormat) Write(d dialect.Dialect, keyName string, items storage.ScoreMapEntries) error {
	return d.SortedSetAdd(keyName, items)
}

func (f *sortedSetsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeSortedSets)
}

func (f *sortedSetsFormat) Copy(items storage.ScoreMapEntries) storage.ScoreMapEntries {
	newItems := make(storage.ScoreMapEntries, 0, len(items))
	for _, item := range items {
		newItem := make(storage.ScoreMapEntry, len(item))
		for k, v := range item {
			newItem[k] = v
		}
		newItems = append(newItems, newItem)
	}
	return newItems
}
