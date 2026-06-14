package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[storage.ScoreMapEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct{}

func NewSortedSetsFormat() Format[storage.ScoreMapEntry] {
	return &sortedSetsFormat{}
}

func (f *sortedSetsFormat) Write(ctx context.Context, d dialect.Dialect, keyName string, items storage.ScoreMapEntries) error {
	return d.SortedSetAdd(ctx, keyName, items)
}

func (f *sortedSetsFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.SortedSetLen(ctx, keyName)
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
