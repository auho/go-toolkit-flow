package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/client"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
)

var _ Format[storage.ScoreMapEntry] = (*sortedSetsFormat)(nil)

type sortedSetsFormat struct {
	keyFormat
}

func NewSortedSetsFormat(key string) Format[storage.ScoreMapEntry] {
	return &sortedSetsFormat{keyFormat{key: key}}
}

func (f *sortedSetsFormat) Type() string {
	return client.KeyTypeSortedSet
}

func (f *sortedSetsFormat) Write(ctx context.Context, d dialect.Dialect, items storage.ScoreMapEntries) error {
	return d.SortedSetAdd(ctx, f.key, items)
}

func (f *sortedSetsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.SortedSetLen(ctx, f.key)
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
