package format

import (
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*listsFormat)(nil)

type listsFormat struct{}

func NewListsFormat() Format[string] {
	return &listsFormat{}
}

func (f *listsFormat) ScanByRange(d dialect.Dialect, keyName string, cursor int64, count int64) ([]string, int64, error) {
	items, err := d.ListRange(keyName, cursor, cursor+count-1)
	if err != nil {
		return nil, 0, err
	}
	// For lists, cursor is offset-based.
	// When no items are returned, signal completion with cursor=0.
	if len(items) == 0 {
		return items, 0, nil
	}
	// Next cursor = current offset + items fetched
	return items, cursor + int64(len(items)), nil
}

func (f *listsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeLists)
}

func (f *listsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
