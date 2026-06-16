package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/client"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*listsFormat)(nil)

type listsFormat struct {
	keyFormat
}

func NewListsFormat(key string) Format[string] {
	return &listsFormat{keyFormat{key: key}}
}

func (f *listsFormat) Type() string {
	return client.KeyTypeList
}

func (f *listsFormat) ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) ([]string, uint64, error) {
	items, err := d.ListRange(ctx, f.key, int64(cursor), int64(cursor)+count-1)
	if err != nil {
		return nil, 0, err
	}

	// For lists, cursor is offset-based.
	// When no items are returned, signal completion with cursor=0.
	if len(items) == 0 {
		return items, 0, nil
	}

	// Next cursor = current offset + items fetched
	return items, cursor + uint64(len(items)), nil
}

func (f *listsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.ListLen(ctx, f.key)
}

func (f *listsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
