package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[string] = (*listsFormat)(nil)

type listsFormat struct{}

func NewListsFormat() Format[string] {
	return &listsFormat{}
}

func (f *listsFormat) Write(ctx context.Context, d dialect.Dialect, keyName string, items []string) error {
	return d.ListPush(ctx, keyName, items)
}

func (f *listsFormat) FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error) {
	return d.ListLen(ctx, keyName)
}

func (f *listsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)

	return newItems
}
