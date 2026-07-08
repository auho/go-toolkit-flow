package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage/redis/client"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
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

func (f *listsFormat) Write(ctx context.Context, d dialect.Dialect, items []string) error {
	return d.ListPush(ctx, f.key, items)
}

func (f *listsFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.ListLen(ctx, f.key)
}

func (f *listsFormat) Copy(items []string) []string {
	return tool.CopySliceString(items)
}
