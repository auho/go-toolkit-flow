package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/client"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*hashesFormat)(nil)

type hashesFormat struct {
	keyFormat
}

func NewHashesFormat(key string) Format[storage.MapEntry] {
	return &hashesFormat{keyFormat{key: key}}
}

func (f *hashesFormat) Type() string {
	return client.KeyTypeHash
}

func (f *hashesFormat) Write(ctx context.Context, d dialect.Dialect, items storage.MapEntries) error {
	return d.HashMSet(ctx, f.key, items)
}

func (f *hashesFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.HashLen(ctx, f.key)
}

func (f *hashesFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
