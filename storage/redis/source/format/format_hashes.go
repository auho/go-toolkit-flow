package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/client"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.StringMapEntry] = (*hashesFormat)(nil)

type hashesFormat struct {
	keyFormat
}

func NewHashesFormat(key string) Format[storage.StringMapEntry] {
	return &hashesFormat{keyFormat{key: key}}
}

func (f *hashesFormat) Type() string {
	return client.KeyTypeHash
}

func (f *hashesFormat) ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) (storage.StringMapEntries, uint64, error) {
	return d.HashScan(ctx, f.key, cursor, count)
}

func (f *hashesFormat) FetchLen(ctx context.Context, d dialect.Dialect) (int64, error) {
	return d.HashLen(ctx, f.key)
}

func (f *hashesFormat) Copy(items storage.StringMapEntries) storage.StringMapEntries {
	return tool.CopySliceMap[string](items)
}
