package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*hashesFormat)(nil)

type hashesFormat struct{}

func NewHashesFormat() Format[storage.MapEntry] {
	return &hashesFormat{}
}

func (f *hashesFormat) Write(d dialect.Dialect, keyName string, items storage.MapEntries) error {
	return d.HashSet(keyName, items)
}

func (f *hashesFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeHashes)
}

func (f *hashesFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
