package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*hashesFormat)(nil)

type hashesFormat struct{}

func NewHashesFormat() Format[storage.MapOfStringsEntry] {
	return &hashesFormat{}
}

func (f *hashesFormat) ScanByRange(d dialect.Dialect, keyName string, cursor int64, count int64) (storage.MapOfStringsEntries, int64, error) {
	entries, newCursor, err := d.HashScan(keyName, uint64(cursor), count)
	return entries, int64(newCursor), err
}

func (f *hashesFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeHashes)
}

func (f *hashesFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}
