package format

import (
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct{}

func NewSetsFormat() Format[string] {
	return &setsFormat{}
}

func (f *setsFormat) ScanByRange(d dialect.Dialect, keyName string, cursor int64, count int64) ([]string, int64, error) {
	items, newCursor, err := d.SetScan(keyName, uint64(cursor), count)
	return items, int64(newCursor), err
}

func (f *setsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeSets)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
