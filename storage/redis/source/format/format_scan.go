package format

import (
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*scanFormat)(nil)

type scanFormat struct{}

func NewScanFormat() Format[string] {
	return &scanFormat{}
}

func (f *scanFormat) ScanByRange(d dialect.Dialect, pattern string, cursor int64, count int64) ([]string, int64, error) {
	keys, newCursor, err := d.KeyScan(pattern, uint64(cursor), count)
	return keys, int64(newCursor), err
}

func (f *scanFormat) FetchLen(d dialect.Dialect, _ string) (int64, error) {
	// SCAN has no pre-known length
	return 0, nil
}

func (f *scanFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
