package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

var _ Format[string] = (*scanKeyFormat)(nil)

type scanKeyFormat struct{}

func NewScanFormat() Format[string] {
	return &scanKeyFormat{}
}

func (f *scanKeyFormat) ScanByRange(ctx context.Context, d dialect.Dialect, pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	return d.KeyScan(ctx, pattern, cursor, count)
}

func (f *scanKeyFormat) FetchLen(ctx context.Context, d dialect.Dialect, _ string) (int64, error) {
	// SCAN has no pre-known length
	return 0, nil
}

func (f *scanKeyFormat) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
