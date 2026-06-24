package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
)

var _ Format[string] = (*scanKeyFormat)(nil)

type scanKeyFormat struct {
	key string
}

func NewScanFormat(key string) Format[string] {
	return &scanKeyFormat{key: key}
}

func (f *scanKeyFormat) Type() string {
	return "scan"
}

func (f *scanKeyFormat) Key() string {
	return ""
}

func (f *scanKeyFormat) Check() error {
	return nil
}

func (f *scanKeyFormat) ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) ([]string, uint64, error) {
	return d.KeyScan(ctx, f.key, cursor, count)
}

func (f *scanKeyFormat) FetchLen(_ context.Context, _ dialect.Dialect) (int64, error) {
	// SCAN has no pre-known length
	return -1, nil
}

func (f *scanKeyFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	_ = copy(newItems, items)
	return newItems
}
