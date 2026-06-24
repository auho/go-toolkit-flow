package dialect

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

type Dialect interface {
	DB() int
	Close() error

	HashLen(ctx context.Context, keyName string) (int64, error)
	HashScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.StringMapEntries, uint64, error)

	ListLen(ctx context.Context, keyName string) (int64, error)
	ListRange(ctx context.Context, keyName string, start, stop int64) ([]string, error)

	SetLen(ctx context.Context, keyName string) (int64, error)
	SetScan(ctx context.Context, keyName string, cursor uint64, count int64) ([]string, uint64, error)

	SortedSetLen(ctx context.Context, keyName string) (int64, error)
	SortedSetScan(ctx context.Context, keyName string, cursor uint64, count int64) (storage.StringMapEntries, uint64, error)

	KeyScan(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error)
}
