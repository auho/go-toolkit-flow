package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

type Format[E storage.Entry] interface {
	ScanByRange(ctx context.Context, d dialect.Dialect, keyName string, cursor uint64, count int64) ([]E, uint64, error)
	FetchLen(ctx context.Context, d dialect.Dialect, keyName string) (int64, error)
	Copy(items []E) []E
}
