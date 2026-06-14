package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

type Format[E storage.Entry] interface {
	FetchLen(ctx context.Context, dialect dialect.Dialect, keyName string) (int64, error)
	Write(ctx context.Context, dialect dialect.Dialect, keyName string, items []E) error
	Copy(items []E) []E
}
