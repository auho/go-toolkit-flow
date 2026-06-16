package format

import (
	"context"
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

type Format[E storage.Entry] interface {
	Type() string
	Key() string
	Check() error
	ScanByRange(ctx context.Context, d dialect.Dialect, cursor uint64, count int64) ([]E, uint64, error)
	FetchLen(ctx context.Context, d dialect.Dialect) (int64, error)
	Copy(items []E) []E
}

type keyFormat struct {
	key string
}

func (f *keyFormat) Check() error {
	if f.key == "" {
		return errors.New("key is empty")
	}

	return nil
}

func (f *keyFormat) Key() string {
	return f.key
}
