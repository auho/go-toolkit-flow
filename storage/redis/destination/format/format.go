package format

import (
	"context"
	"errors"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
)

type Format[E storage.Entry] interface {
	Type() string
	Key() string
	Check() error
	FetchLen(ctx context.Context, dialect dialect.Dialect) (int64, error)
	Write(ctx context.Context, dialect dialect.Dialect, items []E) error
	Copy(items []E) []E
}

type keyFormat struct {
	key string
}

var ErrKeyEmpty = errors.New("key is empty")

func (f *keyFormat) Check() error {
	if f.key == "" {
		return ErrKeyEmpty
	}

	return nil
}

func (f *keyFormat) Key() string {
	return f.key
}
