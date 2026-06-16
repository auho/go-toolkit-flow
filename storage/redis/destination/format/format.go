package format

import (
	"context"
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
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

func (f *keyFormat) Check() error {
	if f.key == "" {
		return errors.New("key is empty")
	}

	return nil
}

func (f *keyFormat) Key() string {
	return f.key
}
