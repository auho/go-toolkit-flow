package dialect

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

type Dialect interface {
	DB() int
	Close() error
	Truncate(ctx context.Context, keyName string) (int64, error)

	HashLen(ctx context.Context, keyName string) (int64, error)
	HashMSet(ctx context.Context, keyName string, entries storage.MapEntries) error

	ListLen(ctx context.Context, keyName string) (int64, error)
	ListPush(ctx context.Context, keyName string, entries []string) error

	SetLen(ctx context.Context, keyName string) (int64, error)
	SetAdd(ctx context.Context, keyName string, entries []string) error

	SortedSetLen(ctx context.Context, keyName string) (int64, error)
	SortedSetAdd(ctx context.Context, keyName string, entries storage.ScoreMapEntries) error
}
