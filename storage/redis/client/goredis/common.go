package goredis

import "context"

// SourceClient is the interface for Redis read/scan operations used by the
// source dialect. Both V8 and V9 wrappers satisfy this interface.
type SourceClient interface {
	DB() int
	Close() error

	HashLen(ctx context.Context, keyName string) (int64, error)
	HashScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error)

	ListLen(ctx context.Context, keyName string) (int64, error)
	ListRange(ctx context.Context, keyName string, start, stop int64) ([]string, error)

	SetLen(ctx context.Context, keyName string) (int64, error)
	SetScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error)

	SortedSetLen(ctx context.Context, keyName string) (int64, error)
	SortedSetScan(ctx context.Context, keyName string, cursor uint64, match string, count int64) ([]string, uint64, error)

	KeyScan(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error)
}

// DestClient is the interface for Redis write operations used by the
// destination dialect. Both V8 and V9 wrappers satisfy this interface.
// Pipeline-based batch operations (HashMSet, SortedSetAdd) are handled
// internally by each wrapper, encapsulating version-specific differences
// (pipe.Close in v8, redis.Z pointer vs value).
type DestClient interface {
	DB() int
	Close() error

	Truncate(ctx context.Context, keyName string) (int64, error)

	HashLen(ctx context.Context, keyName string) (int64, error)
	HashMSet(ctx context.Context, keyName string, entries []map[string]any) error

	ListLen(ctx context.Context, keyName string) (int64, error)
	ListPush(ctx context.Context, keyName string, entries []string) error

	SetLen(ctx context.Context, keyName string) (int64, error)
	SetAdd(ctx context.Context, keyName string, entries []string) error

	SortedSetLen(ctx context.Context, keyName string) (int64, error)
	SortedSetAdd(ctx context.Context, keyName string, entries []map[any]float64) error
}

// Compile-time interface checks.
var (
	_ SourceClient = (*V8)(nil)
	_ SourceClient = (*V9)(nil)
	_ DestClient   = (*V8)(nil)
	_ DestClient   = (*V9)(nil)
)

// flattenMapEntry converts a map[string]any to a flat slice of alternating
// keys and values, suitable for Redis HMSet.
func flattenMapEntry(entry map[string]any) []any {
	flat := make([]any, 0, len(entry)*2)
	for k, v := range entry {
		flat = append(flat, k, v)
	}
	return flat
}
