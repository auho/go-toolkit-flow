package destination

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/redis/client"
	"github.com/go-redis/redis/v8"
)

// acceptStringItems processes string items in pages, calling execFn for each page.
func acceptStringItems(
	itemsChan <-chan []string,
	key string,
	pageSize int64,
	amount *int64,
	execFn func(ctx context.Context, key string, entries []any) error,
	errPrefix string,
) error {
	ctx := context.Background()
	for items := range itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(pageSize) {
			end := i + int(pageSize)
			if end > l {
				end = l
			}

			entries := items[i:end]

			entriesAny := make([]any, 0, end-i)
			for _, entry := range entries {
				entriesAny = append(entriesAny, entry)
			}

			err := execFn(ctx, key, entriesAny)
			if err != nil {
				return fmt.Errorf("%s; %w", errPrefix, err)
			}
		}

		atomic.AddInt64(amount, int64(l))
	}

	return nil
}

// acceptMapItems processes map items in pages using a Redis pipeline.
func acceptMapItems[E storage.Entry](
	itemsChan <-chan []E,
	c *client.Redis,
	key string,
	pageSize int64,
	amount *int64,
	addEntry func(ctx context.Context, pipe redis.Pipeliner, key string, entry E),
	errPrefix string,
) error {
	ctx := context.Background()
	pipe := c.Pipeline()

	for items := range itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(pageSize) {
			end := i + int(pageSize)
			if end > l {
				end = l
			}

			entries := items[i:end]
			for _, entry := range entries {
				addEntry(ctx, pipe, key, entry)
			}

			_, err := pipe.Exec(ctx)
			if err != nil {
				return fmt.Errorf("%s; %w", errPrefix, err)
			}
		}

		atomic.AddInt64(amount, int64(l))
	}

	_ = pipe.Close()

	return nil
}
