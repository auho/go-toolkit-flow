package destination

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/destination/format"
	"golang.org/x/sync/errgroup"
)

var _ storage.Destination[storage.MapEntry] = (*Bulk[storage.MapEntry])(nil)

// Bulk is a Redis destination that writes data in batch via pipeline.
// Destination types are named after their writing strategy (cf. Redis
// source's Iterator, which reads via SCAN-based iteration).
type Bulk[E storage.Entry] struct {
	dialect dialect.Dialect
	format  format.Format[E]
	config  BulkConfig

	isDone    atomic.Bool
	itemsChan chan []E
	state     *storage.Snapshot

	// Concurrency and error handling
	writeGroup *errgroup.Group
	writeCtx   context.Context
	writeErr   error
}

func newBulk[E storage.Entry](f format.Format[E], d dialect.Dialect, c BulkConfig) (*Bulk[E], error) {
	b := &Bulk[E]{
		dialect: d,
		format:  f,
		config:  c,
	}

	if err := b.init(); err != nil {
		return nil, err
	}

	err := b.format.Check()
	if err != nil {
		return nil, fmt.Errorf("format.Check: %w", err)
	}

	return b, nil
}

func (b *Bulk[E]) Prepare(ctx context.Context) error {
	b.state.MarkAsPrepare()

	if b.config.IsTruncate {
		_ctx, cancel := context.WithTimeout(context.Background(), b.config.TimeoutDuration)
		defer cancel()

		_, err := b.dialect.Truncate(_ctx, b.format.Key())
		if err != nil {
			return fmt.Errorf("dialect.Truncate: %w", err)
		}
	}

	b.itemsChan = make(chan []E, b.config.Concurrency)
	b.writeGroup, b.writeCtx = errgroup.WithContext(ctx)

	return nil
}

func (b *Bulk[E]) Accept() {
	b.state.MarkAsAccepted()
	b.state.DurationStart()

	for i := 0; i < b.config.Concurrency; i++ {
		b.writeGroup.Go(func() error {
			return b.write()
		})
	}
}

func (b *Bulk[E]) Receive(items []E) error {
	select {
	case <-b.writeCtx.Done():
		return fmt.Errorf("receive: writeCtx cancelled: %w", b.writeCtx.Err())
	case b.itemsChan <- items:
	}
	return nil
}

func (b *Bulk[E]) Done() {
	if !b.isDone.CompareAndSwap(false, true) {
		return
	}

	b.state.MarkAsDone()

	close(b.itemsChan)
}

func (b *Bulk[E]) Finish() error {
	b.writeErr = b.writeGroup.Wait()

	b.state.DurationStop()
	b.state.MarkAsFinished()

	return b.writeErr
}

func (b *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d; batch size:%d", b.title(), b.config.Concurrency, b.config.BatchSize)}
}

func (b *Bulk[E]) State() storage.State {
	return b.state
}

func (b *Bulk[E]) StateString() []string {
	return []string{b.state.Overview()}
}

func (b *Bulk[E]) title() string {
	return fmt.Sprintf("Destination redis[%s][%d:%s]", b.format.Key(), b.dialect.DB(), b.format.Type())
}

func (b *Bulk[E]) FetchLen() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.TimeoutDuration)
	defer cancel()

	return b.format.FetchLen(ctx, b.dialect)
}

func (b *Bulk[E]) Close() error {
	return b.dialect.Close()
}

func (b *Bulk[E]) init() error {
	if err := b.config.Check(); err != nil {
		return fmt.Errorf("config.Check: %w", err)
	}

	b.state = storage.NewSnapshot()
	b.state.SetConcurrency(b.config.Concurrency)
	b.state.SetTitle(b.title())
	b.state.MarkAsConfigured()

	return nil
}

func (b *Bulk[E]) writeBatch(items []E) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.TimeoutDuration)
	defer cancel()

	if err := b.format.Write(ctx, b.dialect, items); err != nil {
		return fmt.Errorf("format.Write: %w", err)
	}

	b.state.AddAmount(int64(len(items)))

	return nil
}

func (b *Bulk[E]) write() error {
	var buf []E

loop:
	for {
		select {
		case <-b.writeCtx.Done():
			break loop
		case items, ok := <-b.itemsChan:
			if !ok {
				break loop
			}

			if len(items) == 0 {
				continue
			}

			buf = append(buf, items...)

			for int64(len(buf)) >= b.config.BatchSize {
				if err := b.writeBatch(buf[:b.config.BatchSize]); err != nil {
					return fmt.Errorf("writeBatch: %w", err)
				}

				buf = slices.Clone(buf[b.config.BatchSize:])
			}
		}
	}

	if len(buf) > 0 {
		if err := b.writeBatch(buf); err != nil {
			return fmt.Errorf("writeBatch: %w", err)
		}
	}

	return nil
}
