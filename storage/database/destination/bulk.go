package destination

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/format"
	"golang.org/x/sync/errgroup"
)

// WriteConfig is a type alias re-exported so callers need not import the dialect package.
type WriteConfig = dialect.WriteConfig

var _ storage.Destination[storage.MapEntry] = (*Bulk[storage.MapEntry])(nil)

// Bulk is a database destination that writes data in batch via gorm
// CreateInBatches. Destination types are named after their writing strategy
// (cf. database source's Section, which reads by segmented ID ranges).
type Bulk[E storage.Entry] struct {
	dialect dialect.Dialect
	format  format.Format[E]
	config  BulkConfig

	state     *storage.Snapshot
	itemsChan chan []E

	// Concurrency and error handling
	writeGroup *errgroup.Group
	writeCtx   context.Context
	writeErr   error

	isDone atomic.Bool
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

	return b, nil
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

func (b *Bulk[E]) Prepare(ctx context.Context) error {
	b.state.MarkAsPrepare()

	if b.config.IsTruncate {
		truncateCtx, cancel := context.WithTimeout(ctx, b.config.TimeoutDuration)
		err := b.dialect.Truncate(truncateCtx)
		cancel()
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

func (b *Bulk[E]) writeBatch(items []E) error {
	ctx, cancel := context.WithTimeout(b.writeCtx, b.config.TimeoutDuration)
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

	// flush remaining
	if len(buf) > 0 {
		if err := b.writeBatch(buf); err != nil {
			return fmt.Errorf("writeBatch: %w", err)
		}
	}

	return nil
}

func (b *Bulk[E]) title() string {
	return fmt.Sprintf("Destination db[%s]", b.dialect.DBName())
}

func (b *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d", b.title(), b.config.Concurrency)}
}

func (b *Bulk[E]) State() storage.State {
	return b.state
}

func (b *Bulk[E]) StateString() []string {
	return []string{b.state.Overview()}
}

func (b *Bulk[E]) Close() error {
	return b.dialect.Close()
}
