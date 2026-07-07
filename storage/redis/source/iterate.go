package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/redis/source/format"
)

var _ storage.Source[storage.MapEntry] = (*Iterator[storage.MapEntry])(nil)

// Iterator is a Redis source that reads data via SCAN-based iteration
// (HSCAN/SSCAN/ZSCAN/LRANGE). Source types are named after their reading
// strategy (cf. database source's Section, which reads by segmented ID ranges).
type Iterator[E storage.Entry] struct {
	dialect dialect.Dialect
	format  format.Format[E]
	config  KeyConfig

	total int64 // runtime: total items available (from FetchLen)

	state     *storage.TotalSnapshot
	itemsChan chan []E
	scanCtx   context.Context
	scanWg    sync.WaitGroup
	scanErr   error
}

func newIterator[E storage.Entry](f format.Format[E], d dialect.Dialect, c KeyConfig) (*Iterator[E], error) {
	i := &Iterator[E]{
		dialect: d,
		format:  f,
		config:  c,
	}

	if err := i.init(); err != nil {
		return nil, err
	}

	err := f.Check()
	if err != nil {
		return nil, fmt.Errorf("check: %w", err)
	}

	return i, nil
}

func (i *Iterator[E]) init() error {
	if err := i.config.Check(); err != nil {
		return fmt.Errorf("config.Check: %w", err)
	}

	i.state = storage.NewTotalSnapshot()
	i.state.MarkAsConfigured()
	i.state.SetConcurrency(i.config.Concurrency)
	i.state.SetTitle(i.title())

	return nil
}

func (i *Iterator[E]) Prepare(ctx context.Context) error {
	i.state.MarkAsPrepare()
	i.scanCtx = ctx
	i.itemsChan = make(chan []E, i.config.Concurrency)

	lenCtx, lenCancel := context.WithTimeout(ctx, i.config.TimeoutDuration)
	defer lenCancel()

	var err error
	i.total, err = i.format.FetchLen(lenCtx, i.dialect)
	if err != nil {
		return fmt.Errorf("format.FetchLen: %w", err)
	}

	if i.config.Amount > 0 && i.total >= i.config.Amount {
		i.total = i.config.Amount
	}

	i.state.SetTotal(i.total)

	return nil
}

func (i *Iterator[E]) Scan() {
	i.state.MarkAsScanning()
	i.state.DurationStart()

	i.scanWg.Go(func() {
		var cursor uint64
		for {
			scanCtx, scanCancel := context.WithTimeout(i.scanCtx, i.config.TimeoutDuration)
			items, newCursor, err := i.format.ScanByRange(scanCtx, i.dialect, cursor, i.config.PageSize)
			scanCancel()

			if err != nil {
				i.scanErr = fmt.Errorf("format.ScanByRange: %w", err)
				break
			}

			if len(items) > 0 {
				i.state.AddAmount(int64(len(items)))

				select {
				case i.itemsChan <- items:
				case <-i.scanCtx.Done():
					return
				}
			}

			if newCursor == 0 {
				break
			}

			if i.config.Amount > 0 && i.state.Amount() >= i.config.Amount {
				break
			}

			cursor = newCursor
		}
	})
}

func (i *Iterator[E]) ReceiveChan() <-chan []E {
	return i.itemsChan
}

func (i *Iterator[E]) Finish() error {
	i.scanWg.Wait()

	close(i.itemsChan)
	i.state.DurationStop()
	i.state.MarkAsFinished()

	return i.scanErr
}

func (i *Iterator[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d", i.title(), i.total)}
}

func (i *Iterator[E]) State() storage.State {
	return i.state
}

func (i *Iterator[E]) StateString() []string {
	return []string{i.state.Overview()}
}

func (i *Iterator[E]) Copy(items []E) []E {
	return i.format.Copy(items)
}

func (i *Iterator[E]) title() string {
	return fmt.Sprintf("Source redis[%s][%d:%s]", i.format.Key(), i.dialect.DB(), i.format.Type())
}

func (i *Iterator[E]) Close() error {
	return i.dialect.Close()
}
