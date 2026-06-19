package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
)

var _ storage.Source[storage.MapEntry] = (*Iterator[storage.MapEntry])(nil)

type Iterator[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]

	concurrency     int
	pageSize        int64
	amount          int64
	total           int64
	timeoutDuration time.Duration

	state     *storage.TotalState
	itemsChan chan []E
	scanCtx   context.Context
	scanWg    sync.WaitGroup
	scanErr   error
}

func newIterator[E storage.Entry](f format.Format[E], d dialect.Dialect, c KeyConfig) (*Iterator[E], error) {
	i := &Iterator[E]{}
	i.dialect = d
	i.format = f

	err := i.config(c)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	err = f.Check()
	if err != nil {
		return nil, fmt.Errorf("check: %w", err)
	}

	return i, nil
}

func (i *Iterator[E]) config(c KeyConfig) error {
	i.concurrency = c.Concurrency
	i.pageSize = c.PageSize
	i.amount = c.Amount
	i.timeoutDuration = c.getTimeoutDuration()

	if i.concurrency <= 0 {
		i.concurrency = 1
	}

	if i.pageSize <= 0 {
		i.pageSize = 100
	}

	i.state = storage.NewTotalState()
	i.state.MarkAsConfigured()
	i.state.SetConcurrency(i.concurrency)
	i.state.SetTitle(i.title())

	return nil
}

func (i *Iterator[E]) Prepare(ctx context.Context) error {
	i.state.MarkAsPrepare()
	i.scanCtx = ctx
	i.itemsChan = make(chan []E, i.concurrency)

	lenCtx, lenCancel := context.WithTimeout(ctx, i.timeoutDuration)
	defer lenCancel()

	var err error
	i.total, err = i.format.FetchLen(lenCtx, i.dialect)
	if err != nil {
		return fmt.Errorf("format.FetchLen: %w", err)
	}

	if i.amount > 0 && i.total >= i.amount {
		i.total = i.amount
	}

	i.state.SetTotal(i.total)

	return nil
}

func (i *Iterator[E]) Scan() {
	i.state.MarkAsScanning()
	i.state.DurationStart()

	i.scanWg.Add(1)
	go func() {
		defer i.scanWg.Done()

		var cursor uint64
		for {
			scanCtx, scanCancel := context.WithTimeout(i.scanCtx, i.timeoutDuration)
			items, newCursor, err := i.format.ScanByRange(scanCtx, i.dialect, cursor, i.pageSize)
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

			if i.amount > 0 && i.state.Amount() >= i.amount {
				break
			}

			cursor = newCursor
		}
	}()
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

func (i *Iterator[E]) StateInfo() storage.StateInfo {
	return i.state
}

func (i *Iterator[E]) Copy(items []E) []E {
	return i.format.Copy(items)
}

func (i *Iterator[E]) title() string {
	return fmt.Sprintf("Source redis[%s]:[%d:%s]:", i.format.Key(), i.dialect.DB(), i.format.Type())
}

func (i *Iterator[E]) Close() error {
	return i.dialect.Close()
}
