package source

import (
	"context"
	"fmt"
	"sync/atomic"
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
	scanned         int64
	timeoutDuration time.Duration

	state     *storage.TotalState
	itemsChan chan []E
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
	i.state.Concurrency = i.concurrency
	i.state.Title = i.Title()

	return nil
}

func (i *Iterator[E]) Scan() error {
	i.state.MarkAsScanning()
	i.state.DurationStart()
	i.itemsChan = make(chan []E, i.concurrency)

	var err error

	ctx, cancel := context.WithTimeout(context.Background(), i.timeoutDuration)
	i.total, err = i.format.FetchLen(ctx, i.dialect)
	cancel()
	if err != nil {
		return err
	}

	if i.amount > 0 && i.total >= i.amount {
		i.total = i.amount
	}

	i.state.Total = i.total

	go func() {
		defer close(i.itemsChan)

		var cursor uint64
		for {
			ctxScan, cancelScan := context.WithTimeout(context.Background(), i.timeoutDuration)
			items, newCursor, scanErr := i.format.ScanByRange(ctxScan, i.dialect, cursor, i.pageSize)
			cancelScan()

			if scanErr != nil {
				i.scanErr = fmt.Errorf("ScanByRange: %w", scanErr)
				break
			}

			if len(items) > 0 {
				atomic.AddInt64(&i.scanned, int64(len(items)))
				i.itemsChan <- items
			}

			if newCursor == 0 {
				break
			}

			if i.amount > 0 && atomic.LoadInt64(&i.scanned) >= i.amount {
				break
			}

			cursor = newCursor
		}

		i.state.DurationStop()
		i.state.MarkAsFinished()
	}()

	return nil
}

func (i *Iterator[E]) ReceiveChan() <-chan []E {
	return i.itemsChan
}

func (i *Iterator[E]) Error() error {
	return i.scanErr
}

func (i *Iterator[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d", i.Title(), i.total)}
}

func (i *Iterator[E]) State() []string {
	i.state.SetAmount(atomic.LoadInt64(&i.scanned))
	return []string{i.state.Overview()}
}

func (i *Iterator[E]) Copy(items []E) []E {
	return i.format.Copy(items)
}

func (i *Iterator[E]) Title() string {
	return fmt.Sprintf("Source redis[%s]:[%d:%s]:", i.format.Key(), i.dialect.DB(), i.format.Type())
}

func (i *Iterator[E]) Close() error {
	return i.dialect.Close()
}
