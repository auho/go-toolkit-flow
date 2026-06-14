package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/format"
)

var _ storage.Destination[storage.MapEntry] = (*Bulk[storage.MapEntry])(nil)

type Bulk[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]

	concurrency     int
	isTruncate      bool
	pageSize        int64
	timeOutDuration time.Duration
	keyName         string

	isDone    bool
	itemsChan chan []E
	workerWg  sync.WaitGroup
	state     *storage.State

	errChan      chan error
	firstErr     error
	workerErr    error
	workerFailed atomic.Bool
}

func newBulk[E storage.Entry](config BulkConfig, d dialect.Dialect, f format.Format[E]) (*Bulk[E], error) {
	k := &Bulk[E]{}
	k.dialect = d
	k.format = f

	err := k.config(config)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	return k, nil
}

func (b *Bulk[E]) Accept() error {
	b.state.MarkAsAccepted()
	b.state.DurationStart()

	if b.isTruncate {
		ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
		defer cancel()

		_, err := b.dialect.Truncate(ctx, b.keyName)
		if err != nil {
			return err
		}
	}

	b.itemsChan = make(chan []E, b.concurrency)
	b.errChan = make(chan error, b.concurrency)

	for i := 0; i < b.concurrency; i++ {
		b.workerWg.Add(1)
		go func() {
			b.do()

			b.workerWg.Done()
		}()
	}

	return nil
}

func (b *Bulk[E]) Receive(items []E) error {
	if b.workerFailed.Load() {
		return b.workerErr
	}

	b.itemsChan <- items
	return nil
}

func (b *Bulk[E]) Done() {
	b.state.MarkAsDone()

	if b.isDone {
		return
	}

	b.isDone = true

	close(b.itemsChan)
}

func (b *Bulk[E]) Finish() error {
	b.workerWg.Wait()
	close(b.errChan)

	for err := range b.errChan {
		if b.firstErr == nil {
			b.firstErr = err
		}
	}

	b.state.MarkAsFinished()
	b.state.DurationStop()

	return b.firstErr
}

func (b *Bulk[E]) Err() error {
	return b.firstErr
}

func (b *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d; page size:%d", b.Title(), b.concurrency, b.pageSize)}
}

func (b *Bulk[E]) State() []string {
	return []string{b.state.Overview()}
}

func (b *Bulk[E]) Title() string {
	return fmt.Sprintf("Destination redis[%s]:%s", b.dialect.DBName(), b.keyName)
}

func (b *Bulk[E]) FetchLen() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
	defer cancel()

	return b.format.FetchLen(ctx, b.dialect, b.keyName)
}

func (b *Bulk[E]) Close() error {
	return b.dialect.Close()
}

func (b *Bulk[E]) config(config BulkConfig) error {
	b.isTruncate = config.IsTruncate
	b.concurrency = config.Concurrency
	b.pageSize = config.PageSize
	b.timeOutDuration = config.GetTimeOutDuration()
	b.keyName = config.KeyName

	if b.concurrency <= 0 {
		b.concurrency = 1
	}

	if b.pageSize <= 0 {
		b.pageSize = 20
	}

	if b.keyName == "" {
		return fmt.Errorf("key name is empty")
	}

	b.state = storage.NewState()
	b.state.Title = b.Title()
	b.state.MarkAsConfigured()

	return nil
}

func (b *Bulk[E]) do() {
	cancels := make([]context.CancelFunc, 0)

	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	for items := range b.itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(b.pageSize) {
			end := i + int(b.pageSize)
			if end > l {
				end = l
			}

			batch := items[i:end]

			ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
			cancels = append(cancels, cancel)

			err := b.format.Write(ctx, b.dialect, b.keyName, batch)
			if err != nil {
				b.workerErr = err
				b.workerFailed.Store(true)
				b.errChan <- fmt.Errorf("redis destination write error; %w", err)
				return
			}
		}

		b.state.AddAmount(int64(l))
	}
}
