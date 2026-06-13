package destination

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/format"
)

var _ storage.Destination[storage.MapEntry] = (*key[storage.MapEntry])(nil)

type key[E storage.Entry] struct {
	storage.Storage
	dialect      dialect.Dialect
	format       format.Format[E]
	concurrency  int
	isTruncate   bool
	pageSize     int64
	keyName      string
	isDone       bool
	itemsChan    chan []E
	workerWg     sync.WaitGroup
	state        *storage.State
	errChan      chan error
	firstErr     error
	workerErr    error
	workerFailed atomic.Bool
}

func newKey[E storage.Entry](config Config, d dialect.Dialect, f format.Format[E]) (*key[E], error) {
	k := &key[E]{}
	k.dialect = d
	k.format = f
	err := k.config(config)
	if err != nil {
		return nil, err
	}

	return k, nil
}

func (k *key[E]) Accept() error {
	k.state.MarkAsAccepted()
	k.state.DurationStart()

	if k.isTruncate {
		_, err := k.dialect.Truncate(k.keyName)
		if err != nil {
			return err
		}
	}

	k.itemsChan = make(chan []E, k.concurrency)
	k.errChan = make(chan error, k.concurrency)

	for i := 0; i < k.concurrency; i++ {
		k.workerWg.Add(1)
		go func() {
			k.do()

			k.workerWg.Done()
		}()
	}

	return nil
}

func (k *key[E]) Receive(items []E) error {
	if k.workerFailed.Load() {
		return k.workerErr
	}

	k.itemsChan <- items
	return nil
}

func (k *key[E]) Done() {
	k.state.MarkAsDone()

	if k.isDone {
		return
	}

	k.isDone = true

	close(k.itemsChan)
}

func (k *key[E]) Finish() error {
	k.workerWg.Wait()
	close(k.errChan)

	for err := range k.errChan {
		if k.firstErr == nil {
			k.firstErr = err
		}
	}

	k.state.MarkAsFinished()
	k.state.DurationStop()

	return k.firstErr
}

func (k *key[E]) Err() error {
	return k.firstErr
}

func (k *key[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d; page size:%d", k.Title(), k.concurrency, k.pageSize)}
}

func (k *key[E]) State() []string {
	return []string{k.state.Overview()}
}

func (k *key[E]) Title() string {
	return fmt.Sprintf("Destination redis[%s]:%s", k.dialect.DBName(), k.keyName)
}

func (k *key[E]) FetchLen() (int64, error) {
	return k.format.FetchLen(k.dialect, k.keyName)
}

func (k *key[E]) Close() error {
	return k.dialect.Close()
}

func (k *key[E]) config(config Config) error {
	k.isTruncate = config.IsTruncate
	k.concurrency = config.Concurrency
	k.pageSize = config.PageSize
	k.keyName = config.Key

	if k.concurrency <= 0 {
		k.concurrency = 1
	}

	if k.pageSize <= 0 {
		k.pageSize = 20
	}

	if k.keyName == "" {
		panic("key name is empty")
	}

	k.state = storage.NewState()
	k.state.Title = k.Title()
	k.state.MarkAsConfigured()

	return nil
}

func (k *key[E]) do() {
	for items := range k.itemsChan {
		l := len(items)
		for i := 0; i < l; i += int(k.pageSize) {
			end := i + int(k.pageSize)
			if end > l {
				end = l
			}

			batch := items[i:end]
			err := k.format.Write(k.dialect, k.keyName, batch)
			if err != nil {
				k.workerErr = err
				k.workerFailed.Store(true)
				k.errChan <- fmt.Errorf("redis destination write error; %w", err)
				return
			}
		}

		k.state.AddAmount(int64(l))
	}
}
