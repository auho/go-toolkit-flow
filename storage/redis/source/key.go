package source

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
)

var _ storage.Source[storage.MapEntry] = (*key[storage.MapEntry])(nil)

type key[E storage.Entry] struct {
	storage.Storage
	dialect   dialect.Dialect
	format    format.Format[E]
	concurrency int
	pageSize    int64
	amount      int64
	total       int64
	keyName     string
	state       *storage.TotalState
	itemsChan   chan []E
	scanned     int64
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

func (k *key[E]) config(config Config) error {
	k.concurrency = config.Concurrency
	k.pageSize = config.PageSize
	k.keyName = config.Key
	k.amount = config.Amount

	if k.concurrency <= 0 {
		k.concurrency = 1
	}

	if k.pageSize <= 0 {
		k.pageSize = 100
	}

	if k.keyName == "" {
		panic("key name is empty")
	}

	k.state = storage.NewTotalState()
	k.state.MarkAsConfigured()
	k.state.Concurrency = k.concurrency
	k.state.Title = k.Title()

	return nil
}

func (k *key[E]) Scan() error {
	k.state.MarkAsScanning()
	k.state.DurationStart()
	k.itemsChan = make(chan []E, k.concurrency)

	var err error
	k.total, err = k.format.FetchLen(k.dialect, k.keyName)
	if err != nil {
		return err
	}

	if k.amount > 0 && k.total >= k.amount {
		k.total = k.amount
	}

	k.state.Total = k.total

	go func() {
		var cursor int64 = 0
		for {
			items, newCursor, scanErr := k.format.ScanByRange(k.dialect, k.keyName, cursor, k.pageSize)
			if scanErr != nil {
				panic(fmt.Sprintf("scan: %v", scanErr))
			}

			if len(items) > 0 {
				atomic.AddInt64(&k.scanned, int64(len(items)))
				k.itemsChan <- items
			}

			if newCursor == 0 {
				break
			}

			if k.amount > 0 && atomic.LoadInt64(&k.scanned) >= k.amount {
				break
			}

			cursor = newCursor
		}

		close(k.itemsChan)

		k.state.DurationStop()
		k.state.MarkAsFinished()
	}()

	return nil
}

func (k *key[E]) ReceiveChan() <-chan []E {
	return k.itemsChan
}

func (k *key[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d", k.Title(), k.total)}
}

func (k *key[E]) State() []string {
	k.state.SetAmount(atomic.LoadInt64(&k.scanned))
	return []string{k.state.Overview()}
}

func (k *key[E]) Copy(items []E) []E {
	return k.format.Copy(items)
}

func (k *key[E]) Title() string {
	return fmt.Sprintf("Source redis[%s]:%s", k.dialect.DBName(), k.keyName)
}

func (k *key[E]) Close() error {
	return k.dialect.Close()
}
