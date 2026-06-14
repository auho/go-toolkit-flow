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

var _ storage.Source[storage.MapEntry] = (*Key[storage.MapEntry])(nil)

type Key[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]

	concurrency     int
	pageSize        int64
	amount          int64
	total           int64
	timeOutDuration time.Duration
	keyName         string

	state     *storage.TotalState
	itemsChan chan []E
	scanned   int64
}

func newKey[E storage.Entry](config KeyConfig, d dialect.Dialect, f format.Format[E]) (*Key[E], error) {
	k := &Key[E]{}
	k.dialect = d
	k.format = f
	err := k.config(config)
	if err != nil {
		return nil, err
	}

	return k, nil
}

func (k *Key[E]) config(c KeyConfig) error {
	k.concurrency = c.Concurrency
	k.pageSize = c.PageSize
	k.amount = c.Amount
	k.timeOutDuration = c.GetTimeOutDuration()
	k.keyName = c.KeyName

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

func (k *Key[E]) Scan() error {
	k.state.MarkAsScanning()
	k.state.DurationStart()
	k.itemsChan = make(chan []E, k.concurrency)

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), k.timeOutDuration)
	defer cancel()

	k.total, err = k.format.FetchLen(ctx, k.dialect, k.keyName)
	if err != nil {
		return err
	}

	if k.amount > 0 && k.total >= k.amount {
		k.total = k.amount
	}

	k.state.Total = k.total

	go func() {
		cancels := make([]context.CancelFunc, 0)
		defer func() {
			for _, cancel := range cancels {
				cancel()
			}
		}()

		var cursor uint64 = 0
		for {
			ctx, cancel := context.WithTimeout(context.Background(), k.timeOutDuration)
			cancels = append(cancels, cancel)

			items, newCursor, scanErr := k.format.ScanByRange(ctx, k.dialect, k.keyName, cursor, k.pageSize)
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

func (k *Key[E]) ReceiveChan() <-chan []E {
	return k.itemsChan
}

func (k *Key[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d", k.Title(), k.total)}
}

func (k *Key[E]) State() []string {
	k.state.SetAmount(atomic.LoadInt64(&k.scanned))
	return []string{k.state.Overview()}
}

func (k *Key[E]) Copy(items []E) []E {
	return k.format.Copy(items)
}

func (k *Key[E]) Title() string {
	return fmt.Sprintf("Source redis[%s]:%s", k.dialect.DBName(), k.keyName)
}

func (k *Key[E]) Close() error {
	return k.dialect.Close()
}
