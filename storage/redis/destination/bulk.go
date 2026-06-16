package destination

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/format"
	"golang.org/x/sync/errgroup"
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
	state     *storage.State

	// 并发与错误处理
	writeGroup  *errgroup.Group
	writeCtx    context.Context
	writeCancel context.CancelFunc
	writeError  error
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

	ctx, cancel := context.WithCancel(context.Background())
	b.writeGroup, b.writeCtx = errgroup.WithContext(ctx)
	b.writeCancel = cancel

	for i := 0; i < b.concurrency; i++ {
		b.writeGroup.Go(func() error {
			return b.write()
		})
	}

	return nil
}

func (b *Bulk[E]) Receive(items []E) error {
	select {
	case <-b.writeCtx.Done():
	case b.itemsChan <- items:
	}
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
	b.writeError = b.writeGroup.Wait()

	b.writeCancel()

	b.state.MarkAsFinished()
	b.state.DurationStop()

	return b.writeError
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
	b.state.Concurrency = b.concurrency
	b.state.Title = b.Title()
	b.state.MarkAsConfigured()

	return nil
}

func (b *Bulk[E]) writeBatch(items []E) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
	defer cancel()

	if err := b.format.Write(ctx, b.dialect, b.keyName, items); err != nil {
		return fmt.Errorf("redis destination write error; %w", err)
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
			return nil
		case items, ok := <-b.itemsChan:
			if !ok {
				break loop
			}

			if len(items) == 0 {
				continue
			}

			buf = append(buf, items...)

			for int64(len(buf)) >= b.pageSize {
				if err := b.writeBatch(buf[:b.pageSize]); err != nil {
					return err
				}

				buf = slices.Clone(buf[b.pageSize:])
			}
		}
	}

	if len(buf) > 0 {
		if err := b.writeBatch(buf); err != nil {
			return err
		}
	}

	return nil
}
