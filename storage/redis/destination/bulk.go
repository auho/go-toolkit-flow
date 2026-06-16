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

	isDone    bool
	itemsChan chan []E
	state     *storage.State

	// 并发与错误处理
	writeGroup  *errgroup.Group
	writeCtx    context.Context
	writeCancel context.CancelFunc
	writeErr    error
}

func newBulk[E storage.Entry](f format.Format[E], d dialect.Dialect, c BulkConfig) (*Bulk[E], error) {
	b := &Bulk[E]{}
	b.dialect = d
	b.format = f

	err := b.config(c)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	err = b.format.Check()
	if err != nil {
		return nil, fmt.Errorf("check: %w", err)
	}

	return b, nil
}

func (b *Bulk[E]) Accept() error {
	b.state.MarkAsAccepted()
	b.state.DurationStart()

	if b.isTruncate {
		ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
		defer cancel()

		_, err := b.dialect.Truncate(ctx, b.format.Key())
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
	b.writeErr = b.writeGroup.Wait()

	b.writeCancel()

	b.state.MarkAsFinished()
	b.state.DurationStop()

	return b.writeErr
}

func (b *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d; page size:%d", b.Title(), b.concurrency, b.pageSize)}
}

func (b *Bulk[E]) State() []string {
	return []string{b.state.Overview()}
}

func (b *Bulk[E]) Title() string {
	return fmt.Sprintf("Destination redis[%s][%d:%s]", b.format.Key(), b.dialect.DB(), b.format.Type())
}

func (b *Bulk[E]) FetchLen() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeOutDuration)
	defer cancel()

	return b.format.FetchLen(ctx, b.dialect)
}

func (b *Bulk[E]) Close() error {
	return b.dialect.Close()
}

func (b *Bulk[E]) config(config BulkConfig) error {
	b.isTruncate = config.IsTruncate
	b.concurrency = config.Concurrency
	b.pageSize = config.PageSize
	b.timeOutDuration = config.getTimeOutDuration()

	if b.concurrency <= 0 {
		b.concurrency = 1
	}

	if b.pageSize <= 0 {
		b.pageSize = 20
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

	if err := b.format.Write(ctx, b.dialect, items); err != nil {
		return fmt.Errorf("redis destination write: %w", err)
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
