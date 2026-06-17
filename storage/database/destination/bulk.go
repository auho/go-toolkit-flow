package destination

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/database/destination/format"
	"github.com/auho/go-toolkit/time/timing"
	"golang.org/x/sync/errgroup"
)

// WriteConfig 类型别名重导出，用户无需导入 dialect 包
type WriteConfig = dialect.WriteConfig

var _ storage.Destination[storage.MapEntry] = (*Bulk[storage.MapEntry])(nil)
var _ database.Driver = (*Bulk[storage.MapEntry])(nil)

type Bulk[E storage.Entry] struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[E]
	config  BulkConfig

	state     *storage.State
	itemsChan chan []E

	// 并发与错误处理
	writeGroup  *errgroup.Group
	writeCtx    context.Context
	writeCancel context.CancelFunc
	writeError  error

	isDone atomic.Bool
}

func newBulk[E storage.Entry](f format.Format[E], d dialect.Dialect, c BulkConfig) (*Bulk[E], error) {
	if c.PageSize <= 0 {
		return nil, fmt.Errorf("page size[%d] is error", c.PageSize)
	}

	dest := &Bulk[E]{
		dialect: d,
		format:  f,
		config:  c,
	}

	dest.initConfig()

	return dest, nil
}

func (b *Bulk[E]) DB() *database.DB {
	if driver, ok := b.dialect.(database.Driver); ok {
		return driver.DB()
	}

	return nil
}

func (b *Bulk[E]) initConfig() {
	if b.config.Concurrency <= 0 {
		b.config.Concurrency = runtime.NumCPU()
	}

	b.state = storage.NewState()
	b.state.Concurrency = b.config.Concurrency
	b.state.Title = b.Title()
	b.state.MarkAsConfigured()
}

func (b *Bulk[E]) Prepare(ctx context.Context) error {
	b.state.MarkAsPrepare()

	if b.config.IsTruncate {
		err := b.dialect.Truncate()
		if err != nil {
			return err
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
	b.writeError = b.writeGroup.Wait()

	b.writeCancel()

	b.state.MarkAsFinished()
	b.state.DurationStop()

	return b.writeError
}

func (b *Bulk[E]) writeBatch(items []E) error {
	if err := b.format.Write(b.dialect, items); err != nil {
		return fmt.Errorf("format.Write: %w", err)
	}

	b.state.AddAmount(int64(len(items)))

	return nil
}

func (b *Bulk[E]) write() error {
	duration := timing.NewDuration()
	duration.Start()
	duration.Begin()

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

			for int64(len(buf)) >= b.config.PageSize {
				if err := b.writeBatch(buf[:b.config.PageSize]); err != nil {
					return fmt.Errorf("writeBatch: %w", err)
				}

				buf = slices.Clone(buf[b.config.PageSize:])
			}
		}
	}

	// flush remaining
	if len(buf) > 0 {
		if err := b.writeBatch(buf); err != nil {
			return err
		}
	}

	duration.End()
	duration.Stop()

	return nil
}

func (b *Bulk[E]) Title() string {
	return fmt.Sprintf("Destination driver[%s]", b.dialect.DBName())
}

func (b *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d", b.Title(), b.config.Concurrency)}
}

func (b *Bulk[E]) State() []string {
	return []string{b.state.Overview()}
}

func (b *Bulk[E]) Close() error {
	return b.dialect.Close()
}
