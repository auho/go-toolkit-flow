package destination

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/database/destination/format"
	"github.com/auho/go-toolkit/time/timing"
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

	state        *storage.State
	workerWg     sync.WaitGroup
	itemsChan    chan []E
	errChan      chan error
	firstErr     error
	workerErr    error
	workerFailed atomic.Bool
	isDone       bool
}

func newDestination[E storage.Entry](c BulkConfig, d dialect.Dialect, f format.Format[E]) (*Bulk[E], error) {
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

func (b *Bulk[E]) Accept() (err error) {
	b.state.MarkAsAccepted()
	b.state.DurationStart()

	if b.config.IsTruncate {
		err = b.dialect.Truncate()
		if err != nil {
			return
		}
	}

	b.itemsChan = make(chan []E, b.config.Concurrency)
	b.errChan = make(chan error, b.config.Concurrency)

	for i := 0; i < b.config.Concurrency; i++ {
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

func (b *Bulk[E]) do() {
	duration := timing.NewDuration()
	duration.Start()
	var descItems []E

	duration.Begin()
	for items := range b.itemsChan {
		if len(items) <= 0 {
			continue
		}

		descItems = append(descItems, items...)

		length := len(descItems)
		var start, end int64

		batchSize := b.config.PageSize
		for {
			end = start + batchSize
			if end <= int64(length) {
				err := b.format.Write(b.dialect, descItems[start:end])
				if err != nil {
					b.workerErr = err
					b.workerFailed.Store(true)
					b.errChan <- fmt.Errorf("database destination exec error; %w", err)
					return
				}

				b.state.AddAmount(batchSize)

				start += batchSize
			} else {
				descItems = slices.Clone(descItems[start:])
				descItems = slices.Clip(descItems)

				break
			}
		}
	}

	if len(descItems) > 0 {
		err := b.format.Write(b.dialect, descItems)
		if err != nil {
			b.workerErr = err
			b.workerFailed.Store(true)
			b.errChan <- fmt.Errorf("database destination exec error; %w", err)
			return
		}

		b.state.AddAmount(int64(len(descItems)))
	}

	duration.End()
	duration.Stop()
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
