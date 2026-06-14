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

func newDestination[E storage.Entry](config BulkConfig, d dialect.Dialect, f format.Format[E]) (*Bulk[E], error) {
	if config.PageSize <= 0 {
		return nil, fmt.Errorf("page size[%d] is error", config.PageSize)
	}

	dest := &Bulk[E]{
		dialect: d,
		format:  f,
		config:  config,
	}

	dest.initConfig()

	return dest, nil
}

func (d *Bulk[E]) DB() *database.DB {
	if driver, ok := d.dialect.(database.Driver); ok {
		return driver.DB()
	}

	return nil
}

func (d *Bulk[E]) initConfig() {
	if d.config.Concurrency <= 0 {
		d.config.Concurrency = runtime.NumCPU()
	}

	d.state = storage.NewState()
	d.state.Concurrency = d.config.Concurrency
	d.state.Title = d.Title()
	d.state.MarkAsConfigured()
}

func (d *Bulk[E]) Accept() (err error) {
	d.state.MarkAsAccepted()
	d.state.DurationStart()

	if d.config.IsTruncate {
		err = d.dialect.Truncate()
		if err != nil {
			return
		}
	}

	d.itemsChan = make(chan []E, d.config.Concurrency)
	d.errChan = make(chan error, d.config.Concurrency)

	for i := 0; i < d.config.Concurrency; i++ {
		d.workerWg.Add(1)
		go func() {
			d.do()

			d.workerWg.Done()
		}()
	}

	return nil
}

func (d *Bulk[E]) Receive(items []E) error {
	if d.workerFailed.Load() {
		return d.workerErr
	}

	d.itemsChan <- items
	return nil
}

func (d *Bulk[E]) Done() {
	d.state.MarkAsDone()

	if d.isDone {
		return
	}

	d.isDone = true

	close(d.itemsChan)
}

func (d *Bulk[E]) Finish() error {
	d.workerWg.Wait()
	close(d.errChan)
	for err := range d.errChan {
		if d.firstErr == nil {
			d.firstErr = err
		}
	}

	d.state.MarkAsFinished()
	d.state.DurationStop()

	return d.firstErr
}

func (d *Bulk[E]) Err() error {
	return d.firstErr
}

func (d *Bulk[E]) do() {
	duration := timing.NewDuration()
	duration.Start()
	var descItems []E

	duration.Begin()
	for items := range d.itemsChan {
		if len(items) <= 0 {
			continue
		}

		descItems = append(descItems, items...)

		length := len(descItems)
		var start, end int64

		batchSize := d.config.PageSize
		for {
			end = start + batchSize
			if end <= int64(length) {
				err := d.format.Write(d.dialect, descItems[start:end])
				if err != nil {
					d.workerErr = err
					d.workerFailed.Store(true)
					d.errChan <- fmt.Errorf("database destination exec error; %w", err)
					return
				}

				d.state.AddAmount(batchSize)

				start += batchSize
			} else {
				descItems = slices.Clone(descItems[start:])
				descItems = slices.Clip(descItems)

				break
			}
		}
	}

	if len(descItems) > 0 {
		err := d.format.Write(d.dialect, descItems)
		if err != nil {
			d.workerErr = err
			d.workerFailed.Store(true)
			d.errChan <- fmt.Errorf("database destination exec error; %w", err)
			return
		}

		d.state.AddAmount(int64(len(descItems)))
	}

	duration.End()
	duration.Stop()
}

func (d *Bulk[E]) Title() string {
	return fmt.Sprintf("Destination driver[%s]", d.dialect.DBName())
}

func (d *Bulk[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d", d.Title(), d.config.Concurrency)}
}

func (d *Bulk[E]) State() []string {
	return []string{d.state.Overview()}
}

func (d *Bulk[E]) Close() error {
	return d.dialect.Close()
}
