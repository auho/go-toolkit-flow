package destination

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit/time/timing"
)

var _ storage.Destination[storage.MapEntry] = (*Destination[storage.MapEntry])(nil)
var _ database.Driver = (*Destination[storage.MapEntry])(nil)

type Executor[E storage.Entry] interface {
	Exec(d *Destination[E], items []E) error
}

type Destination[E storage.Entry] struct {
	storage.Storage
	db     *database.DB
	isDone bool

	isTruncate  bool
	concurrency int
	table       string
	pageSize    int64

	state     *storage.State
	workerWg  sync.WaitGroup
	dst       Executor[E]
	itemsChan   chan []E
	errChan     chan error
	firstErr    error
	workerErr   error
	workerFailed atomic.Bool
}

func NewDestination[E storage.Entry](config *Config, dst Executor[E], b database.BuildDb) (*Destination[E], error) {
	d := &Destination[E]{}
	err := d.config(config, b)
	if err != nil {
		return nil, err
	}

	d.dst = dst

	return d, nil
}

func (d *Destination[E]) DB() *database.DB {
	return d.db
}

func (d *Destination[E]) TableName() string {
	return d.table
}

func (d *Destination[E]) PageSize() int64 {
	return d.pageSize
}

func (d *Destination[E]) config(config *Config, b database.BuildDb) (err error) {
	d.isTruncate = config.IsTruncate
	d.concurrency = config.Concurrency
	d.pageSize = config.PageSize
	d.table = config.TableName

	d.db, err = b()
	if err != nil {
		return
	}

	err = d.db.Ping()
	if err != nil {
		return
	}

	if d.concurrency <= 0 {
		d.concurrency = runtime.NumCPU()
	}

	if d.pageSize <= 0 {
		err = fmt.Errorf("page size[%d] is error", d.pageSize)
		return
	}

	d.state = storage.NewState()
	d.state.Concurrency = d.concurrency
	d.state.Title = d.Title()
	d.state.MarkAsConfigured()

	return
}

func (d *Destination[E]) Accept() (err error) {
	d.state.MarkAsAccepted()
	d.state.DurationStart()

	if d.isTruncate {
		err = d.db.Truncate(d.table)
		if err != nil {
			return
		}
	}

	d.itemsChan = make(chan []E, d.concurrency)
	d.errChan = make(chan error, d.concurrency)

	for i := 0; i < d.concurrency; i++ {
		d.workerWg.Add(1)
		go func() {
			d.do()

			d.workerWg.Done()
		}()
	}

	return nil
}

func (d *Destination[E]) Receive(items []E) error {
	if d.workerFailed.Load() {
		return d.workerErr
	}

	d.itemsChan <- items
	return nil
}

func (d *Destination[E]) Done() {
	d.state.MarkAsDone()

	if d.isDone {
		return
	}

	d.isDone = true

	close(d.itemsChan)
}

func (d *Destination[E]) Finish() error {
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

func (d *Destination[E]) Err() error {
	return d.firstErr
}

func (d *Destination[E]) do() {
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
		start := 0
		end := 0
		batchSize := int(d.pageSize)
		for {
			end = start + batchSize
			if end <= length {
				err := d.dst.Exec(d, descItems[start:end])
				if err != nil {
					d.workerErr = err
					d.workerFailed.Store(true)
					d.errChan <- fmt.Errorf("database destination exec error; %w", err)
					return
				}

				d.state.AddAmount(int64(batchSize))

				start += batchSize
			} else {
				descItems = slices.Clone(descItems[start:])
				descItems = slices.Clip(descItems)

				break
			}
		}
	}

	if len(descItems) > 0 {
		err := d.dst.Exec(d, descItems)
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

func (d *Destination[E]) Title() string {
	return fmt.Sprintf("Destination driver[%s]", d.db.Name())
}

func (d *Destination[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d", d.Title(), d.concurrency)}
}

func (d *Destination[E]) State() []string {
	return []string{d.state.Overview()}
}

func (d *Destination[E]) Close() error {
	return d.db.Close()
}
