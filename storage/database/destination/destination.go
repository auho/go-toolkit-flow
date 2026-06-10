package destination

import (
	"fmt"
	"runtime"
	"slices"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit/time/timing"
)

var _ storage.Destination[storage.MapEntry] = (*Destination[storage.MapEntry])(nil)
var _ database.Driver = (*Destination[storage.MapEntry])(nil)

type Destinationer[E storage.Entry] interface {
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
	doWg      sync.WaitGroup
	dst       Destinationer[E]
	itemsChan chan []E
	errChan   chan error
	firstErr  error
}

func NewDestination[E storage.Entry](config *Config, dst Destinationer[E], b database.BuildDb) (*Destination[E], error) {
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
	d.state.StatusConfig()

	return
}

func (d *Destination[E]) Accept() (err error) {
	d.state.StatusAccept()
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
		d.doWg.Add(1)
		go func() {
			d.do()

			d.doWg.Done()
		}()
	}

	return nil
}

func (d *Destination[E]) Receive(items []E) error {
	d.itemsChan <- items
	return nil
}

func (d *Destination[E]) Done() {
	d.state.StatusDone()

	if d.isDone {
		return
	}

	d.isDone = true

	close(d.itemsChan)
}

func (d *Destination[E]) Finish() error {
	d.doWg.Wait()
	close(d.errChan)
	for err := range d.errChan {
		if d.firstErr == nil {
			d.firstErr = err
		}
	}

	d.state.StatusFinish()
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

		_len := len(descItems)
		_start := 0
		_end := 0
		_size := int(d.pageSize)
		for {
			_end = _start + _size
			if _end <= _len {
				err := d.dst.Exec(d, descItems[_start:_end])
				if err != nil {
					d.errChan <- fmt.Errorf("database destination exec error; %w", err)
					return
				}

				d.state.AddAmount(int64(_size))

				_start += _size
			} else {
				descItems = slices.Clone(descItems[_start:])
				descItems = slices.Clip(descItems)

				break
			}
		}
	}

	if len(descItems) > 0 {
		err := d.dst.Exec(d, descItems)
		if err != nil {
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
