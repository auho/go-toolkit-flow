package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destination[storage.MapEntry] = (*Destination[storage.MapEntry])(nil)

type Destination[E storage.Entry] struct {
	isDone    bool
	amount    int64
	itemsChan chan []E
	chanWg    sync.WaitGroup
}

func (d *Destination[E]) Prepare(ctx context.Context) error {
	return nil
}

func (d *Destination[E]) Accept() {
	d.itemsChan = make(chan []E)

	d.chanWg.Add(1)
	go func() {
		for items := range d.itemsChan {
			atomic.AddInt64(&d.amount, int64(len(items)))
		}

		d.chanWg.Done()
	}()
}

func (d *Destination[E]) Receive(items []E) error {
	d.itemsChan <- items
	return nil
}

func (d *Destination[E]) Done() {
	if d.isDone {
		return
	}

	d.isDone = true
	close(d.itemsChan)
}

func (d *Destination[E]) Finish() error {
	d.chanWg.Wait()

	return nil
}

func (d *Destination[E]) Summary() []string {
	return []string{fmt.Sprintf("%s", d.title())}
}

func (d *Destination[E]) State() []string {
	return []string{fmt.Sprintf("amount: %d", d.amount)}
}

func (d *Destination[E]) Close() error {
	return nil
}

func (d *Destination[E]) title() string {
	return "Mock:desc"
}
