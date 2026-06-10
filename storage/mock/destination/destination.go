package destination

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destinationer[storage.MapEntry] = (*Destination[storage.MapEntry])(nil)

type Destination[E storage.Entry] struct {
	isDone    bool
	amount    int64
	itemsChan chan []E
	chanWg    sync.WaitGroup
}

func (d *Destination[E]) Accept() error {
	d.itemsChan = make(chan []E)

	d.chanWg.Add(1)
	go func() {
		for items := range d.itemsChan {
			atomic.AddInt64(&d.amount, int64(len(items)))
		}

		d.chanWg.Done()
	}()

	return nil
}

func (d *Destination[E]) Receive(items []E) {
	d.itemsChan <- items
}

func (d *Destination[E]) Done() {
	if d.isDone {
		return
	}

	d.isDone = true
	close(d.itemsChan)
}

func (d *Destination[E]) Finish() {
	d.chanWg.Wait()
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
