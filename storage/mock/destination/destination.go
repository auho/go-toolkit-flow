package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/destination/format"
)

var _ storage.Destination[storage.MapEntry] = (*Destination[storage.MapEntry])(nil)

// Destination is an in-memory Destination implementation for testing.
// It counts the total number of items received via the amount field,
// which can be accessed from the same package (white-box testing) or
// via the State() method (returns "amount: <N>").
//
// Lifecycle:
//   Prepare → Accept (starts counter goroutine) → Receive (writes to channel) → Done → Finish → Close
//
// Concurrency model:
//   - Accept starts a single goroutine that drains itemsChan and increments amount
//   - Receive is called serially by the output forwarder
//   - Done closes itemsChan via CAS to ensure idempotency
//   - Finish waits for the counter goroutine to exit
type Destination[E storage.Entry] struct {
	format format.Format[E]

	isDone    atomic.Bool
	amount    int64
	itemsChan chan []E
	chanWg    sync.WaitGroup
}

// NewDestination creates a Destination with the given format.
func NewDestination[E storage.Entry](f format.Format[E]) *Destination[E] {
	return &Destination[E]{format: f}
}

func (d *Destination[E]) Prepare(ctx context.Context) error {
	return nil
}

// Accept creates the items channel and starts a goroutine that counts
// received items by draining the channel.
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

// Done closes the items channel. Uses CAS to ensure idempotency:
// subsequent calls are no-ops.
func (d *Destination[E]) Done() {
	if !d.isDone.CompareAndSwap(false, true) {
		return
	}

	close(d.itemsChan)
}

// Finish waits for the counter goroutine to exit after the channel is closed.
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
	return fmt.Sprintf("Mock:desc[%s]", d.format.Type())
}
