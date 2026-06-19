package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/destination/format"
)

var _ storage.Destination[storage.MapEntry] = (*Memory[storage.MapEntry])(nil)

// Memory is an in-memory Destination implementation for testing.
// It counts the total number of items received via the state's amount field,
// which can be accessed via the StateInfo() method.
//
// Lifecycle:
//
//	Prepare → Accept (starts counter goroutine) → Receive (writes to channel) → Done → Finish → Close
//
// Concurrency model:
//   - Accept starts a single goroutine that drains itemsChan and increments amount
//   - Receive is called serially by the output forwarder
//   - Done closes itemsChan via CAS to ensure idempotency
//   - Finish waits for the counter goroutine to exit
type Memory[E storage.Entry] struct {
	format format.Format[E]

	isDone    atomic.Bool
	state     *storage.StateInfo
	items     []E
	itemsChan chan []E
	chanWg    sync.WaitGroup
}

// NewMemory creates a Memory with the given format.
func NewMemory[E storage.Entry](f format.Format[E]) *Memory[E] {
	d := &Memory[E]{format: f}
	d.state = storage.NewStateInfo()
	d.state.SetTitle(d.title())
	d.state.MarkAsConfigured()
	return d
}

func (d *Memory[E]) Prepare(ctx context.Context) error {
	d.state.MarkAsPrepare()
	return nil
}

// Accept creates the items channel and starts a goroutine that counts
// received items by draining the channel.
func (d *Memory[E]) Accept() {
	d.state.MarkAsAccepted()
	d.state.DurationStart()
	d.itemsChan = make(chan []E)

	d.chanWg.Add(1)
	go func() {
		for items := range d.itemsChan {
			d.state.AddAmount(int64(len(items)))
			d.items = append(d.items, items...)
		}

		d.chanWg.Done()
	}()
}

func (d *Memory[E]) Receive(items []E) error {
	d.itemsChan <- items
	return nil
}

// Done closes the items channel. Uses CAS to ensure idempotency:
// subsequent calls are no-ops.
func (d *Memory[E]) Done() {
	if !d.isDone.CompareAndSwap(false, true) {
		return
	}

	d.state.MarkAsDone()

	close(d.itemsChan)
}

// Finish waits for the counter goroutine to exit after the channel is closed.
func (d *Memory[E]) Finish() error {
	d.chanWg.Wait()

	d.state.DurationStop()
	d.state.MarkAsFinished()

	return nil
}

func (d *Memory[E]) Summary() []string {
	return []string{d.title()}
}

func (d *Memory[E]) StateInfo() storage.State {
	return d.state
}

func (d *Memory[E]) StateString() string {
	return d.state.Overview()
}

// Items returns all received items. Must be called after Finish() to ensure
// all data has been collected by the drain goroutine.
func (d *Memory[E]) Items() []E {
	return d.items
}

func (d *Memory[E]) Close() error {
	return nil
}

func (d *Memory[E]) title() string {
	return fmt.Sprintf("Mock:desc[%s]", d.format.Type())
}
