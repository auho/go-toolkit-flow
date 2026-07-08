package destination

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/mock/destination/format"
	"golang.org/x/sync/errgroup"
)

var _ storage.Destination[storage.MapEntry] = (*Memory[storage.MapEntry])(nil)

// Memory is an in-memory Destination implementation for testing.
// It counts the total number of items received via the state's amount field,
// which can be accessed via the State() method.
//
// Lifecycle:
//
//	Prepare → Accept (starts counter goroutine) → Receive (writes to channel) → Done → Finish → Close
//
// Concurrency model:
//   - Prepare derives writeCtx from the caller's context via errgroup
//   - Accept starts a single goroutine that drains itemsChan and increments amount
//   - Receive is called serially by the output forwarder; it selects on writeCtx
//     to avoid blocking when the pipeline is cancelled
//   - Done closes itemsChan via CAS to ensure idempotency
//   - Finish waits for the counter goroutine to exit
type Memory[E storage.Entry] struct {
	format format.Format[E]

	isDone     atomic.Bool
	state      *storage.Snapshot
	items      []E
	itemsChan  chan []E
	writeGroup *errgroup.Group
	writeCtx   context.Context
}

// NewMemory creates a Memory with the given format.
func NewMemory[E storage.Entry](f format.Format[E]) *Memory[E] {
	d := &Memory[E]{format: f}
	d.state = storage.NewSnapshot()
	d.state.SetTitle(d.title())
	d.state.MarkAsConfigured()
	return d
}

func (d *Memory[E]) Prepare(ctx context.Context) error {
	d.state.MarkAsPrepare()
	d.writeGroup, d.writeCtx = errgroup.WithContext(ctx)
	return nil
}

// Accept creates the items channel and starts a goroutine that counts
// received items by draining the channel.
func (d *Memory[E]) Accept() {
	d.state.MarkAsAccepted()
	d.state.DurationStart()
	d.itemsChan = make(chan []E)

	d.writeGroup.Go(func() error {
		for {
			select {
			case <-d.writeCtx.Done():
				return nil
			case items, ok := <-d.itemsChan:
				if !ok {
					return nil
				}
				d.state.AddAmount(int64(len(items)))
				d.items = append(d.items, items...)
			}
		}
	})
}

func (d *Memory[E]) Receive(items []E) error {
	select {
	case <-d.writeCtx.Done():
		return fmt.Errorf("receive: writeCtx cancelled: %w", d.writeCtx.Err())
	case d.itemsChan <- items:
	}
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
	err := d.writeGroup.Wait()

	d.state.DurationStop()
	d.state.MarkAsFinished()

	return err
}

func (d *Memory[E]) Summary() []string {
	return []string{d.title()}
}

func (d *Memory[E]) State() storage.State {
	return d.state
}

func (d *Memory[E]) StateString() []string {
	return []string{d.state.Overview()}
}

// Items returns all received items. Must be called after Finish() to ensure
// all data has been collected by the drain goroutine.
func (d *Memory[E]) Items() []E {
	return d.items
}

func (d *Memory[E]) Copy(items []E) []E {
	return d.format.Copy(items)
}

func (d *Memory[E]) Close() error {
	return nil
}

func (d *Memory[E]) title() string {
	return fmt.Sprintf("Destination mock[%s]", d.format.Type())
}
