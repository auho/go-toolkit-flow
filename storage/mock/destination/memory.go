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

func (m *Memory[E]) Prepare(ctx context.Context) error {
	m.state.MarkAsPrepare()
	m.writeGroup, m.writeCtx = errgroup.WithContext(ctx)
	return nil
}

// Accept creates the items channel and starts a goroutine that counts
// received items by draining the channel.
func (m *Memory[E]) Accept() {
	m.state.MarkAsAccepted()
	m.state.DurationStart()
	m.itemsChan = make(chan []E)

	m.writeGroup.Go(func() error {
		for {
			select {
			case <-m.writeCtx.Done():
				return nil
			case items, ok := <-m.itemsChan:
				if !ok {
					return nil
				}
				m.state.AddAmount(int64(len(items)))
				m.items = append(m.items, items...)
			}
		}
	})
}

func (m *Memory[E]) Receive(items []E) error {
	select {
	case <-m.writeCtx.Done():
		return fmt.Errorf("receive: writeCtx cancelled: %w", m.writeCtx.Err())
	case m.itemsChan <- items:
	}
	return nil
}

// Done closes the items channel. Uses CAS to ensure idempotency:
// subsequent calls are no-ops.
func (m *Memory[E]) Done() {
	if !m.isDone.CompareAndSwap(false, true) {
		return
	}

	m.state.MarkAsDone()

	close(m.itemsChan)
}

// Finish waits for the counter goroutine to exit after the channel is closed.
func (m *Memory[E]) Finish() error {
	err := m.writeGroup.Wait()

	m.state.DurationStop()
	m.state.MarkAsFinished()

	return err
}

func (m *Memory[E]) Summary() []string {
	return []string{m.title()}
}

func (m *Memory[E]) State() storage.State {
	return m.state
}

func (m *Memory[E]) StateString() []string {
	return []string{m.state.Overview()}
}

// Items returns all received items. Must be called after Finish() to ensure
// all data has been collected by the drain goroutine.
func (m *Memory[E]) Items() []E {
	return m.items
}

func (m *Memory[E]) Copy(items []E) []E {
	return m.format.Copy(items)
}

func (m *Memory[E]) Close() error {
	return nil
}

func (m *Memory[E]) title() string {
	return fmt.Sprintf("Destination mock[%s]", m.format.Type())
}
