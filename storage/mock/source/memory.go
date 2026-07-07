package source

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/mock/source/format"
)

var _ storage.Source[storage.MapEntry] = (*Memory[storage.MapEntry])(nil)

// Memory is an in-memory Source implementation for testing.
// It generates synthetic data in batches and sends it through a channel,
// mimicking the behavior of real sources (e.g. database, file) without
// any external dependencies.
//
// Lifecycle:
//
//	Prepare → Scan (goroutine generates data) → ReceiveChan (consumed by transport) → Finish → Close
//
// Concurrency model:
//   - Scan runs in a single goroutine that writes to itemsChan
//   - ReceiveChan is read by the transport goroutine
//   - Finish waits for the scan goroutine to complete, then closes itemsChan
type Memory[E storage.Entry] struct {
	format format.Format[E]
	config Config

	id        int64  // runtime: auto-increment ID
	totalPage int64  // runtime: computed from total/pageSize

	state     *storage.PageSnapshot
	itemsChan chan []E
	scanCtx   context.Context
	scanWg    sync.WaitGroup
}

// NewMemory creates a Memory with the given config and format.
// Applies defaults: total=100, pageSize=10, concurrency=1, idName="id".
func NewMemory[E storage.Entry](config Config, f format.Format[E]) (*Memory[E], error) {
	m := &Memory[E]{
		config: config,
		format: f,
	}

	if err := m.init(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Memory[E]) init() error {
	if err := m.config.Check(); err != nil {
		return fmt.Errorf("config.Check: %w", err)
	}

	m.totalPage = int64(math.Ceil(float64(m.config.Total) / float64(m.config.PageSize)))

	m.state = storage.NewPageSnapshot()
	m.state.SetTotal(m.config.Total)
	m.state.SetPageSize(m.config.PageSize)
	m.state.SetTotalPage(m.totalPage)
	m.state.SetConcurrency(m.config.Concurrency)
	m.state.SetTitle(m.title())
	m.state.MarkAsConfigured()

	return nil
}

func (m *Memory[E]) Prepare(ctx context.Context) error {
	m.state.MarkAsPrepare()
	m.scanCtx = ctx
	m.itemsChan = make(chan []E, m.config.Concurrency)

	return nil
}

// Scan launches a goroutine that generates data in batches and writes to itemsChan.
// Respects scanCtx cancellation for early termination.
func (m *Memory[E]) Scan() {
	m.state.MarkAsScanning()
	m.state.DurationStart()

	m.scanWg.Go(func() {
		for i := int64(0); i < m.config.Total; i += m.config.PageSize {
			size := m.config.PageSize
			if i+m.config.PageSize > m.config.Total {
				size = m.config.Total - i
			}

			_, items := m.format.Scan(m.config.IDName, &m.id, size)
			select {
			case m.itemsChan <- items:
			case <-m.scanCtx.Done():
				return
			}

			m.state.AddPage(1)
			m.state.AddAmount(int64(len(items)))
		}
	})
}

func (m *Memory[E]) ReceiveChan() <-chan []E {
	return m.itemsChan
}

// Finish waits for the scan goroutine to complete and closes itemsChan.
func (m *Memory[E]) Finish() error {
	m.scanWg.Wait()

	close(m.itemsChan)
	m.state.DurationStop()
	m.state.MarkAsFinished()

	return nil
}

func (m *Memory[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, pageSize: %d", m.title(), m.config.Total, m.config.PageSize)}
}

func (m *Memory[E]) State() storage.State {
	return m.state
}

func (m *Memory[E]) StateString() []string {
	return []string{m.state.Overview()}
}

// Copy creates a deep copy of the items via the format's Copy method.
func (m *Memory[E]) Copy(items []E) []E {
	return m.format.Copy(items)
}

func (m *Memory[E]) title() string {
	return fmt.Sprintf("Source mock[%s]", m.format.Type())
}

func (m *Memory[E]) Close() error {
	return nil
}
