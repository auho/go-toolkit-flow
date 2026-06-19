package source

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/source/format"
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
	storage.Storage
	format format.Format[E]

	id          int64
	total       int64 // maximum number of items to generate
	pageSize    int64
	totalPage   int64
	concurrency int
	idName      string
	state       *storage.PageStateInfo
	itemsChan   chan []E
	scanCtx     context.Context
	scanWg      sync.WaitGroup
}

// NewMemory creates a Memory with the given config and format.
// Applies defaults: total=100, pageSize=10, concurrency=1, idName="id".
func NewMemory[E storage.Entry](config Config, f format.Format[E]) *Memory[E] {
	m := &Memory[E]{}
	m.idName = config.IDName
	m.format = f
	m.total = config.Total
	m.pageSize = config.PageSize
	m.concurrency = config.Concurrency

	if m.total <= 0 {
		m.total = 1e2
	}

	if m.pageSize <= 0 {
		m.pageSize = 1e1
	}

	if m.concurrency <= 0 {
		m.concurrency = 1
	}

	if m.idName == "" {
		m.idName = "id"
	}

	m.totalPage = int64(math.Ceil(float64(m.total) / float64(m.pageSize)))

	m.state = storage.NewPageState()
	m.state.SetTotal(m.total)
	m.state.SetPageSize(m.pageSize)
	m.state.SetTotalPage(m.totalPage)
	m.state.SetConcurrency(m.concurrency)
	m.state.SetTitle(m.title())
	m.state.MarkAsConfigured()

	return m
}

func (m *Memory[E]) Prepare(ctx context.Context) error {
	m.state.MarkAsPrepare()
	m.scanCtx = ctx
	m.itemsChan = make(chan []E, m.concurrency)

	return nil
}

// Scan launches a goroutine that generates data in batches and writes to itemsChan.
// Respects scanCtx cancellation for early termination.
func (m *Memory[E]) Scan() {
	m.state.MarkAsScanning()
	m.state.DurationStart()

	m.scanWg.Add(1)
	go func() {
		defer m.scanWg.Done()

		for i := int64(0); i < m.total; i += m.pageSize {
			size := m.pageSize
			if i+m.pageSize > m.total {
				size = m.total - i
			}

			_, items := m.format.Scan(m.idName, &m.id, size)
			select {
			case m.itemsChan <- items:
			case <-m.scanCtx.Done():
				return
			}

			m.state.AddPage(1)
			m.state.AddAmount(int64(len(items)))
		}
	}()
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
	return []string{fmt.Sprintf("%s: total: %d, pageSize: %d", m.title(), m.total, m.pageSize)}
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
	return fmt.Sprintf("Mock:source[%s]", m.format.Type())
}

func (m *Memory[E]) Close() error {
	return nil
}
