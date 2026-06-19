package source

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

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
//   Prepare → Scan (goroutine generates data) → ReceiveChan (consumed by transport) → Finish → Close
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
	page        int64
	pageSize    int64
	totalPage   int64
	amount      int64
	concurrency int
	idName      string
	itemsChan   chan []E
	scanCtx     context.Context
	scanWg      sync.WaitGroup
}

// NewMemory creates a Memory with the given config and format.
// Applies defaults: total=100, pageSize=10, concurrency=1, idName="id".
func NewMemory[E storage.Entry](config Config, f format.Format[E]) *Memory[E] {
	m := &Memory[E]{}
	m.idName = config.IDName
	m.total = config.Total
	m.pageSize = config.PageSize
	m.concurrency = config.Concurrency
	m.format = f

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

	return m
}

func (m *Memory[E]) Prepare(ctx context.Context) error {
	m.scanCtx = ctx
	m.itemsChan = make(chan []E, m.concurrency)

	return nil
}

// Scan launches a goroutine that generates data in batches and writes to itemsChan.
// Respects scanCtx cancellation for early termination.
func (m *Memory[E]) Scan() {
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

			atomic.AddInt64(&m.page, 1)
			atomic.AddInt64(&m.amount, int64(len(items)))
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

	return nil
}

func (m *Memory[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, pageSize: %d", m.Title(), m.total, m.pageSize)}
}

func (m *Memory[E]) State() []string {
	return []string{fmt.Sprintf("amount: %d/%d, page: %d/%d(%d)", atomic.LoadInt64(&m.amount), m.total, atomic.LoadInt64(&m.page), m.totalPage, m.pageSize)}
}

// Copy creates a deep copy of the items via the format's Copy method.
func (m *Memory[E]) Copy(items []E) []E {
	return m.format.Copy(items)
}

// Total returns the configured total number of items to generate.
func (m *Memory[E]) Total() int64 {
	return m.total
}

// Amount returns the number of items actually generated so far.
func (m *Memory[E]) Amount() int64 {
	return atomic.LoadInt64(&m.amount)
}

func (m *Memory[E]) Title() string {
	return fmt.Sprintf("Mock:source[%s]", m.format.Type())
}

func (m *Memory[E]) Close() error {
	return nil
}
