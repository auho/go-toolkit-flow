package source

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Source[storage.MapEntry] = (*Mock[storage.MapEntry])(nil)

// generator is the strategy interface for generating and duplicating mock data.
// Each concrete generator produces items of a specific entry type (e.g. MapEntry,
// string) and knows how to deep-copy them.
type generator[E storage.Entry] interface {
	// scan generates a batch of items.
	// idName: the name of the ID field; id: pointer to the current ID counter;
	// amount: number of items to generate in this batch.
	// Returns the updated id pointer and the generated items.
	scan(string, *int64, int64) (*int64, []E)

	// duplicate creates a deep copy of the given items.
	duplicate([]E) []E
}

// Mock is an in-memory Source implementation for testing.
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
type Mock[E storage.Entry] struct {
	storage.Storage
	id          int64
	total       int64 // maximum number of items to generate
	page        int64
	pageSize    int64
	totalPage   int64
	amount      int64
	concurrency int
	idName      string
	itemsChan   chan []E
	generator   generator[E]
	scanCtx     context.Context
	scanWg      sync.WaitGroup
}

// newMock creates a Mock with the given config and generator.
// Applies defaults: total=100, pageSize=10, concurrency=1, idName="id".
func newMock[E storage.Entry](config Config, generator generator[E]) *Mock[E] {
	m := &Mock[E]{}
	m.idName = config.IDName
	m.total = config.Total
	m.pageSize = config.PageSize
	m.concurrency = config.Concurrency
	m.generator = generator

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

func (m *Mock[E]) Prepare(ctx context.Context) error {
	m.scanCtx = ctx
	m.itemsChan = make(chan []E, m.concurrency)

	return nil
}

// Scan launches a goroutine that generates data in batches and writes to itemsChan.
// Respects scanCtx cancellation for early termination.
func (m *Mock[E]) Scan() {
	m.scanWg.Add(1)
	go func() {
		defer m.scanWg.Done()

		for i := int64(0); i < m.total; i += m.pageSize {
			size := m.pageSize
			if i+m.pageSize > m.total {
				size = m.total - i
			}

			_, items := m.generator.scan(m.idName, &m.id, size)
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

func (m *Mock[E]) ReceiveChan() <-chan []E {
	return m.itemsChan
}

// Finish waits for the scan goroutine to complete and closes itemsChan.
func (m *Mock[E]) Finish() error {
	m.scanWg.Wait()

	close(m.itemsChan)

	return nil
}

func (m *Mock[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d, pageSize: %d", m.Title(), m.total, m.pageSize)}
}

func (m *Mock[E]) State() []string {
	return []string{fmt.Sprintf("amount: %d/%d, page: %d/%d(%d)", atomic.LoadInt64(&m.amount), m.total, atomic.LoadInt64(&m.page), m.totalPage, m.pageSize)}
}

// Copy creates a deep copy of the items via the generator's duplicate method.
func (m *Mock[E]) Copy(items []E) []E {
	return m.generator.duplicate(items)
}

func (m *Mock[E]) Title() string {
	return "Source mock"
}

func (m *Mock[E]) Close() error {
	return nil
}
