package exec

import (
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/processor"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

type mockExecutor[SE, DE storage.Entry] struct {
	out       []DE
	amount    int64
	affected  int64
	err       error
	callCount atomic.Int64
}

func (m *mockExecutor[SE, DE]) Exec(items []SE) ([]DE, int64, int64, error) {
	m.callCount.Add(1)
	return m.out, m.amount, m.affected, m.err
}

type mockProcessor[E storage.Entry] struct {
	processor.BaseProcessor
	concurrency     int
	prepareErr      error
	beforeRunErr    error
	afterRunErr     error
	closeErr        error
	summaryStr      string
	prepareCalled   atomic.Int64
	beforeRunCalled atomic.Int64
	afterRunCalled  atomic.Int64
	closeCalled     atomic.Int64
}

func (m *mockProcessor[E]) Concurrency() int {
	if m.concurrency <= 0 {
		return 1
	}
	return m.concurrency
}

func (m *mockProcessor[E]) Summary() string { return m.summaryStr }

func (m *mockProcessor[E]) Prepare() error { m.prepareCalled.Add(1); return m.prepareErr }

func (m *mockProcessor[E]) BeforeRun() error { m.beforeRunCalled.Add(1); return m.beforeRunErr }

func (m *mockProcessor[E]) AfterRun() error { m.afterRunCalled.Add(1); return m.afterRunErr }

func (m *mockProcessor[E]) Close() error { m.closeCalled.Add(1); return m.closeErr }

func (m *mockProcessor[E]) AppendState() {}

func newMockRunner[SE, DE storage.Entry](executor Executor[SE, DE], processor processor.Processor[SE]) Runner[SE, DE] {
	return NewRunner[SE, DE](executor, processor)
}
