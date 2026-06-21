package exec

import (
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/storage"
)

type mockExecutor[SE, DE storage.Entry] struct {
	out      []DE
	amount   int64
	affected int64
	err      error
	callCount atomic.Int64
}

func (m *mockExecutor[SE, DE]) Exec(items []SE) ([]DE, int64, int64, error) {
	m.callCount.Add(1)
	return m.out, m.amount, m.affected, m.err
}

type mockProcessor[E storage.Entry] struct {
	processor.BaseProcessor
	concurrency    int
	prepareErr     error
	beforeExecErr  error
	afterExecErr   error
	closeErr       error
	summaryStr     string
	prepareCalled    atomic.Int64
	beforeExecCalled atomic.Int64
	afterExecCalled  atomic.Int64
	closeCalled      atomic.Int64
}

func (m *mockProcessor[E]) Concurrency() int {
	if m.concurrency <= 0 {
		return 1
	}
	return m.concurrency
}

func (m *mockProcessor[E]) Summary() string { return m.summaryStr }

func (m *mockProcessor[E]) Prepare() error { m.prepareCalled.Add(1); return m.prepareErr }

func (m *mockProcessor[E]) BeforeExec() error { m.beforeExecCalled.Add(1); return m.beforeExecErr }

func (m *mockProcessor[E]) AfterExec() error { m.afterExecCalled.Add(1); return m.afterExecErr }

func (m *mockProcessor[E]) Close() error { m.closeCalled.Add(1); return m.closeErr }

func (m *mockProcessor[E]) AppendState() {}

func newMockRunner[SE, DE storage.Entry](executor Executor[SE, DE], processor processor.Processor[SE]) Runner[SE, DE] {
	return NewRunner[SE, DE](executor, processor)
}
