package exec

import (
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/operator"
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

type mockOperator[E storage.Entry] struct {
	operator.BaseOperator
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

func (m *mockOperator[E]) Concurrency() int {
	if m.concurrency <= 0 {
		return 1
	}
	return m.concurrency
}

func (m *mockOperator[E]) Summary() string { return m.summaryStr }

func (m *mockOperator[E]) Prepare() error { m.prepareCalled.Add(1); return m.prepareErr }

func (m *mockOperator[E]) BeforeExec() error { m.beforeExecCalled.Add(1); return m.beforeExecErr }

func (m *mockOperator[E]) AfterExec() error { m.afterExecCalled.Add(1); return m.afterExecErr }

func (m *mockOperator[E]) Close() error { m.closeCalled.Add(1); return m.closeErr }

func (m *mockOperator[E]) AppendState() {}

func newMockRunner[SE, DE storage.Entry](executor Executor[SE, DE], operator operator.Operator[SE]) Runner[SE, DE] {
	return NewRunner[SE, DE](executor, operator)
}