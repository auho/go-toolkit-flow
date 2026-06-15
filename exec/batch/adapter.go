package batch

import (
	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/task"
)

var _ exec.Processor[string] = (*Adapter[string])(nil)

type Option[E storage.Entry] func(*Adapter[E])

func WithBatch[E storage.Entry](b task.Batch[E]) Option[E] {
	return func(a *Adapter[E]) {
		a.batch = b
	}
}

type Adapter[E storage.Entry] struct {
	batch task.Batch[E]
}

func NewRunner[E storage.Entry](b task.Batch[E]) exec.Runner[E] {
	return NewAdapter(WithBatch(b))
}

func NewAdapter[E storage.Entry](opts ...Option[E]) exec.Runner[E] {
	a := &Adapter[E]{}

	for _, o := range opts {
		o(a)
	}

	return exec.NewRunner[E](a)
}

func (a *Adapter[E]) Concurrency() int {
	return a.batch.Concurrency()
}

func (a *Adapter[E]) Task() task.Task[E] {
	return a.batch
}

func (a *Adapter[E]) Run(items []E) (amount int, affected int, err error) {
	n, err := a.batch.Do(items)
	if err != nil {
		return 0, 0, err
	}

	return len(items), n, nil
}