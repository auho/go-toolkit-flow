package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Processor[string] = (*Adapter[string])(nil)

type Option[E storage.Entry] func(*Adapter[E])

func WithBatch[E storage.Entry](b operator.Batch[E]) Option[E] {
	return func(a *Adapter[E]) {
		a.batch = b
	}
}

type Adapter[E storage.Entry] struct {
	batch operator.Batch[E]
}

func NewRunner[E storage.Entry](b operator.Batch[E]) exec.Runner[E] {
	return NewAdapter(WithBatch(b))
}

func NewAdapter[E storage.Entry](opts ...Option[E]) exec.Runner[E] {
	a := &Adapter[E]{}

	for _, o := range opts {
		o(a)
	}

	return exec.NewRunner[E](a)
}

func (a *Adapter[E]) Operator() operator.Operator[E] {
	return a.batch
}

func (a *Adapter[E]) Run(items []E) (amount, effected int64, err error) {
	n, err := a.batch.Do(items)
	if err != nil {
		return 0, 0, fmt.Errorf("do: %w", err)
	}

	return int64(len(items)), n, nil
}
