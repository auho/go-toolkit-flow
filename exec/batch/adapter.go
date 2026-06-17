package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Processor[string] = (*adapter[string])(nil)

type adapter[E storage.Entry] struct {
	batch operator.Batch[E]
}

func NewRunner[E storage.Entry](b operator.Batch[E]) exec.Runner[E] {
	a := &adapter[E]{
		batch: b,
	}

	return exec.NewRunner[E](a, a.batch)
}

func (a *adapter[E]) Run(items []E) (amount, effected int64, err error) {
	n, err := a.batch.Exec(items)
	if err != nil {
		return 0, 0, fmt.Errorf("batch.Exec: %w", err)
	}

	return int64(len(items)), n, nil
}
