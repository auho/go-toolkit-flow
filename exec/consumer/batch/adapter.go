package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator/consumer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

type adapter[SE, DE storage.Entry] struct {
	batch consumer.Batch[SE]
}

// NewRunner creates a Runner for the consumer batch operator (path one).
// SE and DE are the same type in the consumer path; out is always nil.
func NewRunner[SE, DE storage.Entry](b consumer.Batch[SE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{
		batch: b,
	}

	return exec.NewRunner[SE, DE](a, a.batch)
}

func (a *adapter[SE, DE]) Exec(items []SE) (amount, affected int64, out []DE, err error) {
	n, err := a.batch.Exec(items)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch.Exec: %w", err)
	}

	return int64(len(items)), n, nil, nil
}
