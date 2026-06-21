package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/processor/consumer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

// adapter adapts a consumer.Batch processor to the exec.Executor interface.
// Consumer path: processes data without producing output (out is always nil).
type adapter[SE, DE storage.Entry] struct {
	batch consumer.Batch[SE]
}

// NewRunner creates a Runner for the consumer batch processor (path one).
// SE and DE are the same type in the consumer path; out is always nil.
func NewRunner[SE, DE storage.Entry](b consumer.Batch[SE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{
		batch: b,
	}

	return exec.NewRunner[SE, DE](a, a.batch)
}

func (a *adapter[SE, DE]) Exec(items []SE) (out []DE, amount, affected int64, err error) {
	n, err := a.batch.Exec(items)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("batch.Exec: %w", err)
	}

	return nil, int64(len(items)), n, nil
}
