package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator/producer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

type adapter[SE, DE storage.Entry] struct {
	batch producer.Batch[SE, DE]
}

// NewRunner creates a Runner for the producer batch operator (path two).
// Exec returns produced data which is forwarded to a destination.
func NewRunner[SE, DE storage.Entry](b producer.Batch[SE, DE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{
		batch: b,
	}

	return exec.NewRunner[SE, DE](a, a.batch)
}

func (a *adapter[SE, DE]) Exec(items []SE) (amount, affected int64, out []DE, err error) {
	newItems, n, err := a.batch.Exec(items)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("batch.Exec: %w", err)
	}

	return int64(len(items)), n, newItems, nil
}
