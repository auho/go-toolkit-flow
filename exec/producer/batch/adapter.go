package batch

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/processor/producer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

// adapter adapts a producer.Batch processor to the exec.Executor interface.
// Producer path: processes data and produces output forwarded to a destination.
type adapter[SE, DE storage.Entry] struct {
	batch        producer.Batch[SE, DE]
	afterBatcher processor.AfterBatcher[DE] // nil if not implemented
}

// NewRunner creates a Runner for the producer batch processor (path two).
// Exec returns produced data which is forwarded to a destination.
func NewRunner[SE, DE storage.Entry](b producer.Batch[SE, DE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{batch: b}
	if ab, ok := b.(processor.AfterBatcher[DE]); ok {
		a.afterBatcher = ab
	}

	return exec.NewRunner[SE, DE](a, a.batch)
}

func (a *adapter[SE, DE]) Exec(items []SE) (out []DE, amount, affected int64, err error) {
	newItems, n, err := a.batch.Exec(items)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("batch.Exec: %w", err)
	}

	if a.afterBatcher != nil {
		if err = a.afterBatcher.AfterBatch(newItems); err != nil {
			return nil, int64(len(items)), n, fmt.Errorf("batch.AfterBatch: %w", err)
		}
	}

	return newItems, int64(len(items)), n, nil
}
