package item

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator/producer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

// adapter adapts a producer.Item operator to the exec.Executor interface.
// Producer path: processes data and produces output forwarded to a destination.
type adapter[SE, DE storage.Entry] struct {
	item producer.Item[SE, DE]
}

// NewRunner creates a Runner for the producer item operator (path two).
// Exec returns produced data which is forwarded to a destination.
func NewRunner[SE, DE storage.Entry](it producer.Item[SE, DE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{
		item: it,
	}

	return exec.NewRunner[SE, DE](a, a.item)
}

func (a *adapter[SE, DE]) Exec(items []SE) (out []DE, amount, affected int64, err error) {
	newItems := make([]DE, 0, len(items))
	for k := range items {
		v, ok, err1 := a.item.Exec(items[k])
		if err1 != nil {
			return nil, 0, 0, fmt.Errorf("item.Exec: %w", err1)
		}

		if ok {
			newItems = append(newItems, v...)
			amount += 1
		}
	}

	err = a.item.PostBatchExec(newItems)
	if err != nil {
		return nil, amount, int64(len(newItems)), fmt.Errorf("item.PostBatchExec: %w", err)
	}

	return newItems, amount, int64(len(newItems)), nil
}
