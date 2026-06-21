package item

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/processor/consumer"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Executor[string, string] = (*adapter[string, string])(nil)

// adapter adapts a consumer.Item processor to the exec.Executor interface.
// Consumer path: processes data without producing output (out is always nil).
type adapter[SE, DE storage.Entry] struct {
	item consumer.Item[SE]
}

// NewRunner creates a Runner for the consumer item processor (path one).
// SE and DE are the same type in the consumer path; out is always nil.
func NewRunner[SE, DE storage.Entry](it consumer.Item[SE]) exec.Runner[SE, DE] {
	a := &adapter[SE, DE]{
		item: it,
	}

	return exec.NewRunner[SE, DE](a, a.item)
}

func (a *adapter[SE, DE]) Exec(items []SE) (out []DE, amount, affected int64, err error) {
	for k := range items {
		ok, err1 := a.item.Exec(items[k])
		if err1 != nil {
			return nil, 0, 0, fmt.Errorf("item.Exec: %w", err1)
		}

		if ok {
			amount += 1
		}
	}

	return nil, amount, 0, nil
}
