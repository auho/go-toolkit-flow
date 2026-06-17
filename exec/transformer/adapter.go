package transformer

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Processor[string] = (*adapter[string])(nil)

type adapter[E storage.Entry] struct {
	transformer operator.Transformer[E]
}

func NewRunner[E storage.Entry](t operator.Transformer[E]) exec.Runner[E] {
	a := &adapter[E]{
		transformer: t,
	}

	return exec.NewRunner[E](a, a.transformer)
}

func (a *adapter[E]) Run(items []E) (amount, effected int64, err error) {
	newItems := make([]E, 0, len(items))
	for k := range items {
		v, ok, err1 := a.transformer.Do(items[k])
		if err1 != nil {
			return 0, 0, fmt.Errorf("transformer.Do: %w", err1)
		}

		if ok {
			newItems = append(newItems, v...)
			amount += 1
		}
	}

	err = a.transformer.PostBatchDo(newItems)
	if err != nil {
		return amount, int64(len(newItems)), fmt.Errorf("transformer.PostBatchDo: %w", err)
	}

	return amount, int64(len(newItems)), nil
}
