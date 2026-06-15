package transformer

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ exec.Processor[string] = (*Adapter[string])(nil)

type Option[E storage.Entry] func(adapter *Adapter[E])

func WithTransformer[E storage.Entry](t operator.Transformer[E]) Option[E] {
	return func(a *Adapter[E]) {
		a.transformer = t
	}
}

type Adapter[E storage.Entry] struct {
	transformer operator.Transformer[E]
}

func NewRunner[E storage.Entry](t operator.Transformer[E]) exec.Runner[E] {
	return NewAdapter(WithTransformer(t))
}

func NewAdapter[E storage.Entry](opts ...Option[E]) exec.Runner[E] {
	a := &Adapter[E]{}

	for _, o := range opts {
		o(a)
	}

	return exec.NewRunner[E](a)
}

func (a *Adapter[E]) Operator() operator.Operator[E] {
	return a.transformer
}

func (a *Adapter[E]) Run(items []E) (amount, effected int64, err error) {
	newItems := make([]E, 0, len(items))
	for k := range items {
		v, ok, err1 := a.transformer.Do(items[k])
		if err1 != nil {
			return 0, 0, fmt.Errorf("do: %w", err1)
		}

		if ok {
			newItems = append(newItems, v...)
			amount += 1
		}
	}

	err = a.transformer.PostBatchDo(newItems)
	if err != nil {
		return amount, int64(len(newItems)), fmt.Errorf("PostBatchDo: %w", err)
	}

	return amount, int64(len(newItems)), nil
}
