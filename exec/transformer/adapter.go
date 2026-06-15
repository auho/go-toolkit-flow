package transformer

import (
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

func (a *Adapter[E]) Run(items []E) (amount int, affected int, err error) {
	amount = 0
	newItems := make([]E, 0, len(items))
	for k := range items {
		if v, ok := a.transformer.Do(items[k]); ok {
			newItems = append(newItems, v...)
			amount += 1
		}
	}

	err = a.transformer.PostBatchDo(newItems)
	if err != nil {
		return amount, len(newItems), err
	}

	return amount, len(newItems), nil
}
