package singleton

import (
	"github.com/auho/go-toolkit-flow/action"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/task"
)

var _ action.Moder[string] = (*Action[string])(nil)

type Option[E storage.Entry] func(singleton *Action[E])

func WithSingleton[E storage.Entry](s task.Singleton[E]) Option[E] {
	return func(a *Action[E]) {
		a.singleton = s
	}
}

type Action[E storage.Entry] struct {
	singleton task.Singleton[E]
}

func NewActor[E storage.Entry](w task.Singleton[E]) *action.Action[E] {
	return NewAction(WithSingleton(w))
}

func NewAction[E storage.Entry](opts ...Option[E]) *action.Action[E] {
	a := &Action[E]{}

	for _, o := range opts {
		o(a)
	}

	return action.NewAction[E](a)
}

func (a *Action[E]) Concurrency() int {
	return a.singleton.Concurrency()
}

func (a *Action[E]) Tasker() task.Tasker[E] {
	return a.singleton
}

func (a *Action[E]) Run(items []E) (int, int) {
	amount := 0
	newItems := make([]E, 0, len(items))
	for k := range items {
		if v, ok := a.singleton.Do(items[k]); ok {
			newItems = append(newItems, v...)
			amount += 1
		}
	}

	a.singleton.PostBatchDo(newItems)

	return amount, len(newItems)
}
