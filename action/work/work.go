package work

import (
	"log"

	"github.com/auho/go-toolkit-flow/action"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/task"
)

var _ action.Moder[string] = (*Action[string])(nil)

type Option[E storage.Entry] func(*Action[E])

func WithWork[E storage.Entry](w task.Work[E]) Option[E] {
	return func(a *Action[E]) {
		a.work = w
	}
}

type Action[E storage.Entry] struct {
	work task.Work[E]
}

func NewActor[E storage.Entry](w task.Work[E]) *action.Action[E] {
	return NewAction(WithWork(w))
}

func NewAction[E storage.Entry](opts ...Option[E]) *action.Action[E] {
	a := &Action[E]{}

	for _, o := range opts {
		o(a)
	}

	return action.NewAction[E](a)
}

func (a *Action[E]) Concurrency() int {
	return a.work.Concurrency()
}

func (a *Action[E]) Tasker() task.Tasker[E] {
	return a.work
}

func (a *Action[E]) Run(items []E) (int, int) {
	effected := 0
	n, err := a.work.Do(items)
	if err != nil {
		log.Printf("work.Do error: %v", err)
	}
	effected += n

	return len(items), effected
}
