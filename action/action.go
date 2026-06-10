package action

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/task"
)

var _ Actor[string] = (*Action[string])(nil)

type Mode[E storage.Entry] interface {
	Concurrency() int
	Task() task.Task[E]

	// Run
	// amount: input amount
	// effected: output effected amount
	Run([]E) (int, int) // Process data
}

type Actor[E storage.Entry] interface {
	Prepare() error // preparation before processing data
	Receive([]E) error // receive data asynchronously
	Run() error     // Process data
	Done()          // triggered after upstream data processing
	Finish() error  // data processing completed
	Summary() string
	State() []string
	Output() []string
}

type Action[E storage.Entry] struct {
	total     int64
	amount    int64
	effected  int64
	itemsChan chan []E
	mode      Mode[E]
	task      task.Task[E]
	taskWg    sync.WaitGroup
}

func NewAction[E storage.Entry](mode Mode[E]) *Action[E] {
	a := &Action[E]{}
	a.mode = mode
	a.task = a.mode.Task()
	a.itemsChan = make(chan []E, a.mode.Concurrency())

	return a
}

func (a *Action[E]) Prepare() error {
	if !a.task.HasBeenInit() {
		a.task.Init()
	}

	err := a.task.Prepare()
	if err != nil {
		return err
	}

	return nil
}

func (a *Action[E]) Receive(items []E) error {
	a.itemsChan <- items
	return nil
}

func (a *Action[E]) Run() error {
	err := a.task.PreDo()
	if err != nil {
		return fmt.Errorf("PreDo error; %w", err)
	}

	for i := 0; i < a.task.Concurrency(); i++ {
		a.taskWg.Add(1)

		go func() {
			for items := range a.itemsChan {
				atomic.AddInt64(&a.total, int64(len(items)))
				amount, effected := a.mode.Run(items)
				atomic.AddInt64(&a.amount, int64(amount))
				atomic.AddInt64(&a.effected, int64(effected))
			}

			a.taskWg.Done()
		}()
	}

	return nil
}

func (a *Action[E]) Done() {
	close(a.itemsChan)
}

func (a *Action[E]) Finish() error {
	a.taskWg.Wait()

	err := a.task.PostDo()
	if err != nil {
		return fmt.Errorf("PostDo error; %w", err)
	}

	err = a.task.Close()
	if err != nil {
		return fmt.Errorf("close error; %w", err)
	}

	return nil
}

func (a *Action[E]) Summary() string {
	return a.task.Title()
}

func (a *Action[E]) State() []string {
	a.task.Blink()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, effected %d", a.total, a.amount, a.effected)}, a.task.State()...)
}

func (a *Action[E]) Output() []string {
	return a.task.Output()
}
