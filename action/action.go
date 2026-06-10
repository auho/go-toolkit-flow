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
	// affected: output affected amount
	Run(items []E) (amount int, affected int, err error) // Process data
}

type Actor[E storage.Entry] interface {
	Prepare() error // preparation before processing data
	Send([]E) error  // send data asynchronously
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
	firstErr  error
	errOnce   sync.Once
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

func (a *Action[E]) Send(items []E) error {
	a.itemsChan <- items
	return nil
}

func (a *Action[E]) Run() error {
	err := a.task.BeforeRun()
	if err != nil {
		return fmt.Errorf("BeforeRun error; %w", err)
	}

	for i := 0; i < a.task.Concurrency(); i++ {
		a.taskWg.Add(1)

		go func() {
			for items := range a.itemsChan {
				atomic.AddInt64(&a.total, int64(len(items)))
				amount, effected, err := a.mode.Run(items)
				if err != nil {
					a.errOnce.Do(func() { a.firstErr = err })
					break
				}
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

	if a.firstErr != nil {
		return fmt.Errorf("run error; %w", a.firstErr)
	}

	err := a.task.AfterRun()
	if err != nil {
		return fmt.Errorf("AfterRun error; %w", err)
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
	a.task.RefreshState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, effected %d", a.total, a.amount, a.effected)}, a.task.State()...)
}

func (a *Action[E]) Output() []string {
	return a.task.Output()
}
