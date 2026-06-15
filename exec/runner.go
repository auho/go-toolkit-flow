package exec

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/task"
)

var _ Runner[string] = (*runner[string])(nil)

// Processor unifies Transformer and Batch processing strategies.
type Processor[E storage.Entry] interface {
	Concurrency() int
	Task() task.Task[E]

	// Run
	// amount: input amount
	// affected: output affected amount
	Run(items []E) (amount int, affected int, err error)
}

// Runner defines the lifecycle interface for an executable task.
type Runner[E storage.Entry] interface {
	Prepare() error  // preparation before processing data
	Send([]E) error   // send data asynchronously
	Run() error      // Process data
	Done()           // triggered after upstream data processing
	Finish() error   // data processing completed
	Summary() string
	State() []string
	Output() []string
}

type runner[E storage.Entry] struct {
	total     int64
	amount    int64
	effected  int64
	itemsChan chan []E
	processor Processor[E]
	task      task.Task[E]
	taskWg    sync.WaitGroup
	firstErr  error
	errOnce   sync.Once
}

func NewRunner[E storage.Entry](processor Processor[E]) Runner[E] {
	r := &runner[E]{}
	r.processor = processor
	r.task = r.processor.Task()
	r.itemsChan = make(chan []E, r.processor.Concurrency())

	return r
}

func (r *runner[E]) Prepare() error {
	if !r.task.HasBeenInit() {
		r.task.Init()
	}

	err := r.task.Prepare()
	if err != nil {
		return err
	}

	return nil
}

func (r *runner[E]) Send(items []E) error {
	r.itemsChan <- items
	return nil
}

func (r *runner[E]) Run() error {
	err := r.task.BeforeRun()
	if err != nil {
		return fmt.Errorf("BeforeRun error; %w", err)
	}

	for i := 0; i < r.task.Concurrency(); i++ {
		r.taskWg.Add(1)

		go func() {
			for items := range r.itemsChan {
				atomic.AddInt64(&r.total, int64(len(items)))
				amount, effected, err := r.processor.Run(items)
				if err != nil {
					r.errOnce.Do(func() { r.firstErr = err })
					break
				}
				atomic.AddInt64(&r.amount, int64(amount))
				atomic.AddInt64(&r.effected, int64(effected))
			}

			r.taskWg.Done()
		}()
	}

	return nil
}

func (r *runner[E]) Done() {
	close(r.itemsChan)
}

func (r *runner[E]) Finish() error {
	r.taskWg.Wait()

	if r.firstErr != nil {
		return fmt.Errorf("run error; %w", r.firstErr)
	}

	err := r.task.AfterRun()
	if err != nil {
		return fmt.Errorf("AfterRun error; %w", err)
	}

	err = r.task.Close()
	if err != nil {
		return fmt.Errorf("close error; %w", err)
	}

	return nil
}

func (r *runner[E]) Summary() string {
	return r.task.Title()
}

func (r *runner[E]) State() []string {
	r.task.RefreshState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, effected %d", r.total, r.amount, r.effected)}, r.task.State()...)
}

func (r *runner[E]) Output() []string {
	return r.task.Output()
}