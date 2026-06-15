package exec

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

var _ Runner[string] = (*runner[string])(nil)

// Processor unifies Transformer and Batch processing strategies.
type Processor[E storage.Entry] interface {
	Operator() operator.Operator[E]

	// Run
	// amount: input amount
	// affected: output affected amount
	Run(items []E) (amount int, affected int, err error)
}

// Runner defines the lifecycle interface for an executable task.
type Runner[E storage.Entry] interface {
	Prepare() error // preparation before processing data
	Send([]E) error // send data asynchronously
	Run() error     // Process data
	Done()          // triggered after upstream data processing
	Finish() error  // data processing completed
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
	operator_ operator.Operator[E]
	taskWg    sync.WaitGroup
	firstErr  error
	errOnce   sync.Once
}

func NewRunner[E storage.Entry](processor Processor[E]) Runner[E] {
	r := &runner[E]{}
	r.processor = processor
	r.operator_ = r.processor.Operator()
	r.itemsChan = make(chan []E, r.operator_.Concurrency())

	return r
}

func (r *runner[E]) Prepare() error {
	r.operator_.Init()

	err := r.operator_.Prepare()
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
	err := r.operator_.BeforeRun()
	if err != nil {
		return fmt.Errorf("BeforeRun error; %w", err)
	}

	for i := 0; i < r.operator_.Concurrency(); i++ {
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

	err := r.operator_.AfterRun()
	if err != nil {
		return fmt.Errorf("AfterRun error; %w", err)
	}

	err = r.operator_.Close()
	if err != nil {
		return fmt.Errorf("close error; %w", err)
	}

	return nil
}

func (r *runner[E]) Summary() string {
	return r.operator_.Title()
}

func (r *runner[E]) State() []string {
	r.operator_.RefreshState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, Effected %d", r.total, r.amount, r.effected)}, r.operator_.State()...)
}

func (r *runner[E]) Output() []string {
	return r.operator_.Output()
}
