package exec

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
	"golang.org/x/sync/errgroup"
)

var _ Runner[string] = (*runner[string])(nil)

// Processor unifies Transformer and Batch processing strategies.
type Processor[E storage.Entry] interface {
	// Run
	// amount: input amount
	// effected: output effected amount
	Run(items []E) (amount, effected int64, err error)
}

// Runner defines the lifecycle interface for an executable task.
type Runner[E storage.Entry] interface {
	Prepare() error // preparation before processing data
	Receive([]E)    // receive data asynchronously
	Run() error     // Process data
	Done()          // triggered after upstream data processing
	Finish() error  // data processing completed
	Close() error
	Summary() string
	State() []string
	Output() []string
}

type runner[E storage.Entry] struct {
	total    int64
	amount   int64
	effected int64

	itemsChan chan []E
	processor Processor[E]
	operator  operator.Operator[E]

	runGroup  *errgroup.Group
	runCtx    context.Context
	runCancel context.CancelFunc
}

func NewRunner[E storage.Entry](p Processor[E], o operator.Operator[E]) Runner[E] {
	r := &runner[E]{}
	r.processor = p
	r.operator = o
	r.itemsChan = make(chan []E, r.operator.Concurrency())

	ctx, cancel := context.WithCancel(context.Background())
	r.runGroup, r.runCtx = errgroup.WithContext(ctx)
	r.runCancel = cancel

	return r
}

func (r *runner[E]) Prepare() error {
	r.operator.Init()

	return r.operator.Prepare()
}

func (r *runner[E]) Receive(items []E) {
	select {
	case <-r.runCtx.Done():
	case r.itemsChan <- items:
	}
	return
}

func (r *runner[E]) Run() error {
	err := r.operator.BeforeRun()
	if err != nil {
		return fmt.Errorf("BeforeRun: %w", err)
	}

	for i := 0; i < r.operator.Concurrency(); i++ {
		r.runGroup.Go(func() error {
			for {
				select {
				case <-r.runCtx.Done():
					return nil
				case items, ok := <-r.itemsChan:
					if !ok {
						return nil
					}

					atomic.AddInt64(&r.total, int64(len(items)))
					amount, effected, err1 := r.processor.Run(items)
					if err1 != nil {
						return fmt.Errorf("run: %w", err1)
					}

					atomic.AddInt64(&r.amount, amount)
					atomic.AddInt64(&r.effected, effected)
				}
			}
		})
	}

	return nil
}

func (r *runner[E]) Done() {
	close(r.itemsChan)
}

func (r *runner[E]) Finish() error {
	err := r.runGroup.Wait()
	r.runCancel()

	if err != nil {
		return fmt.Errorf("run: %w", err)
	}

	err = r.operator.AfterRun()
	if err != nil {
		return fmt.Errorf("AfterRun: %w", err)
	}

	return nil
}

func (r *runner[E]) Close() error {
	return r.operator.Close()
}

func (r *runner[E]) Summary() string {
	return r.operator.Title()
}

func (r *runner[E]) State() []string {
	r.operator.RefreshState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, Effected %d", atomic.LoadInt64(&r.total), atomic.LoadInt64(&r.amount), atomic.LoadInt64(&r.effected))}, r.operator.State()...)
}

func (r *runner[E]) Output() []string {
	return r.operator.Output()
}
