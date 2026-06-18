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
	Prepare(ctx context.Context) error // preparation before processing data
	Receive([]E)                       // receive data asynchronously
	Run()                              // process data
	Done()                             // triggered after upstream data processing
	Finish() error                     // data processing completed
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

	runGroup *errgroup.Group
	runCtx   context.Context
}

func NewRunner[E storage.Entry](p Processor[E], o operator.Operator[E]) Runner[E] {
	r := &runner[E]{}
	r.processor = p
	r.operator = o
	r.itemsChan = make(chan []E, o.Concurrency())

	return r
}

func (r *runner[E]) Prepare(ctx context.Context) error {
	r.operator.Init()

	err := r.operator.Prepare()
	if err != nil {
		return fmt.Errorf("operator.Prepare: %w", err)
	}

	err = r.operator.BeforeExec()
	if err != nil {
		return fmt.Errorf("operator.BeforeExec: %w", err)
	}

	r.runGroup, r.runCtx = errgroup.WithContext(ctx)

	return nil
}

func (r *runner[E]) Receive(items []E) {
	select {
	case <-r.runCtx.Done():
	case r.itemsChan <- items:
	}
}

func (r *runner[E]) Run() {
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
						return fmt.Errorf("processor.Run: %w", err1)
					}

					atomic.AddInt64(&r.amount, amount)
					atomic.AddInt64(&r.effected, effected)
				}
			}
		})
	}
}

func (r *runner[E]) Done() {
	close(r.itemsChan)
}

func (r *runner[E]) Finish() error {
	err := r.runGroup.Wait()
	if err != nil {
		return fmt.Errorf("runGroup.Wait: %w", err)
	}

	err = r.operator.AfterExec()
	if err != nil {
		return fmt.Errorf("operator.AfterExec: %w", err)
	}

	return nil
}

func (r *runner[E]) Close() error {
	return r.operator.Close()
}

func (r *runner[E]) Summary() string {
	return r.operator.Summary()
}

func (r *runner[E]) State() []string {
	r.operator.AppendState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, Effected %d", atomic.LoadInt64(&r.total), atomic.LoadInt64(&r.amount), atomic.LoadInt64(&r.effected))}, r.operator.State()...)
}

func (r *runner[E]) Output() []string {
	return r.operator.Output()
}
