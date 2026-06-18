package exec

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
	"golang.org/x/sync/errgroup"
)

var _ Runner[string, string] = (*runner[string, string])(nil)

// Executor unifies consumer and producer processing strategies.
// SE is the source element type; DE is the destination element type.
// In the consumer path, out is nil (no data produced).
// In the producer path, out carries the produced data forwarded to a destination.
type Executor[SE, DE storage.Entry] interface {
	// Exec
	// out: produced data (producer path); nil (consumer path)
	// amount: input amount
	// affected: output affected amount
	Exec(items []SE) (out []DE, amount, affected int64, err error)
}

// Runner defines the lifecycle interface for an executable task.
type Runner[SE, DE storage.Entry] interface {
	Prepare(ctx context.Context) error // preparation before processing data
	Receive([]SE)                      // receive data asynchronously
	Start()                            // start processing data
	Done()                             // triggered after upstream data processing
	Finish() error                     // data processing completed
	Close() error
	Summary() string
	State() []string
	Output() []string
	OutChan() <-chan []DE // produced data output channel (producer path)
}

type runner[SE, DE storage.Entry] struct {
	total    int64
	amount   int64
	affected int64

	inChan   chan []SE
	outChan  chan []DE
	executor Executor[SE, DE]
	operator operator.Operator[SE]

	startGroup *errgroup.Group
	startCtx   context.Context
}

func NewRunner[SE, DE storage.Entry](e Executor[SE, DE], o operator.Operator[SE]) Runner[SE, DE] {
	r := &runner[SE, DE]{}
	r.executor = e
	r.operator = o
	r.inChan = make(chan []SE, o.Concurrency())
	r.outChan = make(chan []DE, o.Concurrency())

	return r
}

func (r *runner[SE, DE]) Prepare(ctx context.Context) error {
	r.operator.Init()

	err := r.operator.Prepare()
	if err != nil {
		return fmt.Errorf("operator.Prepare: %w", err)
	}

	err = r.operator.BeforeExec()
	if err != nil {
		return fmt.Errorf("operator.BeforeExec: %w", err)
	}

	r.startGroup, r.startCtx = errgroup.WithContext(ctx)

	return nil
}

func (r *runner[SE, DE]) Receive(items []SE) {
	select {
	case <-r.startCtx.Done():
	case r.inChan <- items:
	}
}

func (r *runner[SE, DE]) Start() {
	for i := 0; i < r.operator.Concurrency(); i++ {
		r.startGroup.Go(func() error {
			for {
				select {
				case <-r.startCtx.Done():
					return nil
				case items, ok := <-r.inChan:
					if !ok {
						return nil
					}

					atomic.AddInt64(&r.total, int64(len(items)))
					out, amount, affected, err1 := r.executor.Exec(items)
					if err1 != nil {
						return fmt.Errorf("executor.Exec: %w", err1)
					}

					atomic.AddInt64(&r.amount, amount)
					atomic.AddInt64(&r.affected, affected)

					if len(out) > 0 {
						select {
						case <-r.startCtx.Done():
							return nil
						case r.outChan <- out:
						}
					}
				}
			}
		})
	}
}

func (r *runner[SE, DE]) Done() {
	close(r.inChan)
}

func (r *runner[SE, DE]) Finish() error {
	err := r.startGroup.Wait()
	if err != nil {
		return fmt.Errorf("runGroup.Wait: %w", err)
	}

	close(r.outChan)

	err = r.operator.AfterExec()
	if err != nil {
		return fmt.Errorf("operator.AfterExec: %w", err)
	}

	return nil
}

func (r *runner[SE, DE]) Close() error {
	return r.operator.Close()
}

func (r *runner[SE, DE]) Summary() string {
	return r.operator.Summary()
}

func (r *runner[SE, DE]) State() []string {
	r.operator.AppendState()
	return append([]string{fmt.Sprintf("Total: %d, Amount %d, Affected %d", atomic.LoadInt64(&r.total), atomic.LoadInt64(&r.amount), atomic.LoadInt64(&r.affected))}, r.operator.State()...)
}

func (r *runner[SE, DE]) Output() []string {
	return r.operator.Output()
}

func (r *runner[SE, DE]) OutChan() <-chan []DE {
	return r.outChan
}
