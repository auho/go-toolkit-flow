package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

// lifecycleDestination is the lifecycle subset of storage.Destination,
// type-erased. Any storage.Destination[E] automatically satisfies this
// interface because the lifecycle methods do not reference E.
// Used by multiStageRunner to manage internal destinations from stages
// that have different element types.
type lifecycleDestination interface {
	Prepare(context.Context) error
	Accept()
	Done()
	Finish() error
	Close() error
	Summary() []string
	StateString() []string
}

// DestinationFinisher is optionally implemented by Runners that manage
// their own internal destinations (e.g., multiStageRunner). The group
// calls DestinationFinish in Phase 4, after all data has been forwarded.
type DestinationFinisher interface {
	DestinationFinish() error
}

// stageRunner is the type-erased Runner interface for internal use.
// Each item passed to Receive is []EIn; each item in OutChan is []EOut.
// Type safety is guaranteed by MultiStageBuilder at construction time.
type stageRunner interface {
	Prepare(runnerCtx, destCtx context.Context) error
	Start()
	Receive(items any)
	Done()
	Finish() error
	OutChan() <-chan any
	Close() error
	Summary() []string
	StateString() []string
	Output() []string
	InternalDests() []lifecycleDestination
}

// stageRunnerAdapter adapts a typed Runner[SE, DE] to stageRunner.
type stageRunnerAdapter[SE, DE storage.Entry] struct {
	runner      Runner[SE, DE]
	outChanOnce sync.Once
	bridged     <-chan any
}

func (a *stageRunnerAdapter[SE, DE]) Prepare(runnerCtx, destCtx context.Context) error {
	return a.runner.Prepare(runnerCtx, destCtx)
}

func (a *stageRunnerAdapter[SE, DE]) Start() {
	a.runner.Start()
}

func (a *stageRunnerAdapter[SE, DE]) Receive(items any) {
	a.runner.Receive(items.([]SE))
}

func (a *stageRunnerAdapter[SE, DE]) Done() {
	a.runner.Done()
}

func (a *stageRunnerAdapter[SE, DE]) Finish() error {
	return a.runner.Finish()
}

func (a *stageRunnerAdapter[SE, DE]) OutChan() <-chan any {
	a.outChanOnce.Do(func() {
		src := a.runner.OutChan()
		ch := make(chan any, cap(src))
		go func() {
			defer close(ch)
			for items := range src {
				ch <- items
			}
		}()
		a.bridged = ch
	})
	return a.bridged
}

func (a *stageRunnerAdapter[SE, DE]) Close() error {
	return a.runner.Close()
}

func (a *stageRunnerAdapter[SE, DE]) Summary() []string {
	return a.runner.Summary()
}

func (a *stageRunnerAdapter[SE, DE]) StateString() []string {
	return a.runner.StateString()
}

func (a *stageRunnerAdapter[SE, DE]) Output() []string {
	return a.runner.Output()
}

func (a *stageRunnerAdapter[SE, DE]) InternalDests() []lifecycleDestination {
	dests := a.runner.Destinations()
	result := make([]lifecycleDestination, len(dests))
	for i, d := range dests {
		result[i] = d
	}
	return result
}

// adaptRunner wraps a typed Runner[SE, DE] as a stageRunner.
func adaptRunner[SE, DE storage.Entry](r Runner[SE, DE]) stageRunner {
	return &stageRunnerAdapter[SE, DE]{runner: r}
}

// multiStageRunner chains multiple Runners as stages.
// It implements Runner[SE, DE] where SE is the first stage's input type
// and DE is the last stage's output type.
type multiStageRunner[SE, DE storage.Entry] struct {
	stages []stageRunner

	ctx           context.Context
	bridgeWG      sync.WaitGroup
	internalDests []lifecycleDestination

	outChanOnce sync.Once
	outChan     <-chan []DE
}

var _ Runner[string, string] = (*multiStageRunner[string, string])(nil)

func (r *multiStageRunner[SE, DE]) Prepare(runnerCtx, destCtx context.Context) error {
	r.ctx = runnerCtx

	for i, s := range r.stages {
		if err := s.Prepare(runnerCtx, destCtx); err != nil {
			return fmt.Errorf("stage[%d].Prepare: %w", i, err)
		}
		r.internalDests = append(r.internalDests, s.InternalDests()...)
	}

	for i, d := range r.internalDests {
		if err := d.Prepare(destCtx); err != nil {
			return fmt.Errorf("internal dest[%d].Prepare: %w", i, err)
		}
	}

	return nil
}

func (r *multiStageRunner[SE, DE]) Start() {
	for _, s := range r.stages {
		s.Start()
	}

	for _, d := range r.internalDests {
		d.Accept()
	}

	for i := 0; i < len(r.stages)-1; i++ {
		r.bridgeWG.Go(func() {
			defer r.stages[i+1].Done()
			for {
				select {
				case <-r.ctx.Done():
					return
				case items, ok := <-r.stages[i].OutChan():
					if !ok {
						return
					}
					r.stages[i+1].Receive(items)
				}
			}
		})
	}
}

func (r *multiStageRunner[SE, DE]) Receive(items []SE) {
	r.stages[0].Receive(items)
}

func (r *multiStageRunner[SE, DE]) Done() {
	r.stages[0].Done()
}

func (r *multiStageRunner[SE, DE]) Finish() error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for i, s := range r.stages {
		wg.Go(func() {
			if err := s.Finish(); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("stage[%d].Finish: %w", i, err))
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	r.bridgeWG.Wait()

	for _, d := range r.internalDests {
		d.Done()
	}

	return errors.Join(errs...)
}

func (r *multiStageRunner[SE, DE]) OutChan() <-chan []DE {
	r.outChanOnce.Do(func() {
		src := r.stages[len(r.stages)-1].OutChan()
		ch := make(chan []DE, cap(src))
		go func() {
			defer close(ch)
			for items := range src {
				ch <- items.([]DE)
			}
		}()
		r.outChan = ch
	})
	return r.outChan
}

func (r *multiStageRunner[SE, DE]) Close() error {
	var errs []error
	for i, s := range r.stages {
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("stage[%d].Close: %w", i, err))
		}
	}
	for i, d := range r.internalDests {
		if err := d.Close(); err != nil {
			errs = append(errs, fmt.Errorf("internal dest[%d].Close: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (r *multiStageRunner[SE, DE]) Destinations() []storage.Destination[DE] {
	return nil
}

// DestinationFinish finalizes internal destinations managed by this runner.
// Called by the group in Phase 4, after all data has been forwarded.
func (r *multiStageRunner[SE, DE]) DestinationFinish() error {
	var errs []error
	for i, d := range r.internalDests {
		if err := d.Finish(); err != nil {
			errs = append(errs, fmt.Errorf("internal dest[%d].Finish: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (r *multiStageRunner[SE, DE]) Summary() []string {
	var lines []string
	for i, s := range r.stages {
		lines = append(lines, fmt.Sprintf("  Stage %d:", i))
		lines = append(lines, s.Summary()...)
	}
	return lines
}

func (r *multiStageRunner[SE, DE]) StateString() []string {
	var lines []string
	for _, s := range r.stages {
		lines = append(lines, s.StateString()...)
	}
	return lines
}

func (r *multiStageRunner[SE, DE]) Output() []string {
	var lines []string
	for _, s := range r.stages {
		lines = append(lines, s.Output()...)
	}
	return lines
}
