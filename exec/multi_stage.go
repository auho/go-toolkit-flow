package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

// stageRunner is a type-erased Runner for internal use in multi-stage pipelines.
// Created by the Stage function; type safety is guaranteed at construction time
// by the builder's compile-time type parameter evolution.
type stageRunner struct {
	prepare  func(runnerCtx, destCtx context.Context) error
	start    func()
	receive  func(items any)
	done     func()
	finish   func() error
	outChan  func() <-chan any
	close    func() error
	summary  func() []string
	stateStr func() []string
	output   func() []string
}

// MultiStageBuilder constructs a multi-stage Runner with compile-time type safety.
// SE is the source element type (first stage input).
// DE is the current output type (evolves with each Stage call).
type MultiStageBuilder[SE, DE storage.Entry] struct {
	stages []stageRunner
}

// NewMultiStage creates a new builder with the given source type.
func NewMultiStage[SE storage.Entry]() *MultiStageBuilder[SE, SE] {
	return &MultiStageBuilder[SE, SE]{}
}

// Stage adds a processing stage to the builder.
// The runner must accept DE (current output type) and produce DE2 (new output type).
// Type parameters are inferred from the runner argument.
//
// This is a function (not a method) because Go methods cannot have
// additional type parameters beyond the receiver's.
func Stage[SE, DE, DE2 storage.Entry](
	b *MultiStageBuilder[SE, DE],
	r Runner[DE, DE2],
) *MultiStageBuilder[SE, DE2] {
	var outOnce sync.Once
	var bridged <-chan any

	s := stageRunner{
		prepare:  r.Prepare,
		start:    r.Start,
		receive:  func(items any) { r.Receive(items.([]DE)) },
		done:     r.Done,
		finish:   r.Finish,
		close:    r.Close,
		summary:  r.Summary,
		stateStr: r.StateString,
		output:   r.Output,
		outChan: func() <-chan any {
			outOnce.Do(func() {
				src := r.OutChan()
				ch := make(chan any, cap(src))
				go func() {
					defer close(ch)
					for items := range src {
						ch <- items
					}
				}()
				bridged = ch
			})
			return bridged
		},
	}

	stages := make([]stageRunner, len(b.stages), len(b.stages)+1)
	copy(stages, b.stages)
	stages = append(stages, s)
	return &MultiStageBuilder[SE, DE2]{stages: stages}
}

// Build creates the multi-stage Runner.
func (b *MultiStageBuilder[SE, DE]) Build() Runner[SE, DE] {
	return &multiStageRunner[SE, DE]{stages: b.stages}
}

// multiStageRunner chains multiple Runners as stages.
// It implements Runner[SE, DE] where SE is the first stage's input type
// and DE is the last stage's output type.
type multiStageRunner[SE, DE storage.Entry] struct {
	stages      []stageRunner
	ctx         context.Context
	bridgeWG    sync.WaitGroup
	outChanOnce sync.Once
	outChan     <-chan []DE
}

var _ Runner[string, string] = (*multiStageRunner[string, string])(nil)

func (r *multiStageRunner[SE, DE]) Prepare(runnerCtx, destCtx context.Context) error {
	r.ctx = runnerCtx

	for i, s := range r.stages {
		if err := s.prepare(runnerCtx, destCtx); err != nil {
			return fmt.Errorf("stage[%d].Prepare: %w", i, err)
		}
	}

	return nil
}

func (r *multiStageRunner[SE, DE]) Start() {
	for _, s := range r.stages {
		s.start()
	}

	for i := 0; i < len(r.stages)-1; i++ {
		r.bridgeWG.Go(func() {
			defer r.stages[i+1].done()
			for {
				select {
				case <-r.ctx.Done():
					return
				case items, ok := <-r.stages[i].outChan():
					if !ok {
						return
					}
					r.stages[i+1].receive(items)
				}
			}
		})
	}
}

func (r *multiStageRunner[SE, DE]) Receive(items []SE) {
	r.stages[0].receive(items)
}

func (r *multiStageRunner[SE, DE]) Done() {
	r.stages[0].done()
}

func (r *multiStageRunner[SE, DE]) Finish() error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for i, s := range r.stages {
		wg.Go(func() {
			if err := s.finish(); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("stage[%d].Finish: %w", i, err))
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	r.bridgeWG.Wait()

	return errors.Join(errs...)
}

func (r *multiStageRunner[SE, DE]) OutChan() <-chan []DE {
	r.outChanOnce.Do(func() {
		src := r.stages[len(r.stages)-1].outChan()
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
		if err := s.close(); err != nil {
			errs = append(errs, fmt.Errorf("stage[%d].Close: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (r *multiStageRunner[SE, DE]) Destinations() []storage.Destination[DE] {
	return nil
}

func (r *multiStageRunner[SE, DE]) Summary() []string {
	var lines []string
	for i, s := range r.stages {
		lines = append(lines, fmt.Sprintf("  Stage %d:", i))
		lines = append(lines, s.summary()...)
	}
	return lines
}

func (r *multiStageRunner[SE, DE]) StateString() []string {
	var lines []string
	for _, s := range r.stages {
		lines = append(lines, s.stateStr()...)
	}
	return lines
}

func (r *multiStageRunner[SE, DE]) Output() []string {
	var lines []string
	for _, s := range r.stages {
		lines = append(lines, s.output()...)
	}
	return lines
}
