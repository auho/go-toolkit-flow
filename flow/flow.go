package flow

import (
	"errors"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

type Option[E storage.Entry] func(*flow[E])

func WithSource[E storage.Entry](se storage.Source[E]) Option[E] {
	return func(s *flow[E]) {
		s.source = se
	}
}

func WithRunner[E storage.Entry](runner exec.Runner[E]) Option[E] {
	return func(s *flow[E]) {
		s.runners = append(s.runners, runner)
	}
}

func WithStateInterval[E storage.Entry](d time.Duration) Option[E] {
	return func(f *flow[E]) {
		f.stateInterval = d
	}
}

type flow[E storage.Entry] struct {
	source        storage.Source[E]
	refreshOutput *output.Refresh
	runners       []exec.Runner[E]
	stateInterval time.Duration
}

func RunFlow[E storage.Entry](opts ...Option[E]) error {
	d := timing.NewDuration()
	d.Start()

	f := &flow[E]{}
	for _, o := range opts {
		o(f)
	}

	err := f.check()
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}

	err = f.run()
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}

	d.StringStartToStop()

	return nil
}

func (f *flow[E]) check() error {
	if f.source == nil {
		return errors.New("source not found")
	}

	if len(f.runners) <= 0 {
		return errors.New("runner not found")
	}

	return nil
}

func (f *flow[E]) run() error {
	defer f.close()

	f.refreshOutput = output.NewRefresh(
		output.WithInterval(f.stateInterval),
		output.WithContent(func() ([]string, error) {
			return f.state(), nil
		}),
	)

	err := f.source.Scan()
	if err != nil {
		return fmt.Errorf("source.Scan: %w", err)
	}

	err = f.runnersPrepare()
	if err != nil {
		return fmt.Errorf("runnersPrepare; %w", err)
	}

	f.summary()

	err = f.runnersRun()
	if err != nil {
		return fmt.Errorf("runnersRun; %w", err)
	}

	f.refreshOutput.Start()

	f.transport()

	return f.finish()
}

func (f *flow[E]) transport() {
	needCopy := false
	if len(f.runners) > 1 {
		needCopy = true
	}

	go func() {
		for {
			items, ok := <-f.source.ReceiveChan()
			if !ok {
				break
			}

			for _, r := range f.runners {
				if needCopy {
					newItems := f.source.Copy(items)
					r.Receive(newItems)
				} else {
					r.Receive(items)
				}
			}
		}

		f.runnersDone()
	}()
}

func (f *flow[E]) finish() error {
	err := f.runnersFinish()
	if err != nil {
		return fmt.Errorf("runnersFinish: %w", err)
	}

	err = f.source.Error()
	if err != nil {
		return fmt.Errorf("source.Error: %w", err)
	}

	return nil
}

func (f *flow[E]) close() {
	defer func() {
		f.refreshOutput.Stop()
		f.runnersOutput()
	}()

	err := f.source.Close()
	if err != nil {
		f.refreshOutput.PrintNext(fmt.Errorf("source.Close: %w", err).Error())
	}

	err = f.runnersClose()
	if err != nil {
		f.refreshOutput.PrintNext(fmt.Errorf("runnersClose: %w", err).Error())
	}
}

func (f *flow[E]) summary() {
	lines := f.source.Summary()
	lines = append(lines, "Runners: ")
	for _, a := range f.runners {
		lines = append(lines, "  "+a.Summary())
	}

	for _, s := range lines {
		fmt.Println(s)
	}

	fmt.Println("")
}

func (f *flow[E]) state() []string {
	sourceLines := f.source.State()
	lines := make([]string, len(sourceLines))
	copy(lines, sourceLines)

	for _, r := range f.runners {
		lines = append(lines, r.Summary())
		for _, s := range r.State() {
			lines = append(lines, "  "+s)
		}
	}

	return lines
}

func (f *flow[E]) runnersOutput() {
	fmt.Println("\nOutput: ")

	for _, r := range f.runners {
		for _, s := range r.Output() {
			fmt.Println(s)
		}

		fmt.Println()
	}
}

func (f *flow[E]) runnersPrepare() error {
	for _, r := range f.runners {
		if err := r.Prepare(); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}

	return nil
}

func (f *flow[E]) runnersRun() error {
	for _, r := range f.runners {
		if err := r.Run(); err != nil {
			return fmt.Errorf("run: %w", err)
		}
	}

	return nil
}

func (f *flow[E]) runnersDone() {
	for _, r := range f.runners {
		r.Done()
	}
}

func (f *flow[E]) runnersFinish() error {
	for _, r := range f.runners {
		if err := r.Finish(); err != nil {
			return fmt.Errorf("finish: %w", err)
		}
	}

	return nil
}

func (f *flow[E]) runnersClose() error {
	for _, r := range f.runners {
		if err := r.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	return nil
}
