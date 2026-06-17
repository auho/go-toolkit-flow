package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
	"golang.org/x/sync/errgroup"
)

type Option[E storage.Entry] func(*flow[E])

func WithSource[E storage.Entry](se storage.Source[E]) Option[E] {
	return func(s *flow[E]) {
		s.source = se
	}
}

func WithRunner[E storage.Entry](rs ...exec.Runner[E]) Option[E] {
	return func(s *flow[E]) {
		s.runners.Add(rs...)
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
	runners       *exec.Runners[E]
	stateInterval time.Duration
}

func RunFlow[E storage.Entry](opts ...Option[E]) error {
	d := timing.NewDuration()
	d.Start()

	f := &flow[E]{
		runners: exec.NewRunners[E](),
	}
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

	if f.runners.Len() <= 0 {
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

	// 创建全局 context errgroup
	g, ctx := errgroup.WithContext(context.Background())

	// Prepare 阶段（同步，可能出错，传入 ctx 创建 errgroup）
	err := f.source.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("source.Prepare: %w", err)
	}

	err = f.runners.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("runnersPrepare: %w", err)
	}

	f.summary()

	// 异步启动
	f.source.Scan()
	f.runners.Run()

	f.refreshOutput.Start()

	// errgroup 协调并发
	g.Go(func() error {
		err1 := f.source.Finish()
		if err1 != nil {
			return fmt.Errorf("source.Finish: %w", err1)
		}

		return nil
	})

	g.Go(func() error {
		f.transport(ctx)
		return nil
	})

	g.Go(func() error {
		err1 := f.runners.Finish()
		if err1 != nil {
			return fmt.Errorf("runnersFinish: %w", err1)
		}

		return nil
	})

	// 等待全部退出
	return g.Wait()
}

func (f *flow[E]) transport(ctx context.Context) {
	needCopy := f.runners.Len() > 1

	for {
		select {
		case items, ok := <-f.source.ReceiveChan():
			if !ok {
				f.runners.Done()
				return
			}
			if needCopy {
				for _, r := range f.runners.All() {
					newItems := f.source.Copy(items)
					r.Receive(newItems)
				}
			} else {
				f.runners.Receive(items)
			}
		case <-ctx.Done():
			f.runners.Done()
			return
		}
	}
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

	err = f.runners.Close()
	if err != nil {
		f.refreshOutput.PrintNext(fmt.Errorf("runnersClose: %w", err).Error())
	}
}

func (f *flow[E]) summary() {
	lines := f.source.Summary()
	lines = append(lines, "Runners: ")
	for _, s := range f.runners.Summary() {
		lines = append(lines, "  "+s)
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

	lines = append(lines, f.runners.State()...)

	return lines
}

func (f *flow[E]) runnersOutput() {
	fmt.Println("\nOutput: ")

	for _, s := range f.runners.Output() {
		fmt.Println(s)
	}

	fmt.Println()
}
