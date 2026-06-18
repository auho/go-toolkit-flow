package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
	"golang.org/x/sync/errgroup"
)

type Option[SE, DE storage.Entry] func(*flow[SE, DE])

func WithSource[SE, DE storage.Entry](se storage.Source[SE]) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.source = se
	}
}

func WithDestination[SE, DE storage.Entry](d storage.Destination[DE]) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.destination = d
	}
}

func WithRunner[SE, DE storage.Entry](rs ...exec.Runner[SE, DE]) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.runners.Add(rs...)
	}
}

func WithStateInterval[SE, DE storage.Entry](d time.Duration) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.stateInterval = d
	}
}

type flow[SE, DE storage.Entry] struct {
	source        storage.Source[SE]
	destination   storage.Destination[DE]
	refreshOutput *output.Refresh
	runners       *exec.Runners[SE, DE]
	stateInterval time.Duration
}

func RunFlow[SE, DE storage.Entry](opts ...Option[SE, DE]) error {
	d := timing.NewDuration()
	d.Start()

	f := &flow[SE, DE]{
		runners:     exec.NewRunners[SE, DE](),
		destination: storage.NoopDestination[DE]{},
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

func (f *flow[SE, DE]) check() error {
	if f.source == nil {
		return errors.New("source not found")
	}

	if f.runners.Len() <= 0 {
		return errors.New("runner not found")
	}

	return nil
}

func (f *flow[SE, DE]) run() error {
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
		return fmt.Errorf("runners.Prepare: %w", err)
	}

	err = f.destination.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("destination.Prepare: %w", err)
	}

	f.summary()

	// 异步启动
	f.source.Scan()
	f.runners.Start()
	f.destination.Accept()

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
			return fmt.Errorf("runners.Finish: %w", err1)
		}

		return nil
	})

	g.Go(func() error {
		err1 := f.outputForward(ctx)
		if err1 != nil {
			return fmt.Errorf("outputForward: %w", err1)
		}

		return nil
	})

	// 等待全部退出
	err = g.Wait()
	if err != nil {
		return err
	}

	err = f.destination.Finish()
	if err != nil {
		return fmt.Errorf("destination.Finish: %w", err)
	}

	return nil
}

func (f *flow[SE, DE]) transport(ctx context.Context) {
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

// outputForward fans in data from all runners' OutChan and forwards it to the destination.
// In the consumer path, OutChan carries no data (out is nil), so this drains and calls
// destination.Receive zero times; NoopDestination.Receive is a no-op anyway.
// In the producer path, produced data is forwarded to the destination for persistence.
func (f *flow[SE, DE]) outputForward(ctx context.Context) error {
	merged := make(chan []DE)
	var wg sync.WaitGroup
	for _, r := range f.runners.All() {
		wg.Add(1)
		go func(r exec.Runner[SE, DE]) {
			defer wg.Done()
			for out := range r.OutChan() {
				select {
				case <-ctx.Done():
					return
				case merged <- out:
				}
			}
		}(r)
	}
	go func() { wg.Wait(); close(merged) }()

	for out := range merged {
		if err := f.destination.Receive(out); err != nil {
			return err
		}
	}
	f.destination.Done()
	return nil
}

func (f *flow[SE, DE]) close() {
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
		f.refreshOutput.PrintNext(fmt.Errorf("runners.Close: %w", err).Error())
	}

	err = f.destination.Close()
	if err != nil {
		f.refreshOutput.PrintNext(fmt.Errorf("destination.Close: %w", err).Error())
	}
}

func (f *flow[SE, DE]) summary() {
	lines := f.source.Summary()
	lines = append(lines, "Runners: ")
	for _, s := range f.runners.Summary() {
		lines = append(lines, "  "+s)
	}
	lines = append(lines, "Destination: ")
	lines = append(lines, f.destination.Summary()...)

	for _, s := range lines {
		fmt.Println(s)
	}

	fmt.Println("")
}

func (f *flow[SE, DE]) state() []string {
	sourceLines := f.source.State()
	lines := make([]string, len(sourceLines))
	copy(lines, sourceLines)

	lines = append(lines, f.runners.State()...)
	lines = append(lines, f.destination.State()...)

	return lines
}

func (f *flow[SE, DE]) runnersOutput() {
	fmt.Println("\nOutput: ")

	for _, s := range f.runners.Output() {
		fmt.Println(s)
	}

	fmt.Println()
}
