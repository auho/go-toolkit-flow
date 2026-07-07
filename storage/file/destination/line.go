package destination

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"golang.org/x/sync/errgroup"
)

var _ storage.Destination[string] = (*Line)(nil)

type Line struct {
	isDone atomic.Bool
	f      *os.File
	b      *bufio.Writer
	state  *storage.Snapshot

	itemsChan  chan []string
	writeGroup *errgroup.Group
	writeCtx   context.Context
}

func NewLine(c Config) (*Line, error) {
	f, err := os.Create(c.Name)
	if err != nil {
		return nil, err
	}

	d := &Line{
		f:     f,
		b:     bufio.NewWriter(f),
		state: storage.NewSnapshot(),
	}

	d.state.SetTitle(d.title())
	d.state.MarkAsConfigured()

	return d, nil
}

func (l *Line) Prepare(ctx context.Context) error {
	l.state.MarkAsPrepare()

	l.itemsChan = make(chan []string, 1)
	l.writeGroup, l.writeCtx = errgroup.WithContext(ctx)

	return nil
}

func (l *Line) Accept() {
	l.state.MarkAsAccepted()
	l.state.DurationStart()

	l.writeGroup.Go(func() error {
		return l.write()
	})
}

func (l *Line) Receive(items []string) error {
	select {
	case <-l.writeCtx.Done():
		return fmt.Errorf("receive: writeCtx cancelled: %w", l.writeCtx.Err())
	case l.itemsChan <- items:
	}
	return nil
}

func (l *Line) Done() {
	if !l.isDone.CompareAndSwap(false, true) {
		return
	}

	l.state.MarkAsDone()

	close(l.itemsChan)
}

func (l *Line) Finish() error {
	err := l.writeGroup.Wait()

	if ferr := l.b.Flush(); ferr != nil && err == nil {
		err = fmt.Errorf("flush: %w", ferr)
	}

	l.state.DurationStop()
	l.state.MarkAsFinished()

	return err
}

func (l *Line) write() error {
loop:
	for {
		select {
		case <-l.writeCtx.Done():
			break loop
		case items, ok := <-l.itemsChan:
			if !ok {
				break loop
			}

			for _, item := range items {
				l.state.AddAmount(1)
				_, err := l.b.WriteString(item + "\n")
				if err != nil {
					return fmt.Errorf("WriteString: %w", err)
				}
			}
		}
	}

	return nil
}

func (l *Line) Close() error {
	if err := l.b.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return l.f.Close()
}

func (l *Line) Summary() []string {
	return []string{l.title()}
}

func (l *Line) State() storage.State {
	return l.state
}

func (l *Line) StateString() []string {
	return []string{l.state.Overview()}
}

func (l *Line) title() string {
	return fmt.Sprintf("Destination file[%s]", l.f.Name())
}
