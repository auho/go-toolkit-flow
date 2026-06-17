package destination

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destination[string] = (*Line)(nil)

type Line struct {
	storage.Storage
	isDone bool
	f      *os.File
	b      *bufio.Writer
	state  *storage.State
	wg     sync.WaitGroup
}

func NewLine(c Config) (*Line, error) {
	f, err := os.Create(c.Name)
	if err != nil {
		return nil, err
	}

	d := &Line{
		f:     f,
		b:     bufio.NewWriter(f),
		state: storage.NewState(),
	}

	d.state.Title = d.Title()

	return d, nil
}

func (l *Line) Prepare(ctx context.Context) error {
	l.state.MarkAsPrepare()

	return nil
}

func (l *Line) Accept() {
	l.state.MarkAsAccepted()
	l.state.DurationStart()
	l.wg.Add(1)
}

func (l *Line) Receive(items []string) error {
	for k := range items {
		l.state.AddAmount(1)
		_, err := l.b.WriteString(items[k] + "\n")
		if err != nil {
			return fmt.Errorf("file destination receive: %w", err)
		}
	}

	err := l.b.Flush()
	if err != nil {
		return fmt.Errorf("file destination receive: %w", err)
	}

	return nil
}

func (l *Line) Done() {
	l.state.MarkAsDone()

	if l.isDone {
		return
	}

	l.isDone = true
	l.wg.Done()
}

func (l *Line) Finish() error {
	l.wg.Wait()

	l.state.DurationStop()
	l.state.MarkAsFinished()

	return nil
}

func (l *Line) Close() error {
	return l.f.Close()
}

func (l *Line) Summary() []string {
	return []string{l.Title()}
}

func (l *Line) State() []string {
	return []string{l.state.Overview()}
}

func (l *Line) Title() string {
	return fmt.Sprintf("Line file[%s]", l.f.Name())
}
