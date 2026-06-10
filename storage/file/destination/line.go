package destination

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destinationer[string] = (*Line)(nil)

type Line struct {
	storage.Storage
	isDone bool
	f      *os.File
	b      *bufio.Writer
	state  *storage.State
	sw     sync.WaitGroup
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

func (l *Line) Accept() error {
	l.state.StatusAccept()
	l.state.DurationStart()
	l.sw.Add(1)

	return nil
}

func (l *Line) Receive(items []string) {
	for k := range items {
		l.state.AddAmount(1)
		_, err := l.b.WriteString(items[k] + "\n")
		if err != nil {
			panic(err)
		}
	}

	err := l.b.Flush()
	if err != nil {
		panic(err)
	}
}

func (l *Line) Done() {
	l.state.StatusDone()

	if l.isDone {
		return
	}

	l.isDone = true
	l.sw.Done()
}

func (l *Line) Finish() {
	l.sw.Wait()

	l.state.DurationStop()
	l.state.StatusFinish()
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
