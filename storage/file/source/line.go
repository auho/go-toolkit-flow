package source

import (
	"bufio"
	"fmt"
	"os"
	"runtime"

	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Source[string] = (*Line)(nil)

type Line struct {
	storage.Storage
	c         Config
	f         *os.File
	fi        os.FileInfo
	b         *bufio.Scanner
	state     *storage.State
	itemsChan chan []string
}

func NewLine(c Config) (*Line, error) {
	var err error
	l := &Line{
		c: c,
	}

	l.f, err = os.Open(l.c.Name)
	if err != nil {
		return nil, err
	}

	l.fi, err = l.f.Stat()
	if err != nil {
		return nil, err
	}

	l.b = bufio.NewScanner(l.f)
	l.state = storage.NewState()

	if l.c.Concurrency <= 0 {
		l.c.Concurrency = runtime.NumCPU()
	}

	if l.c.Line <= 0 {
		l.c.Line = 100
	}

	return l, nil
}

func (l *Line) Scan() error {
	l.state.MarkAsScanning()
	l.state.DurationStart()
	l.state.Title = l.Title()
	l.itemsChan = make(chan []string, l.c.Concurrency)

	go func() error {
		items := make([]string, 0, l.c.Line)
		i := 1
		for l.b.Scan() {
			if i%l.c.Line == 0 {
				l.itemsChan <- items
				items = make([]string, 0, l.c.Line)
			}

			items = append(items, l.b.Text())
			l.state.AddAmount(1)
			i++
		}

		if len(items) > 0 {
			l.itemsChan <- items
		}

		err := l.b.Err()
		if err != nil {
			close(l.itemsChan)
			return fmt.Errorf("file source scan error; %w", err)
		}

		close(l.itemsChan)
		l.state.MarkAsFinished()
		l.state.DurationStop()
		return nil
	}()

	return nil
}

func (l *Line) ReceiveChan() <-chan []string {
	return l.itemsChan
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

func (l *Line) Copy(items []string) []string {
	ns := make([]string, 0, len(items))
	copy(ns, items)
	return ns
}

func (l *Line) Title() string {
	return fmt.Sprintf("Source file[%s]\n", l.f.Name())
}
