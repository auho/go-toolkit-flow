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
	config    Config
	file      *os.File
	scanner   *bufio.Scanner
	state     *storage.State
	itemsChan chan []string
	scanErr   error
}

func NewLine(c Config) (*Line, error) {
	var err error
	l := &Line{
		config: c,
	}

	l.file, err = os.Open(l.config.Name)
	if err != nil {
		return nil, err
	}

	l.scanner = bufio.NewScanner(l.file)
	l.state = storage.NewState()

	if l.config.Concurrency <= 0 {
		l.config.Concurrency = runtime.NumCPU()
	}

	if l.config.Line <= 0 {
		l.config.Line = 100
	}

	return l, nil
}

func (l *Line) Scan() error {
	l.state.MarkAsScanning()
	l.state.DurationStart()
	l.state.Title = l.Title()
	l.itemsChan = make(chan []string, l.config.Concurrency)

	go func() {
		defer close(l.itemsChan)

		items := make([]string, 0, l.config.Line)
		for l.scanner.Scan() {
			items = append(items, l.scanner.Text())
			l.state.AddAmount(1)
			if len(items) >= l.config.Line {
				l.itemsChan <- items
				items = make([]string, 0, l.config.Line)
			}
		}

		if len(items) > 0 {
			l.itemsChan <- items
		}

		if err := l.scanner.Err(); err != nil {
			l.scanErr = fmt.Errorf("file source scan error: %w", err)
			return
		}

		l.state.MarkAsFinished()
		l.state.DurationStop()
	}()

	return nil
}

func (l *Line) ReceiveChan() <-chan []string {
	return l.itemsChan
}

func (l *Line) Error() error {
	return l.scanErr
}

func (l *Line) Close() error {
	return l.file.Close()
}

func (l *Line) Summary() []string {
	return []string{l.Title()}
}

func (l *Line) State() []string {
	return []string{l.state.Overview()}
}

func (l *Line) Copy(items []string) []string {
	ns := make([]string, len(items))
	copy(ns, items)
	return ns
}

func (l *Line) Title() string {
	return fmt.Sprintf("Source file[%s]", l.file.Name())
}
