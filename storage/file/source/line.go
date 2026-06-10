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
	fileInfo  os.FileInfo
	scanner   *bufio.Scanner
	state     *storage.State
	itemsChan chan []string
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

	l.fileInfo, err = l.file.Stat()
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

	go func() error {
		items := make([]string, 0, l.config.Line)
		i := 1
		for l.scanner.Scan() {
			if i%l.config.Line == 0 {
				l.itemsChan <- items
				items = make([]string, 0, l.config.Line)
			}

			items = append(items, l.scanner.Text())
			l.state.AddAmount(1)
			i++
		}

		if len(items) > 0 {
			l.itemsChan <- items
		}

		err := l.scanner.Err()
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
	return l.file.Close()
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
	return fmt.Sprintf("Source file[%s]\n", l.file.Name())
}
