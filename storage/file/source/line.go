package source

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
)

var _ storage.Source[string] = (*Line)(nil)

type Line struct {
	config    Config
	file      *os.File
	scanner   *bufio.Scanner
	state     *storage.Snapshot
	itemsChan chan []string
	scanCtx   context.Context
	scanWg    sync.WaitGroup
	scanErr   error
}

func NewLine(c Config) (*Line, error) {
	l := &Line{
		config: c,
	}

	if err := l.init(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Line) init() error {
	if err := l.config.Check(); err != nil {
		return fmt.Errorf("config.Check: %w", err)
	}

	var err error
	l.file, err = os.Open(l.config.Name)
	if err != nil {
		return err
	}

	l.scanner = bufio.NewScanner(l.file)
	l.state = storage.NewSnapshot()
	l.state.MarkAsConfigured()

	return nil
}

func (l *Line) Prepare(ctx context.Context) error {
	l.state.MarkAsPrepare()
	l.state.SetTitle(l.title())
	l.scanCtx = ctx
	l.itemsChan = make(chan []string, l.config.Concurrency)

	return nil
}

func (l *Line) send(items []string) bool {
	select {
	case l.itemsChan <- items:
		return true
	case <-l.scanCtx.Done():
		return false
	}
}

func (l *Line) Scan() {
	l.state.MarkAsScanning()
	l.state.DurationStart()

	l.scanWg.Go(func() {
		items := make([]string, 0, l.config.PageSize)
		for l.scanner.Scan() {
			items = append(items, l.scanner.Text())
			l.state.AddAmount(1)
			if len(items) >= l.config.PageSize {
				if !l.send(items) {
					return
				}
				items = make([]string, 0, l.config.PageSize)
			}
		}

		if len(items) > 0 {
			if !l.send(items) {
				return
			}
		}

		if err := l.scanner.Err(); err != nil {
			l.scanErr = fmt.Errorf("scanner: %w", err)
			return
		}
	})
}

func (l *Line) ReceiveChan() <-chan []string {
	return l.itemsChan
}

func (l *Line) Finish() error {
	l.scanWg.Wait()

	close(l.itemsChan)
	l.state.DurationStop()
	l.state.MarkAsFinished()

	return l.scanErr
}

func (l *Line) Close() error {
	return l.file.Close()
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

func (l *Line) Copy(items []string) []string {
	return tool.CopySliceString(items)
}

func (l *Line) title() string {
	return fmt.Sprintf("Source file[%s]", l.file.Name())
}
