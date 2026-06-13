package source

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
)

var _ storage.Source[string] = (*scanKey)(nil)

type scanKey struct {
	storage.Storage
	dialect     dialect.Dialect
	format      format.Format[string]
	concurrency int
	pageSize    int64
	total       int64
	amount      int64
	keyPattern  string
	state       *storage.State
	itemsChan   chan []string
	scanned     int64
}

func newScanKey(config Config, d dialect.Dialect, f format.Format[string]) (*scanKey, error) {
	s := &scanKey{}
	s.dialect = d
	s.format = f
	err := s.config(config)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *scanKey) config(config Config) error {
	s.concurrency = config.Concurrency
	s.pageSize = config.PageSize
	s.keyPattern = config.Key
	s.total = config.Amount

	if s.concurrency <= 0 {
		s.concurrency = 1
	}

	if s.pageSize <= 0 {
		s.pageSize = 100
	}

	s.state = storage.NewState()
	s.state.MarkAsConfigured()
	s.state.Concurrency = s.concurrency
	s.state.Title = s.Title()

	return nil
}

func (s *scanKey) Scan() error {
	s.state.MarkAsScanning()
	s.state.DurationStart()
	s.itemsChan = make(chan []string, s.concurrency)

	go func() {
		var cursor int64 = 0
		for {
			keys, newCursor, err := s.format.ScanByRange(s.dialect, s.keyPattern, cursor, s.pageSize)
			if err != nil {
				panic(fmt.Sprintf("scan keys: %v", err))
			}

			if len(keys) > 0 {
				atomic.AddInt64(&s.scanned, int64(len(keys)))
				s.itemsChan <- keys
			}

			if newCursor == 0 {
				break
			}

			if s.total > 0 && atomic.LoadInt64(&s.scanned) >= s.total {
				break
			}

			cursor = newCursor
		}

		close(s.itemsChan)

		s.state.DurationStop()
		s.state.MarkAsFinished()
	}()

	return nil
}

func (s *scanKey) ReceiveChan() <-chan []string {
	return s.itemsChan
}

func (s *scanKey) Close() error {
	return s.dialect.Close()
}

func (s *scanKey) Summary() []string {
	return []string{fmt.Sprintf("%s:", s.Title())}
}

func (s *scanKey) State() []string {
	s.state.SetAmount(atomic.LoadInt64(&s.scanned))
	return []string{s.state.Overview()}
}

func (s *scanKey) Copy(items []string) []string {
	return s.format.Copy(items)
}

func (s *scanKey) Title() string {
	return fmt.Sprintf("Source redis[scan keys]:[%s]", s.keyPattern)
}
