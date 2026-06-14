package source

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
	"github.com/auho/go-toolkit-flow/storage/redis/source/format"
)

var _ storage.Source[string] = (*ScanKey)(nil)

type ScanKey struct {
	storage.Storage
	dialect dialect.Dialect
	format  format.Format[string]

	concurrency     int
	pageSize        int64
	total           int64
	amount          int64
	timeOutDuration time.Duration
	keyPattern      string

	state     *storage.State
	itemsChan chan []string
	scanned   int64
}

func newScanKey(config KeyConfig, d dialect.Dialect, f format.Format[string]) (*ScanKey, error) {
	s := &ScanKey{}
	s.dialect = d
	s.format = f
	err := s.config(config)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *ScanKey) config(c KeyConfig) error {
	s.concurrency = c.Concurrency
	s.pageSize = c.PageSize
	s.timeOutDuration = c.GetTimeOutDuration()
	s.keyPattern = c.KeyName
	s.total = c.Amount

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

func (s *ScanKey) Scan() error {
	s.state.MarkAsScanning()
	s.state.DurationStart()
	s.itemsChan = make(chan []string, s.concurrency)

	go func() {
		cancels := make([]context.CancelFunc, 0)
		defer func() {
			for _, cancel := range cancels {
				cancel()
			}
		}()

		var cursor uint64 = 0
		for {
			ctx, cancel := context.WithTimeout(context.Background(), s.timeOutDuration)
			cancels = append(cancels, cancel)

			keys, newCursor, err := s.format.ScanByRange(ctx, s.dialect, s.keyPattern, cursor, s.pageSize)
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

func (s *ScanKey) ReceiveChan() <-chan []string {
	return s.itemsChan
}

func (s *ScanKey) Close() error {
	return s.dialect.Close()
}

func (s *ScanKey) Summary() []string {
	return []string{fmt.Sprintf("%s:", s.Title())}
}

func (s *ScanKey) State() []string {
	s.state.SetAmount(atomic.LoadInt64(&s.scanned))
	return []string{s.state.Overview()}
}

func (s *ScanKey) Copy(items []string) []string {
	return s.format.Copy(items)
}

func (s *ScanKey) Title() string {
	return fmt.Sprintf("Source redis[scan keys]:[%s]", s.keyPattern)
}
