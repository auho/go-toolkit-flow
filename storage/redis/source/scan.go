package source

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ storage.Source[string] = (*scanKey)(nil)
var _ redis.Rediser = (*scanKey)(nil)

type scanKey struct {
	storage.Storage
	concurrency int
	pageSize    int64
	total       int64
	amount      int64
	keyPattern  string
	state       *storage.State
	client      *client.Redis
	itemsChan   chan []string
}

func NewScan(config Config) (*scanKey, error) {
	s := &scanKey{}
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

	if config.Options == nil {
		s.LogFatalWithTitle("config options is nil")
	}

	s.state = storage.NewState()
	s.state.MarkAsConfigured()
	s.state.Concurrency = s.concurrency
	s.state.Title = s.Title()

	var err error
	s.client, err = client.NewRedisClient(config.Options)
	if err != nil {
		return err
	}

	return nil
}

func (s *scanKey) GetClient() *client.Redis {
	return s.client
}

func (s *scanKey) Scan() error {
	s.state.MarkAsScanning()
	s.state.DurationStart()
	s.itemsChan = make(chan []string, s.concurrency)

	go func() {
		var err error
		var cursor uint64 = 0
		var keys []string
		for {
			keys, cursor, err = s.client.Scan(context.Background(), cursor, s.keyPattern, s.pageSize).Result()
			if err != nil {
				s.LogFatal(err)
			}

			if len(keys) > 0 {
				s.amount += int64(len(keys))
				s.itemsChan <- keys
			}

			if cursor == 0 {
				break
			}

			if s.total > 0 && s.amount >= s.total {
				break
			}
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
	return s.client.Close()
}

func (s *scanKey) Summary() []string {
	return []string{fmt.Sprintf("%s:", s.Title())}
}

func (s *scanKey) State() []string {
	s.state.SetAmount(s.amount)
	return []string{s.state.Overview()}
}

func (s *scanKey) Copy(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)

	return newItems
}

func (s *scanKey) Title() string {
	return fmt.Sprintf("Source redis[scan keys]:[%s]", s.keyPattern)
}
