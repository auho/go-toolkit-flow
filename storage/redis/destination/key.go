package destination

import (
	"context"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ storage.Destination[storage.MapEntry] = (*key[storage.MapEntry])(nil)

type keyWriter[E storage.Entry] interface {
	redis.KeyOperator
	accept(itemsChan <-chan []E, c *client.Redis, key string, pageSize int64) error
	stateAmount() int64
}

type key[E storage.Entry] struct {
	storage.Storage
	concurrency int
	isTruncate  bool
	pageSize    int64
	keyName     string
	isDone      bool
	itemsChan   chan []E
	workerWg    sync.WaitGroup
	client      *client.Redis
	handler     keyWriter[E]
	state       *storage.State
	errChan     chan error
	firstErr    error
}

func newKey[E storage.Entry](config Config, handler keyWriter[E]) (*key[E], error) {
	k := &key[E]{}
	k.handler = handler
	err := k.config(config)
	if err != nil {
		return nil, err
	}

	return k, nil
}

func (k *key[E]) GetClient() *client.Redis {
	return k.client
}

func (k *key[E]) Accept() error {
	k.state.MarkAsAccepted()
	k.state.DurationStart()

	if k.isTruncate {
		_, err := k.handler.Truncate(context.Background(), k.client, k.keyName)
		if err != nil {
			return err
		}
	}

	k.itemsChan = make(chan []E, k.concurrency)
	k.errChan = make(chan error, k.concurrency)

	for i := 0; i < k.concurrency; i++ {
		k.workerWg.Add(1)
		go func() {
			if err := k.handler.accept(k.itemsChan, k.client, k.keyName, k.pageSize); err != nil {
				k.errChan <- err
			}

			k.workerWg.Done()
		}()
	}

	return nil
}

func (k *key[E]) Receive(items []E) error {
	k.itemsChan <- items
	return nil
}

func (k *key[E]) Done() {
	k.state.MarkAsDone()

	if k.isDone {
		return
	}

	k.isDone = true

	close(k.itemsChan)
}

func (k *key[E]) Finish() error {
	k.workerWg.Wait()
	close(k.errChan)

	for err := range k.errChan {
		if k.firstErr == nil {
			k.firstErr = err
		}
	}

	k.state.MarkAsFinished()
	k.state.DurationStop()

	return k.firstErr
}

func (k *key[E]) Err() error {
	return k.firstErr
}

func (k *key[E]) Summary() []string {
	return []string{fmt.Sprintf("%s Concurrency:%d; page size:%d", k.Title(), k.concurrency, k.pageSize)}
}

func (k *key[E]) State() []string {
	k.state.SetAmount(k.handler.stateAmount())
	return []string{k.state.Overview()}
}

func (k *key[E]) Title() string {
	return fmt.Sprintf("Destination redis[%s] %s", k.handler.Type(), k.keyName)
}

func (k *key[E]) Close() error {
	return k.client.Close()
}

func (k *key[E]) config(config Config) error {
	k.isTruncate = config.IsTruncate
	k.concurrency = config.Concurrency
	k.pageSize = config.PageSize
	k.keyName = config.Key

	if k.concurrency <= 0 {
		k.concurrency = 1
	}

	if k.pageSize <= 0 {
		k.pageSize = 20
	}

	if k.keyName == "" {
		k.LogFatalWithTitle("key name is empty")
	}

	var err error
	k.client, err = client.NewRedisClient(config.Options)
	if err != nil {
		return err
	}

	k.state = storage.NewState()
	k.state.Title = k.Title()
	k.state.MarkAsConfigured()

	return nil
}
