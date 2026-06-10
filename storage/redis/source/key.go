package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit/redis/client"
)

var _ storage.Source[storage.MapEntry] = (*key[storage.MapEntry])(nil)
var _ redis.ClientProvider = (*key[storage.MapEntry])(nil)

type keyScanner[E storage.Entry] interface {
	// redis key type
	keyType() redis.KeyType
	// redis client, key name
	len(redisClient *client.Redis, keyName string) (int64, error)
	//chan []E, redis client, key name, amount, page size
	scan(itemsChan chan<- []E, redisClient *client.Redis, keyName string, amount int64, pageSize int64)
	// duplicate items
	duplicate([]E) []E
	stateAmount() int64
}

type key[E storage.Entry] struct {
	storage.Storage
	concurrency int
	pageSize    int64
	amount      int64
	total       int64
	keyName     string
	state       *storage.TotalState
	client      *client.Redis
	handler     keyScanner[E]
	itemsChan   chan []E
}

func newKey[E storage.Entry](config Config, handler keyScanner[E]) (*key[E], error) {
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

func (k *key[E]) config(config Config) error {
	k.concurrency = config.Concurrency
	k.pageSize = config.PageSize
	k.keyName = config.Key
	k.amount = config.Amount

	if k.concurrency <= 0 {
		k.concurrency = 1
	}

	if k.pageSize <= 0 {
		k.pageSize = 100
	}

	if k.keyName == "" {
		k.LogFatalWithTitle("key name is empty")
	}

	if config.Options == nil {
		k.LogFatalWithTitle("config options is nil")
	}

	k.state = storage.NewTotalState()
	k.state.MarkAsConfigured()
	k.state.Concurrency = k.concurrency
	k.state.Title = k.Title()

	var err error
	k.client, err = client.NewRedisClient(config.Options)
	if err != nil {
		return err
	}

	return nil
}

func (k *key[E]) Scan() error {
	k.state.MarkAsScanning()
	k.state.DurationStart()
	k.itemsChan = make(chan []E, k.concurrency)

	var err error
	k.total, err = k.handler.len(k.client, k.keyName)
	if err != nil {
		return err
	}

	if k.amount > 0 && k.total >= k.amount {
		k.total = k.amount
	}

	k.state.Total = k.total

	go func() {
		k.handler.scan(k.itemsChan, k.client, k.keyName, k.total, k.pageSize)

		close(k.itemsChan)

		k.state.DurationStop()
		k.state.MarkAsFinished()
	}()

	return nil
}

func (k *key[E]) ReceiveChan() <-chan []E {
	return k.itemsChan
}

func (k *key[E]) Summary() []string {
	return []string{fmt.Sprintf("%s: total: %d", k.Title(), k.total)}
}

func (k *key[E]) State() []string {
	k.state.SetAmount(k.handler.stateAmount())
	return []string{k.state.Overview()}
}

func (k *key[E]) Copy(items []E) []E {
	return k.handler.duplicate(items)
}

func (k *key[E]) Title() string {
	return fmt.Sprintf("Source redis[%s]:%s", k.handler.keyType(), k.keyName)
}

func (k *key[E]) Close() error {
	return k.client.Close()
}
