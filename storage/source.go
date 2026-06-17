package storage

import "context"

type Source[E Entry] interface {
	Prepare(ctx context.Context) error
	Scan()
	ReceiveChan() <-chan []E
	Finish() error
	Close() error
	Summary() []string
	State() []string
	Copy([]E) []E
}
