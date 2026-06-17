package storage

import "context"

type Destination[E Entry] interface {
	Prepare(ctx context.Context) error
	Accept()
	Receive([]E) error
	Done()
	Finish() error
	Close() error
	Summary() []string
	State() []string
}
