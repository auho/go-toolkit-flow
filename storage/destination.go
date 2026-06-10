package storage

type Destinationer[E Entry] interface {
	Accept() error
	Receive([]E) error
	Done()
	Finish() error
	Close() error
	Summary() []string
	State() []string
}
