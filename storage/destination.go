package storage

type Destinationer[E Entry] interface {
	Accept() error
	Receive([]E)
	Done()
	Finish()
	Close() error
	Summary() []string
	State() []string
}
