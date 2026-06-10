package storage

import (
	"errors"
)

var EOF = errors.New("EOF")

type Sourceor[E Entry] interface {
	Scan() error
	ReceiveChan() <-chan []E
	Close() error
	Summary() []string
	State() []string
	Copy([]E) []E
}
