package source

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/source/format"
)

// NewMap creates a Mock source that generates MapEntry items.
func NewMap(config Config) *Mock[storage.MapEntry] {
	return NewMock[storage.MapEntry](config, format.NewMapFormat())
}

// NewString creates a Mock source that generates string items.
func NewString(config Config) *Mock[string] {
	return NewMock[string](config, format.NewStringFormat())
}

// NewMapOfString creates a Mock source that generates MapOfStringsEntry items.
func NewMapOfString(config Config) *Mock[storage.MapOfStringsEntry] {
	return NewMock[storage.MapOfStringsEntry](config, format.NewMapOfStringFormat())
}