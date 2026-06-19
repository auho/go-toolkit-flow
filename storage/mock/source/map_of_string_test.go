package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestMapOfString(t *testing.T) {
	_testMock[storage.MapOfStringsEntry](t, func(config Config) *Mock[storage.MapOfStringsEntry] {
		return NewMapOfString(config)
	})
}