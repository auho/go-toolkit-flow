package source

import (
	"testing"
)

func TestString(t *testing.T) {
	_testMock[string](t, func(config Config) *Mock[string] {
		return NewString(config)
	})
}