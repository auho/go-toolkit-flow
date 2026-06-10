package source

import (
	"testing"
)

func TestSliceString(t *testing.T) {
	_testMock[string](t, func(config Config) *Mock[string] {
		return NewSliceString(config)
	})
}
