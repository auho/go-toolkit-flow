package source

import (
	"testing"
)

func TestString(t *testing.T) {
	_testMemory[string](t, func(config Config) *Memory[string] {
		return NewString(config)
	})
}