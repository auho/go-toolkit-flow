package destination

import (
	"strconv"
	"testing"
)

var _setsKey = "test:destination:sets"

func _buildSetsData(k *key[string]) int64 {
	amount := _randAmount()
	size := 100

	for i := 0; i < amount; i += 100 {
		tSize := size

		if i+size >= amount {
			tSize = amount - i
		}

		items := make([]string, 0, tSize)
		for j := 0; j < tSize; j++ {
			a := i + j
			items = append(items, strconv.Itoa(a))
		}

		k.Receive(items)
	}

	return int64(amount)
}

func TestSets(t *testing.T) {
	_testKey[string](t, _setsKey, NewSets, _buildSetsData)
}
