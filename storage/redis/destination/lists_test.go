package destination

import (
	"strconv"
	"testing"
)

var _listsKey = "test:destination:lists"

func _buildListsData(k *key[string]) int64 {
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

func TestLists(t *testing.T) {
	_testKey[string](t, _listsKey, NewLists, _buildListsData)
}
