package destination

import (
	"strconv"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

var _sortedSetsKey = "test:destination:sortedSets"

func _buildSortedSetsData(k *key[storage.ScoreMap]) int64 {
	amount := _randAmount()
	size := 100

	for i := 0; i < amount; i += 100 {
		tSize := size

		if i+size >= amount {
			tSize = amount - i
		}

		items := make([]storage.ScoreMap, 0, tSize)
		for j := 0; j < tSize; j++ {
			a := i + j
			items = append(items, storage.ScoreMap{strconv.Itoa(a): float64(a) + 1e-5})
		}

		k.Receive(items)
	}

	return int64(amount)
}

func TestSortedSets(t *testing.T) {
	_testKey[storage.ScoreMap](t, _sortedSetsKey, NewSortedSets, _buildSortedSetsData)
}
