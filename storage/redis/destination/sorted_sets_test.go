package destination

import (
	"strconv"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

var _sortedSetsKey = "test:destination:sortedSets"

func _buildSortedSetsData(k *key[storage.ScoreMapEntry]) int64 {
	amount := _randAmount()
	size := 100

	for i := 0; i < amount; i += 100 {
		tSize := size

		if i+size >= amount {
			tSize = amount - i
		}

		items := make([]storage.ScoreMapEntry, 0, tSize)
		for j := 0; j < tSize; j++ {
			a := i + j
			items = append(items, storage.ScoreMapEntry{strconv.Itoa(a): float64(a) + 1e-5})
		}

		k.Receive(items)
	}

	return int64(amount)
}

func TestSortedSets(t *testing.T) {
	_testKey[storage.ScoreMapEntry](t, _sortedSetsKey, NewSortedSetsWithGoRedisV8, _buildSortedSetsData)
}
