package destination

import (
	"strconv"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

var _hashesKey = "test:destination:hashes"

func _buildHashesData(k *Bulk[storage.MapEntry]) int64 {
	amount := _randAmount()
	size := 100

	for i := 0; i < amount; i += 100 {
		tSize := size

		if i+size >= amount {
			tSize = amount - i
		}

		items := make(storage.MapEntries, 0, tSize)
		for j := 0; j < tSize; j++ {
			a := i + j
			items = append(items, map[string]any{strconv.Itoa(a): a})
		}

		k.Receive(items)
	}

	return int64(amount)
}

func TestHashes(t *testing.T) {
	_testKey[storage.MapEntry](t, _hashesKey, NewHashesWithGoRedisV8, _buildHashesData)
}
