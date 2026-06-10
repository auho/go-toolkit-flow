package source

import (
	"fmt"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/storage"
)

// scanKeyValues is a common scan function for Redis key-value scan commands (HScan, ZScan).
// It iterates over a Redis key using the provided scan function, parses results into entries,
// and sends them to the provided channel.
func scanKeyValues[E storage.Entry](
	amount int64,
	count int64,
	amountPtr *int64,
	itemsChan chan<- []E,
	scanFn func(cursor uint64) ([]string, uint64, error),
	scanName string,
	parseFn func(items []string) []E,
) {
	var err error
	var items []string
	cursor := uint64(0)

	for {
		items, cursor, err = scanFn(cursor)
		if err != nil {
			panic(fmt.Sprintf("%s: %v", scanName, err))
		}

		entries := parseFn(items)

		atomic.AddInt64(amountPtr, int64(len(entries)))
		itemsChan <- entries

		if cursor == 0 {
			break
		}

		if atomic.LoadInt64(amountPtr) >= amount {
			break
		}
	}
}

// parseMapOfStringsEntries parses flat key-value string pairs into MapOfStringsEntry slices.
func parseMapOfStringsEntries(items []string) storage.MapOfStringsEntries {
	entries := make(storage.MapOfStringsEntries, 0, len(items)/2)
	for i := 0; i < len(items)-1; i += 2 {
		entries = append(entries, storage.MapOfStringsEntry{items[i]: items[i+1]})
	}
	return entries
}
