// Package format defines data format interfaces for generating and deep-copying
// mock data produced by the source package.
package format

import "github.com/auho/go-toolkit-flow/v3/storage"

// Format is the data format interface for the mock source.
// It describes the scan type, generates items in batches, and provides
// deep-copy semantics, mirroring the format pattern used by database and
// redis sources.
type Format[E storage.Entry] interface {
	// Type returns a short identifier for the format (e.g. "sliceMap").
	Type() string

	// Scan generates a batch of items.
	// idName: the name of the ID field; id: pointer to the current ID counter;
	// amount: number of items to generate in this batch.
	// Returns the updated id pointer and the generated items.
	Scan(idName string, id *int64, amount int64) (*int64, []E)

	// Copy deep-copies the given items.
	Copy(items []E) []E
}
