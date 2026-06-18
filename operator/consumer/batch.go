package consumer

import (
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

// Batch is a consumer-path operator that processes items in bulk.
// Path one: source -> operator (no data produced) -> exec flow control.
// Exec does not return data; nothing is sent to a destination.
type Batch[SE storage.Entry] interface {
	operator.Operator[SE]

	// Exec need to be implemented
	// affected
	Exec([]SE) (int64, error)
}
