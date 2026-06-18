package consumer

import (
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

// Item is a consumer-path operator that processes items one by one.
// Path one: source -> operator (no data produced) -> exec flow control.
// Exec does not return data; nothing is sent to a destination.
type Item[SE storage.Entry] interface {
	operator.Operator[SE]

	// Exec need to be implemented
	// ok: whether the item was processed
	Exec(SE) (bool, error)
}
