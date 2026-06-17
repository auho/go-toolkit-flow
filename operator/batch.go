package operator

import "github.com/auho/go-toolkit-flow/storage"

type Batch[E storage.Entry] interface {
	Operator[E]

	// Exec need to be implemented
	// effected
	Exec([]E) (int64, error)
}
