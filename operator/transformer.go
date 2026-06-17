package operator

import "github.com/auho/go-toolkit-flow/storage"

type Transformer[E storage.Entry] interface {
	Operator[E]

	// Exec need to be implemented
	Exec(E) ([]E, bool, error)

	// PostBatchExec need to be implemented
	PostBatchExec([]E) error
}
