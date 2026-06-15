package operator

import "github.com/auho/go-toolkit-flow/storage"

type Transformer[E storage.Entry] interface {
	Operator[E]

	// Do need to be implemented
	Do(E) ([]E, bool)

	// PostBatchDo need to be implemented
	PostBatchDo([]E) error
}
