package task

import "github.com/auho/go-toolkit-flow/storage"

type Transformer[E storage.Entry] interface {
	Task[E]

	// Do need to be implemented
	Do(E) ([]E, bool)

	// PostBatchDo need to be implemented
	PostBatchDo([]E) error
}
