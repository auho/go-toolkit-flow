package task

import "github.com/auho/go-toolkit-flow/storage"

type Batch[E storage.Entry] interface {
	Task[E]

	// Do need to be implemented
	// effected
	Do([]E) (int, error)
}
