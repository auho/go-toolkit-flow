package task

import "github.com/auho/go-toolkit-flow/storage"

type Singleton[E storage.Entry] interface {
	Tasker[E]

	// Do need to be implemented
	Do(E) ([]E, bool)

	// PostBatchDo need to be implemented
	PostBatchDo([]E)
}
