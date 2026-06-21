package consumer

import (
	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/storage"
)

type Processor = processor.BaseProcessor

// DestinationHolder is a re-export of storage.DestinationHolder for convenience
// so consumers can compose consumer types without importing storage separately.
type DestinationHolder[DE storage.Entry] = storage.DestinationHolder[DE]
