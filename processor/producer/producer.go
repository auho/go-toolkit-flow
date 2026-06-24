package producer

import (
	"github.com/auho/go-toolkit-flow/v3/processor"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

type Processor = processor.BaseProcessor

// DestinationHolder is a re-export of storage.DestinationHolder for convenience
// so producers can compose producer types without importing storage separately.
type DestinationHolder[DE storage.Entry] = storage.DestinationHolder[DE]

// AfterBatcher is a re-export of processor.AfterBatcher for convenience
// so producers can compose this optional capability without importing processor separately.
// Producer-path instances are parameterized by DE (processing produced data).
type AfterBatcher[DE storage.Entry] = processor.AfterBatcher[DE]
