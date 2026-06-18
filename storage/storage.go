// Package storage defines the core data types and contracts for the data
// pipeline. It provides the Entry type constraint, Source and Destination
// interfaces, state tracking, and infrastructure destinations
// (MultiDestination for fan-out, NoopDestination for the consumer path).
package storage

type SliceEntry = []any
type SliceOfStringsEntry = []string

type MapEntry = map[string]any
type MapOfStringsEntry = map[string]string
type ScoreMapEntry = map[any]float64

type SliceEntries = []SliceEntry
type SliceOfStringsEntries = []SliceOfStringsEntry
type MapEntries = []MapEntry
type MapOfStringsEntries = []MapOfStringsEntry
type ScoreMapEntries = []ScoreMapEntry

// Entry is the type constraint for all supported data element types.
// It is used as the generic parameter for Source, Destination, and other
// pipeline components to ensure type safety across the data flow.
type Entry interface {
	SliceEntry | SliceOfStringsEntry | MapEntry | MapOfStringsEntry | ScoreMapEntry | string
}

// Storage is an empty struct embedded by implementations to share package identity.
type Storage struct{}
