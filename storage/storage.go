package storage

import "log"

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

type Entry interface {
	SliceEntry | SliceOfStringsEntry | MapEntry | MapOfStringsEntry | ScoreMapEntry | string
}

type Storage struct {
}

func (s *Storage) Title() string {
	return ""
}

func (s *Storage) LogFatalWithTitle(v ...any) {
	log.Fatal(append([]any{s.Title()}, v...)...)
}

func (s *Storage) LogFatal(v ...any) {
	log.Fatal(v...)
}
