package storage

import (
	"strings"
)

// MultiSnapshot aggregates multiple State into a single State.
// Used by MultiDestination to present a unified state view.
type MultiSnapshot struct {
	states []State
}

// NewMultiSnapshot creates a MultiSnapshot from the given sub-states.
func NewMultiSnapshot(states []State) *MultiSnapshot {
	return &MultiSnapshot{states: states}
}

func (m *MultiSnapshot) Overview() string {
	var sb strings.Builder
	sb.WriteString(m.Title())
	for _, s := range m.states {
		sb.WriteString("\n  ")
		sb.WriteString(s.Overview())
	}
	return sb.String()
}

func (m *MultiSnapshot) Amount() int64 {
	var sum int64
	for _, s := range m.states {
		sum += s.Amount()
	}
	return sum
}

func (m *MultiSnapshot) Title() string {
	var sb strings.Builder
	sb.WriteString("MultiDestination")
	for _, s := range m.states {
		sb.WriteString("\n  ")
		sb.WriteString(s.Title())
	}
	return sb.String()
}

func (m *MultiSnapshot) Concurrency() int {
	if len(m.states) == 0 {
		return 0
	}
	_min := m.states[0].Concurrency()
	for _, s := range m.states[1:] {
		if c := s.Concurrency(); c < _min {
			_min = c
		}
	}
	return _min
}

// Compile-time interface conformance check.
var _ State = (*MultiSnapshot)(nil)
