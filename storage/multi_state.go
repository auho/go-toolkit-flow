package storage

import (
	"strings"
)

// MultiState aggregates multiple StateInfo into a single StateInfo.
// Used by MultiDestination to present a unified state view.
type MultiState struct {
	states []StateInfo
}

// NewMultiState creates a MultiState from the given sub-states.
func NewMultiState(states []StateInfo) *MultiState {
	return &MultiState{states: states}
}

func (m *MultiState) Overview() string {
	var sb strings.Builder
	sb.WriteString(m.Title())
	for _, s := range m.states {
		sb.WriteString("\n  ")
		sb.WriteString(s.Overview())
	}
	return sb.String()
}

func (m *MultiState) Amount() int64 {
	var sum int64
	for _, s := range m.states {
		sum += s.Amount()
	}
	return sum
}

func (m *MultiState) Title() string {
	var sb strings.Builder
	sb.WriteString("MultiDestination")
	for _, s := range m.states {
		sb.WriteString("\n  ")
		sb.WriteString(s.Title())
	}
	return sb.String()
}

func (m *MultiState) Concurrency() int {
	if len(m.states) == 0 {
		return 0
	}
	min := m.states[0].Concurrency()
	for _, s := range m.states[1:] {
		if c := s.Concurrency(); c < min {
			min = c
		}
	}
	return min
}

// Compile-time interface conformance check.
var _ StateInfo = (*MultiState)(nil)
