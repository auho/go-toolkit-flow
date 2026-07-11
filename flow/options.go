package flow

import (
	"errors"
	"time"

	"github.com/auho/go-toolkit-flow/v3/exec"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// Option configures a flow.
type Option[SE, DE storage.Entry] func(*flow[SE, DE])

// Sentinel errors for programmatic error checking.
var (
	ErrSourceNotFound = errors.New("source not found")
	ErrGroupNotFound  = errors.New("group not found")
)

// WithSource sets the data source for the flow.
func WithSource[SE, DE storage.Entry](se storage.Source[SE]) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.source = se
	}
}

// WithStateInterval sets the state refresh interval.
func WithStateInterval[SE, DE storage.Entry](d time.Duration) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.stateInterval = d
	}
}

// WithGroup registers a group of runners bound to one or more destinations.
//   - 0 dests: destination defaults to NoopDestination (consumer path, no data produced)
//   - 1 dest:   single destination
//   - N dests:  wrapped as MultiDestination (fan-out to all destinations)
//
// Each group runs independently: runners' outputs are fan-in merged within the group,
// then forwarded to the group's destination(s). Groups execute concurrently.
func WithGroup[SE, DE storage.Entry](
	runners []exec.Runner[SE, DE],
	dests ...storage.Destination[DE],
) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		rs := exec.NewRunners[SE, DE]()
		rs.Add(runners...)

		var dest storage.Destination[DE]
		switch len(dests) {
		case 0:
			dest = storage.NoopDestination[DE]{}
		case 1:
			dest = dests[0]
		default:
			dest = storage.MultiDestination[DE](dests)
		}

		f.groups.Add(newGroup(rs, dest))
	}
}
