package flow

import (
	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
)

// group binds a set of runners with one destination.
// Runners' outputs are fan-in merged and then forwarded to the destination.
// destination may be NoopDestination (consumer path), a single Destination, or
// MultiDestination (fan-out to multiple destinations).
// group is an internal type managed by flow and not exported.
type group[SE, DE storage.Entry] struct {
	// runners is a reusable batch collection that supports concurrent iteration in future.
	runners *exec.Runners[SE, DE]
	// destination receives the fan-in merged output from all runners in this group.
	destination storage.Destination[DE]
}