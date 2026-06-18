// Package operator defines the business logic layer of the pipeline.
// An Operator wraps the user's processing logic (Exec) with lifecycle hooks
// (Prepare, BeforeExec, AfterExec, Close) and state/output tracking.
//
// There are two processing paths:
//   - Consumer path (operator/consumer): processes data without producing output.
//   - Producer path (operator/producer): processes data and produces output
//     that is forwarded to a Destination.
//
// BaseOperator provides zero-value-usable defaults for state/output/log
// management via sync.Once lazy initialization.
package operator

import (
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

// Operator is the core interface that all operators must implement.
// It defines the lifecycle and state management contract for the
// business logic layer of the pipeline.
type Operator[E storage.Entry] interface {
	// Summary returns a human-readable description of the operator.
	Summary() string

	// Prepare initializes the operator before processing begins.
	Prepare() error

	// BeforeExec is called once before the first batch is processed.
	BeforeExec() error

	// AfterExec is called once after all batches have been processed.
	AfterExec() error

	// Close releases resources held by the operator.
	Close() error

	// AppendState appends current state lines for status display.
	AppendState()

	// Concurrency returns the number of worker goroutines to use.
	Concurrency() int

	// Init initializes internal fields. Called by exec before Prepare.
	Init()

	// State returns the current state lines for status display.
	State() []string

	// Output returns the output lines for display.
	Output() []string
}

// BaseOperator provides default implementations for state, output, and log
// management. It is zero-value usable: fields are lazily initialized via
// sync.Once on the first call to any method that requires them.
type BaseOperator struct {
	initOnce sync.Once

	duration *timing.Duration
	state    *output.MultilineText
	output   *output.MultilineText
	log      *output.MultilineText
}

// ensureInit guarantees all fields are initialized.
// Called at the entry of every public method.
func (t *BaseOperator) ensureInit() {
	t.initOnce.Do(func() {
		t.duration = timing.NewDuration()
		t.state = output.NewMultilineText()
		t.output = output.NewMultilineText()
		t.log = output.NewMultilineText()
	})
}

// Init ensures all internal fields are initialized.
// Must be called before State(), Output(), or Log() can be used safely.
// Idempotent: multiple calls are safe.
func (t *BaseOperator) Init() {
	t.ensureInit()
}

func (t *BaseOperator) State() []string {
	return t.state.Content()
}

// AddStateLine appends a state line and returns its line number (1-based).
func (t *BaseOperator) AddStateLine(s string) int {
	return t.state.PrintNext(s)
}

// SetStateLine overwrites the state line at the given 1-based line number.
func (t *BaseOperator) SetStateLine(line int, s string) {
	t.state.Print(line, s)
}

func (t *BaseOperator) Output() []string {
	return t.output.Content()
}

// Outputf appends a formatted line to the output.
func (t *BaseOperator) Outputf(format string, a ...any) {
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

// Outputln appends a line to the output.
func (t *BaseOperator) Outputln(a ...any) {
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *BaseOperator) Log() []string {
	return t.log.Content()
}

// Logf appends a formatted line to the log.
func (t *BaseOperator) Logf(format string, a ...any) {
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

// Logln appends a line to the log.
func (t *BaseOperator) Logln(a ...any) {
	t.log.PrintNext(fmt.Sprint(a...))
}
