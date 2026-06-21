// Package processor defines the business logic layer of the pipeline.
// A Processor wraps the user's processing logic (Exec) with lifecycle hooks
// (Prepare, BeforeRun, AfterRun, Close) and state/output tracking.
//
// There are two processing paths:
//   - Consumer path (processor/consumer): processes data without producing output.
//   - Producer path (processor/producer): processes data and produces output
//     that is forwarded to a Destination.
//
// BaseProcessor provides zero-value-usable defaults for state/output/log
// management via sync.Once lazy initialization.
//
// Optional capabilities (discovered via type assertion, like DestinationHolder):
//   - AfterBatcher[T]: post-batch processing hook, called after each batch's
//     Exec completes. Producer-path instances process produced data (DE);
//     consumer-path instances process the input batch (SE).
package processor

import (
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

// Processor is the core interface that all processors must implement.
// It defines the lifecycle and state management contract for the
// business logic layer of the pipeline.
type Processor[E storage.Entry] interface {
	// Summary returns a human-readable description of the processor.
	Summary() string

	// Prepare initializes the processor before processing begins.
	Prepare() error

	// BeforeRun is called once before the first batch is processed.
	BeforeRun() error

	// AfterRun is called once after all batches have been processed.
	AfterRun() error

	// Close releases resources held by the processor.
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

// AfterBatcher is optionally implemented by processors that need post-batch
// processing. The pipeline discovers this via type assertion (like
// DestinationHolder) and calls AfterBatch after each batch's Exec completes.
//
// Producer-path instances are parameterized by DE (processing produced data);
// consumer-path instances are parameterized by SE (processing the input batch).
type AfterBatcher[T storage.Entry] interface {
	AfterBatch([]T) error
}

// BaseProcessor provides default implementations for state, output, and log
// management. It is zero-value usable: fields are lazily initialized via
// sync.Once on the first call to any method that requires them.
type BaseProcessor struct {
	initOnce sync.Once

	duration *timing.Duration
	state    *output.MultilineText
	output   *output.MultilineText
	log      *output.MultilineText
}

// ensureInit guarantees all fields are initialized.
// Called at the entry of every public method.
func (t *BaseProcessor) ensureInit() {
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
func (t *BaseProcessor) Init() {
	t.ensureInit()
}

func (t *BaseProcessor) State() []string {
	return t.state.Content()
}

// AddStateLine appends a state line and returns its line number (1-based).
func (t *BaseProcessor) AddStateLine(s string) int {
	return t.state.PrintNext(s)
}

// SetStateLine overwrites the state line at the given 1-based line number.
func (t *BaseProcessor) SetStateLine(line int, s string) {
	t.state.Print(line, s)
}

func (t *BaseProcessor) Output() []string {
	return t.output.Content()
}

// Outputf appends a formatted line to the output.
func (t *BaseProcessor) Outputf(format string, a ...any) {
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

// Outputln appends a line to the output.
func (t *BaseProcessor) Outputln(a ...any) {
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *BaseProcessor) Log() []string {
	return t.log.Content()
}

// Logf appends a formatted line to the log.
func (t *BaseProcessor) Logf(format string, a ...any) {
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

// Logln appends a line to the log.
func (t *BaseProcessor) Logln(a ...any) {
	t.log.PrintNext(fmt.Sprint(a...))
}
