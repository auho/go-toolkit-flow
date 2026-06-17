package operator

import (
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

type Operator[E storage.Entry] interface {
	// Summary need to be implemented
	Summary() string

	// Prepare need to be implemented
	Prepare() error

	// BeforeExec need to be implemented
	BeforeExec() error

	// AfterExec need to be implemented
	AfterExec() error

	// Close need to be implemented
	Close() error

	// AdditionalState need to be implemented
	AdditionalState()

	// Concurrency need to be implemented
	Concurrency() int

	Init()
	State() []string
	Output() []string
}

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

// Init applies options and ensures initialization.
// It is now optional — BaseOperator is zero-value usable.
func (t *BaseOperator) Init() {
	t.ensureInit()
}

func (t *BaseOperator) State() []string {
	return t.state.Content()
}

// AddStateLine
// int 当前行行数 从 1 开始
func (t *BaseOperator) AddStateLine(s string) int {
	return t.state.PrintNext(s)
}

func (t *BaseOperator) SetStateLine(line int, s string) {
	t.state.Print(line, s)
}

func (t *BaseOperator) Output() []string {
	return t.output.Content()
}

func (t *BaseOperator) Outputf(format string, a ...any) {
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseOperator) Outputln(a ...any) {
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *BaseOperator) Log() []string {
	return t.log.Content()
}

func (t *BaseOperator) Logf(format string, a ...any) {
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseOperator) Logln(a ...any) {
	t.log.PrintNext(fmt.Sprint(a...))
}
