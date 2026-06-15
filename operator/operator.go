package operator

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

type Operator[E storage.Entry] interface {
	// Title need to be implemented
	Title() string

	// Prepare need to be implemented
	Prepare() error

	// BeforeRun need to be implemented
	BeforeRun() error

	// AfterRun need to be implemented
	AfterRun() error

	// Close need to be implemented
	Close() error

	// RefreshState need to be implemented
	RefreshState()

	Init(...Option)
	HasBeenInit() bool
	Concurrency() int
	State() []string
	Output() []string
}

type Option func(*BaseOperator)

func WithConcurrency(c int) Option {
	return func(t *BaseOperator) {
		t.concurrency = c
	}
}

type BaseOperator struct {
	initOnce    sync.Once
	hasBeenInit bool
	concurrency int

	duration *timing.Duration
	state    *output.MultilineText
	output   *output.MultilineText
	log      *output.MultilineText
}

// ensureInit guarantees all fields are initialized.
// Called at the entry of every public method.
func (t *BaseOperator) ensureInit() {
	t.initOnce.Do(func() {
		if t.concurrency <= 0 {
			t.concurrency = runtime.NumCPU()
		}
		t.duration = timing.NewDuration()
		t.state = output.NewMultilineText()
		t.output = output.NewMultilineText()
		t.log = output.NewMultilineText()
		t.hasBeenInit = true
	})
}

// Init applies options and ensures initialization.
// It is now optional — BaseOperator is zero-value usable.
func (t *BaseOperator) Init(opts ...Option) {
	for _, o := range opts {
		o(t)
	}

	t.ensureInit()
}

func (t *BaseOperator) HasBeenInit() bool {
	return t.hasBeenInit
}

func (t *BaseOperator) Concurrency() int {
	t.ensureInit()
	return t.concurrency
}

// AddState
// int 当前行行数 从 1 开始
func (t *BaseOperator) AddState(s string) int {
	t.ensureInit()
	return t.state.PrintNext(s)
}

func (t *BaseOperator) SetState(line int, s string) {
	t.ensureInit()
	t.state.Print(line, s)
}

func (t *BaseOperator) State() []string {
	t.ensureInit()
	return t.state.Content()
}

func (t *BaseOperator) Output() []string {
	t.ensureInit()
	return t.output.Content()
}

func (t *BaseOperator) Log() []string {
	t.ensureInit()
	return t.log.Content()
}

func (t *BaseOperator) Printf(format string, a ...any) {
	t.ensureInit()
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseOperator) Println(a ...any) {
	t.ensureInit()
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *BaseOperator) Logf(format string, a ...any) {
	t.ensureInit()
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseOperator) Logln(a ...any) {
	t.ensureInit()
	t.log.PrintNext(fmt.Sprint(a...))
}
