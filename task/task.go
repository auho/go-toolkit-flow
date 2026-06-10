package task

import (
	"fmt"
	"runtime"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

type Task[E storage.Entry] interface {
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

type Option func(*BaseTask)

func WithConcurrency(c int) Option {
	return func(t *BaseTask) {
		t.concurrency = c
	}
}

type BaseTask struct {
	hasBeenInit bool
	concurrency int

	duration *timing.Duration
	state    *output.MultilineText
	output   *output.MultilineText
	log      *output.MultilineText
}

func (t *BaseTask) Init(opts ...Option) {
	for _, o := range opts {
		o(t)
	}

	if t.concurrency <= 0 {
		t.concurrency = runtime.NumCPU()
	}

	if !t.HasBeenInit() {
		t.duration = timing.NewDuration()
		t.state = output.NewMultilineText()
		t.output = output.NewMultilineText()
		t.log = output.NewMultilineText()
	}

	t.hasBeenInit = true
}

func (t *BaseTask) HasBeenInit() bool {
	return t.hasBeenInit
}

func (t *BaseTask) Concurrency() int {
	return t.concurrency
}

// AddState
// int 当前行行数 从 1 开始
func (t *BaseTask) AddState(s string) int {
	return t.state.PrintNext(s)
}

func (t *BaseTask) SetState(line int, s string) {
	t.state.Print(line, s)
}

func (t *BaseTask) State() []string {
	return t.state.Content()
}

func (t *BaseTask) Output() []string {
	return t.output.Content()
}

func (t *BaseTask) Log() []string {
	return t.log.Content()
}

func (t *BaseTask) Printf(format string, a ...any) {
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseTask) Println(a ...any) {
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *BaseTask) Logf(format string, a ...any) {
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

func (t *BaseTask) Logln(a ...any) {
	t.log.PrintNext(fmt.Sprint(a...))
}
