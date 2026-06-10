package task

import (
	"fmt"
	"runtime"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
)

type Tasker[E storage.Entry] interface {
	// Title need to be implemented
	Title() string

	// Prepare need to be implemented
	Prepare() error

	// PreDo need to be implemented
	PreDo() error

	// PostDo need to be implemented
	PostDo() error

	// Close need to be implemented
	Close() error

	// Blink need to be implemented
	Blink()

	Init(...Option)
	HasBeenInit() bool
	Concurrency() int
	State() []string
	Output() []string
}

type Option func(*Task)

func WithConcurrency(c int) Option {
	return func(t *Task) {
		t.concurrency = c
	}
}

type Task struct {
	hasBeenInit bool
	concurrency int

	duration *timing.Duration
	state    *output.MultilineText
	output   *output.MultilineText
	log      *output.MultilineText
}

func (t *Task) Init(opts ...Option) {
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

func (t *Task) HasBeenInit() bool {
	return t.hasBeenInit
}

func (t *Task) Concurrency() int {
	return t.concurrency
}

// AddState
// int 当前行行数 从 1 开始
func (t *Task) AddState(s string) int {
	return t.state.PrintNext(s)
}

func (t *Task) SetState(line int, s string) {
	t.state.Print(line, s)
}

func (t *Task) State() []string {
	return t.state.Content()
}

func (t *Task) Output() []string {
	return t.output.Content()
}

func (t *Task) Log() []string {
	return t.log.Content()
}

func (t *Task) Printf(format string, a ...any) {
	t.output.PrintNext(fmt.Sprintf(format, a...))
}

func (t *Task) Println(a ...any) {
	t.output.PrintNext(fmt.Sprint(a...))
}

func (t *Task) Logf(format string, a ...any) {
	t.log.PrintNext(fmt.Sprintf(format, a...))
}

func (t *Task) Logln(a ...any) {
	t.log.PrintNext(fmt.Sprint(a...))
}
