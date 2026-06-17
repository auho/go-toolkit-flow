package exec

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
)

// Runners 是 Runner 的集合类型，封装了对 []Runner 的遍历操作。
// 后续可扩展为并发执行，调用方无需感知。
type Runners[E storage.Entry] []Runner[E]

func NewRunners[E storage.Entry]() *Runners[E] {
	r := make(Runners[E], 0)
	return &r
}

func (rs *Runners[E]) Add(r ...Runner[E]) {
	*rs = append(*rs, r...)
}

func (rs *Runners[E]) Prepare(ctx context.Context) error {
	for _, r := range *rs {
		if err := r.Prepare(ctx); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}

	return nil
}

func (rs *Runners[E]) Run() {
	for _, r := range *rs {
		r.Run()
	}
}

func (rs *Runners[E]) Receive(items []E) {
	for _, r := range *rs {
		r.Receive(items)
	}
}

func (rs *Runners[E]) Done() {
	for _, r := range *rs {
		r.Done()
	}
}

func (rs *Runners[E]) Finish() error {
	for _, r := range *rs {
		if err := r.Finish(); err != nil {
			return fmt.Errorf("finish: %w", err)
		}
	}

	return nil
}

func (rs *Runners[E]) Close() error {
	for _, r := range *rs {
		if err := r.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	return nil
}

func (rs *Runners[E]) Summary() []string {
	lines := make([]string, 0, len(*rs))
	for _, r := range *rs {
		lines = append(lines, r.Summary())
	}

	return lines
}

func (rs *Runners[E]) State() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Summary())
		for _, s := range r.State() {
			lines = append(lines, "  "+s)
		}
	}

	return lines
}

func (rs *Runners[E]) Output() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Output()...)
	}

	return lines
}

func (rs *Runners[E]) Len() int {
	return len(*rs)
}

func (rs *Runners[E]) All() []Runner[E] {
	return *rs
}
