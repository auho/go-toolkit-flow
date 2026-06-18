package exec

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
)

// Runners 是 Runner 的集合类型，封装了对 []Runner 的遍历操作。
// 后续可扩展为并发执行，调用方无需感知。
type Runners[SE, DE storage.Entry] []Runner[SE, DE]

func NewRunners[SE, DE storage.Entry]() *Runners[SE, DE] {
	r := make(Runners[SE, DE], 0)
	return &r
}

func (rs *Runners[SE, DE]) Add(r ...Runner[SE, DE]) {
	*rs = append(*rs, r...)
}

func (rs *Runners[SE, DE]) Prepare(ctx context.Context) error {
	for _, r := range *rs {
		if err := r.Prepare(ctx); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}

	return nil
}

func (rs *Runners[SE, DE]) Start() {
	for _, r := range *rs {
		r.Start()
	}
}

func (rs *Runners[SE, DE]) Receive(items []SE) {
	for _, r := range *rs {
		r.Receive(items)
	}
}

func (rs *Runners[SE, DE]) Done() {
	for _, r := range *rs {
		r.Done()
	}
}

func (rs *Runners[SE, DE]) Finish() error {
	for _, r := range *rs {
		if err := r.Finish(); err != nil {
			return fmt.Errorf("finish: %w", err)
		}
	}

	return nil
}

func (rs *Runners[SE, DE]) Close() error {
	for _, r := range *rs {
		if err := r.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	return nil
}

func (rs *Runners[SE, DE]) Summary() []string {
	lines := make([]string, 0, len(*rs))
	for _, r := range *rs {
		lines = append(lines, r.Summary())
	}

	return lines
}

func (rs *Runners[SE, DE]) State() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Summary())
		for _, s := range r.State() {
			lines = append(lines, "  "+s)
		}
	}

	return lines
}

func (rs *Runners[SE, DE]) Output() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Output()...)
	}

	return lines
}

func (rs *Runners[SE, DE]) Len() int {
	return len(*rs)
}

func (rs *Runners[SE, DE]) All() []Runner[SE, DE] {
	return *rs
}
