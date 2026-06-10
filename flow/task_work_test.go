package flow

import (
	"github.com/auho/go-toolkit-flow/task"
)

var _ task.Work[map[string]any] = (*work)(nil)

type work struct {
	task.Task
}

func (w *work) Blink() {}

func (w *work) Title() string {
	return "test work"
}

func (w *work) Prepare() error {
	w.SetState(0, "prepare")
	return nil
}

func (w *work) Do(items []map[string]any) (int, error) {
	for _, item := range items {
		_ = item
	}

	return len(items), nil
}

func (w *work) PreDo() error {
	w.SetState(0, "pre do")
	w.Println("pre do")

	return nil
}

func (w *work) PostDo() error {
	w.SetState(0, "post do")
	w.Println("post do")

	return nil
}

func (w *work) Close() error { return nil }
