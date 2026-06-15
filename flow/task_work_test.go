package flow

import (
	"github.com/auho/go-toolkit-flow/task"
)

var _ task.Batch[map[string]any] = (*batch)(nil)

type batch struct {
	task.BaseTask
}

func (w *batch) RefreshState() {}

func (w *batch) Title() string {
	return "test work"
}

func (w *batch) Prepare() error {
	w.SetState(0, "prepare")
	return nil
}

func (w *batch) Do(items []map[string]any) (int, error) {
	for _, item := range items {
		_ = item
	}

	return len(items), nil
}

func (w *batch) BeforeRun() error {
	w.SetState(0, "pre do")
	w.Println("pre do")

	return nil
}

func (w *batch) AfterRun() error {
	w.SetState(0, "post do")
	w.Println("post do")

	return nil
}

func (w *batch) Close() error { return nil }