package flow

import "github.com/auho/go-toolkit-flow/task"

var _ task.Singleton[map[string]any] = (*singleton)(nil)

type singleton struct {
	task.Task
}

func (s *singleton) Blink() {}

func (s *singleton) Title() string {
	return "test singleton"
}

func (s *singleton) Prepare() error {
	s.SetState(0, "prepare")
	return nil
}

func (s *singleton) Do(item map[string]any) ([]map[string]any, bool) {
	return []map[string]any{item}, true
}

func (s *singleton) PostBatchDo(items []map[string]any) error {
	for _, item := range items {
		_ = item
	}

	return nil
}

func (s *singleton) PreDo() error {
	s.SetState(0, "pre do")
	s.Println("pre do")

	return nil
}

func (s *singleton) PostDo() error {
	s.SetState(0, "post do")
	s.Println("post do")

	return nil
}

func (s *singleton) Close() error { return nil }
