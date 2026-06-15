package flow

import "github.com/auho/go-toolkit-flow/task"

var _ task.Transformer[map[string]any] = (*transformer)(nil)

type transformer struct {
	task.BaseTask
}

func (s *transformer) RefreshState() {}

func (s *transformer) Title() string {
	return "test singleton"
}

func (s *transformer) Prepare() error {
	s.SetState(0, "prepare")
	return nil
}

func (s *transformer) Do(item map[string]any) ([]map[string]any, bool) {
	return []map[string]any{item}, true
}

func (s *transformer) PostBatchDo(items []map[string]any) error {
	for _, item := range items {
		_ = item
	}

	return nil
}

func (s *transformer) BeforeRun() error {
	s.SetState(0, "pre do")
	s.Println("pre do")

	return nil
}

func (s *transformer) AfterRun() error {
	s.SetState(0, "post do")
	s.Println("post do")

	return nil
}

func (s *transformer) Close() error { return nil }