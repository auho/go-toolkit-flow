// Package processor provides test helpers for processor implementations.
// TestProcessor provides no-op lifecycle defaults for test processors that
// don't need lifecycle logic.
package processor

import "github.com/auho/go-toolkit-flow/v3/processor"

// TestProcessor provides no-op lifecycle defaults for test processors.
// Embed this instead of processor.BaseProcessor when tests don't need
// lifecycle logic (Prepare/BeforeRun/AfterRun/Close/AppendState).
type TestProcessor struct {
	processor.BaseProcessor
}

func (t *TestProcessor) Prepare() error    { return nil }
func (t *TestProcessor) BeforeRun() error  { return nil }
func (t *TestProcessor) AfterRun() error   { return nil }
func (t *TestProcessor) Close() error      { return nil }
func (t *TestProcessor) AppendState()      {}
