package exec

import (
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// MultiStageBuilder constructs a multi-stage Runner with compile-time type safety.
// SE is the source element type (first stage input).
// DE is the current output type (evolves with each Stage call).
type MultiStageBuilder[SE, DE storage.Entry] struct {
	stages []stageRunner
}

// NewMultiStage creates a new builder with the given source type.
func NewMultiStage[SE storage.Entry]() *MultiStageBuilder[SE, SE] {
	return &MultiStageBuilder[SE, SE]{}
}

// Stage adds a processing stage to the builder.
// The runner must accept DE (current output type) and produce DE2 (new output type).
// Type parameters are inferred from the runner argument.
//
// This is a function (not a method) because Go methods cannot have
// additional type parameters beyond the receiver's.
func Stage[SE, DE, DE2 storage.Entry](
	b *MultiStageBuilder[SE, DE],
	r Runner[DE, DE2],
) *MultiStageBuilder[SE, DE2] {
	stages := make([]stageRunner, len(b.stages), len(b.stages)+1)
	copy(stages, b.stages)
	stages = append(stages, adaptRunner[DE, DE2](r))
	return &MultiStageBuilder[SE, DE2]{stages: stages}
}

// Build creates the multi-stage Runner.
func (b *MultiStageBuilder[SE, DE]) Build() Runner[SE, DE] {
	return &multiStageRunner[SE, DE]{stages: b.stages}
}
