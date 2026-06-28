package flow

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/exec"
	"github.com/auho/go-toolkit-flow/v3/exec/consumer/batch"
	produceritem "github.com/auho/go-toolkit-flow/v3/exec/producer/item"
	mockdest "github.com/auho/go-toolkit-flow/v3/storage/mock/destination"
	mocksrc "github.com/auho/go-toolkit-flow/v3/storage/mock/source"
)

// ExampleRunFlow demonstrates the producer path: a mock source generates
// MapEntry items, a producer.Item runner passes them through, and an InsertMap
// destination collects the output.
//
// Note: RunFlow prints summary/state/output containing runtime-dependent
// values (concurrency from runtime.NumCPU, duration from timing), so the output
// is not asserted with // Output:.
func ExampleRunFlow() {
	src := mocksrc.NewMap(mocksrc.Config{Total: 10, PageSize: 5})

	dest, err := mockdest.NewInsertMap()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest,
		),
	}

	if err := RunFlow(opts...); err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println(dest.StateInfo().Amount())
}

// ExampleRunFlow_consumer demonstrates the consumer path: a mock source feeds
// a consumer.Batch runner with no destination registered. WithGroup defaults to
// NoopDestination when no dest is provided, so the pipeline runs without
// producing output.
//
// Note: RunFlow prints runtime-dependent values (concurrency, duration), so
// the output is not asserted with // Output:.
func ExampleRunFlow_consumer() {
	src := mocksrc.NewMap(mocksrc.Config{Total: 10, PageSize: 5})

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				batch.NewRunner[map[string]any, map[string]any](&batchOp{}),
			},
			// No destination → defaults to NoopDestination (consumer path)
		),
	}

	if err := RunFlow(opts...); err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println(src.State().Amount())
}

// ExampleRunFlow_multiDestination demonstrates fan-out to multiple destinations
// within a single group: a producer.Item runner forwards output to two
// InsertMap destinations (wrapped as MultiDestination). Both destinations
// receive the complete dataset.
//
// Note: RunFlow prints runtime-dependent values (concurrency, duration), so
// the output is not asserted with // Output:.
func ExampleRunFlow_multiDestination() {
	src := mocksrc.NewMap(mocksrc.Config{Total: 10, PageSize: 5})

	dest1, err := mockdest.NewInsertMap()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	dest2, err := mockdest.NewInsertMap()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest1, dest2, // WithGroup wraps multiple dests as MultiDestination
		),
	}

	if err := RunFlow(opts...); err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println("dest1:", dest1.StateInfo().Amount())
	fmt.Println("dest2:", dest2.StateInfo().Amount())
}
