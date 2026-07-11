package exec_test

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/exec"
	testutilprocessor "github.com/auho/go-toolkit-flow/v3/internal/testutil/processor"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// exampleExecutor is a consumer-style executor that counts items.
type exampleExecutor struct{}

func (e *exampleExecutor) Exec(items []storage.MapEntry) (out []storage.MapEntry, amount, affected int64, err error) {
	return nil, int64(len(items)), int64(len(items)), nil
}

// exampleProducerExecutor is a producer-style executor that doubles each item's id.
type exampleProducerExecutor struct{}

func (e *exampleProducerExecutor) Exec(items []storage.MapEntry) (out []storage.MapEntry, amount, affected int64, err error) {
	produced := make([]storage.MapEntry, 0, len(items))
	for _, item := range items {
		id, _ := item["id"].(int)
		produced = append(produced, storage.MapEntry{"id": id * 2})
	}
	return produced, int64(len(items)), int64(len(produced)), nil
}

// exampleProc is a minimal processor implementation.
type exampleProc struct {
	testutilprocessor.TestProcessor
}

func (p *exampleProc) Concurrency() int { return 1 }
func (p *exampleProc) Summary() string  { return "exampleProc" }

// ExampleNewRunner demonstrates creating a single Runner and driving its
// full consumer lifecycle: Prepare -> Start -> Receive -> Done -> Finish -> Close.
func ExampleNewRunner() {
	r := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleExecutor{}, &exampleProc{})

	ctx := context.Background()
	if err := r.Prepare(ctx, ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	r.Start()
	r.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	r.Done()

	if err := r.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}

	defer r.Close()

	for _, s := range r.Summary() {
		fmt.Println(s)
	}
	// Output:
	// exampleProc
}

// ExampleNewRunners demonstrates creating a Runners collection, adding multiple
// runners, and driving the batch lifecycle. Received items are fanned out to
// all runners.
func ExampleNewRunners() {
	rs := exec.NewRunners[storage.MapEntry, storage.MapEntry]()

	r1 := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleExecutor{}, &exampleProc{})
	r2 := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleExecutor{}, &exampleProc{})
	rs.Add(r1, r2)

	ctx := context.Background()
	if err := rs.Prepare(ctx, ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	rs.Start()
	rs.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	rs.Done()

	if err := rs.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}

	defer rs.Close()

	fmt.Println(rs.Len())
	fmt.Println(rs.Summary())
	// Output:
	// 2
	// [exampleProc exampleProc]
}

// ExampleRunner_OutChan demonstrates the producer path: the executor produces
// output that is forwarded through OutChan and collected by a consumer goroutine.
func ExampleRunner_OutChan() {
	r := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleProducerExecutor{}, &exampleProc{})

	ctx := context.Background()
	if err := r.Prepare(ctx, ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	// Collect produced output from OutChan in a separate goroutine.
	var collected []storage.MapEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range r.OutChan() {
			collected = append(collected, batch...)
		}
	}()

	r.Start()
	r.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}})
	r.Done()

	if err := r.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}
	<-done

	defer r.Close()

	for _, item := range collected {
		fmt.Println("id:", item["id"])
	}
	// Output:
	// id: 2
	// id: 4
	// id: 6
}

// ExampleNewPipeline demonstrates building a multi-stage pipeline where
// stage 1 doubles each item's id, and stage 2 doubles it again.
// The pipeline is built with compile-time type safety using
// NewPipeline -> Stage -> Stage -> Build.
func ExampleNewPipeline() {
	// Stage 1: producer that doubles IDs
	r1 := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleProducerExecutor{}, &exampleProc{})
	// Stage 2: producer that doubles IDs again
	r2 := exec.NewRunner[storage.MapEntry, storage.MapEntry](&exampleProducerExecutor{}, &exampleProc{})

	// Build the pipeline: MapEntry -> MapEntry -> MapEntry
	pipeline := exec.Stage(
		exec.Stage(exec.NewPipeline[storage.MapEntry](), r1),
		r2,
	).Build()

	ctx := context.Background()
	if err := pipeline.Prepare(ctx, ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	// Collect final output from the pipeline's OutChan
	var collected []storage.MapEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range pipeline.OutChan() {
			collected = append(collected, batch...)
		}
	}()

	pipeline.Start()
	pipeline.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}})
	pipeline.Done()

	if err := pipeline.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}
	<-done

	defer pipeline.Close()

	for _, item := range collected {
		fmt.Println("id:", item["id"])
	}
	// Output:
	// id: 4
	// id: 8
	// id: 12
}
