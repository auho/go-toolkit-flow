package source_test

import (
	"context"
	"fmt"
	"os"

	filesource "github.com/auho/go-toolkit-flow/v3/storage/file/source"
)

// ExampleNewLine demonstrates a file source that reads lines from a file in
// batches. A temporary file is created with sample content so the example is
// self-contained.
func ExampleNewLine() {
	// Create a temporary file with sample content.
	f, err := os.CreateTemp("", "example-src-*")
	if err != nil {
		fmt.Println("create temp error:", err)
		return
	}
	name := f.Name()
	defer os.Remove(name)

	for i := 0; i < 5; i++ {
		fmt.Fprintf(f, "line%d\n", i)
	}
	f.Close()

	s, err := filesource.NewLine(filesource.Config{
		Name:        name,
		BatchSize:   2,
		Concurrency: 1,
	})
	if err != nil {
		fmt.Println("new error:", err)
		return
	}

	ctx := context.Background()
	if err := s.Prepare(ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	s.Scan()

	go func() {
		_ = s.Finish()
	}()

	var count int
	for items := range s.ReceiveChan() {
		count += len(items)
	}

	defer s.Close()

	fmt.Println("count:", count)
	// Output:
	// count: 5
}
