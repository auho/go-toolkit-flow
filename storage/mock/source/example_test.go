package source_test

import (
	"context"
	"fmt"

	mocksource "github.com/auho/go-toolkit-flow/v3/storage/mock/source"
)

// ExampleNewMap demonstrates a mock in-memory source that generates MapEntry
// items. Scan launches a goroutine that produces data into ReceiveChan; Finish
// waits for the goroutine and closes the channel.
func ExampleNewMap() {
	s := mocksource.NewMap(mocksource.Config{
		Total:       5,
		PageSize:    2,
		Concurrency: 1,
	})

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

// ExampleNewString demonstrates a mock in-memory source that generates string
// items.
func ExampleNewString() {
	s := mocksource.NewString(mocksource.Config{
		Total:       3,
		PageSize:    1,
		Concurrency: 1,
	})

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
	// count: 3
}
