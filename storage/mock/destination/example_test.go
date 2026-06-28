package destination_test

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage"
	mockdest "github.com/auho/go-toolkit-flow/v3/storage/mock/destination"
)

// ExampleNewInsertMap demonstrates a mock in-memory destination that receives
// MapEntry items in insert mode. After the full lifecycle, Items() returns all
// received data.
func ExampleNewInsertMap() {
	d, err := mockdest.NewInsertMap()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	ctx := context.Background()
	if err := d.Prepare(ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	d.Accept()
	_ = d.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	d.Done()

	if err := d.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}
	defer d.Close()

	items := d.Items()
	fmt.Println("items:", len(items))
	// Output:
	// items: 2
}

// ExampleNewInsertSlice demonstrates a mock in-memory destination that receives
// SliceEntry items in insert mode.
func ExampleNewInsertSlice() {
	d, err := mockdest.NewInsertSlice()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	ctx := context.Background()
	if err := d.Prepare(ctx); err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	d.Accept()
	_ = d.Receive([]storage.SliceEntry{{"a", 1}, {"b", 2}, {"c", 3}})
	d.Done()

	if err := d.Finish(); err != nil {
		fmt.Println("finish error:", err)
		return
	}
	defer d.Close()

	items := d.Items()
	fmt.Println("items:", len(items))
	// Output:
	// items: 3
}
