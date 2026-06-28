package consumer_test

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/processor/consumer"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// counterBatch is a consumer.Batch implementation that counts processed items.
type counterBatch struct {
	consumer.Processor
}

func (b *counterBatch) Summary() string  { return "counterBatch" }
func (b *counterBatch) Concurrency() int { return 1 }
func (b *counterBatch) AppendState()     {}
func (b *counterBatch) Prepare() error   { return nil }
func (b *counterBatch) BeforeRun() error { return nil }
func (b *counterBatch) AfterRun() error  { return nil }
func (b *counterBatch) Close() error     { return nil }

func (b *counterBatch) Exec(items []storage.MapEntry) (int64, error) {
	return int64(len(items)), nil
}

// ExampleBatch demonstrates a consumer.Batch processor that processes items
// in bulk and returns the affected count.
func ExampleBatch() {
	var b consumer.Batch[storage.MapEntry] = &counterBatch{}

	items := []storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}}
	affected, err := b.Exec(items)
	if err != nil {
		fmt.Println("exec error:", err)
		return
	}

	fmt.Println("affected:", affected)
	// Output:
	// affected: 3
}

// evenFilterItem is a consumer.Item implementation that keeps only even ids.
type evenFilterItem struct {
	consumer.Processor
}

func (it *evenFilterItem) Summary() string  { return "evenFilterItem" }
func (it *evenFilterItem) Concurrency() int { return 1 }
func (it *evenFilterItem) AppendState()     {}
func (it *evenFilterItem) Prepare() error   { return nil }
func (it *evenFilterItem) BeforeRun() error { return nil }
func (it *evenFilterItem) AfterRun() error  { return nil }
func (it *evenFilterItem) Close() error     { return nil }

func (it *evenFilterItem) Exec(item storage.MapEntry) (bool, error) {
	id, _ := item["id"].(int)
	return id%2 == 0, nil
}

// ExampleItem demonstrates a consumer.Item processor that processes items one
// by one, keeping only those with even ids.
func ExampleItem() {
	var it consumer.Item[storage.MapEntry] = &evenFilterItem{}

	items := []storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}}
	for _, item := range items {
		ok, err := it.Exec(item)
		if err != nil {
			fmt.Println("exec error:", err)
			return
		}
		if ok {
			fmt.Println("kept:", item["id"])
		}
	}
	// Output:
	// kept: 2
	// kept: 4
}
