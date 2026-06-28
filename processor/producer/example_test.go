package producer_test

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/processor/producer"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// doublerBatch is a producer.Batch implementation that doubles each item's id.
type doublerBatch struct {
	producer.Processor
}

func (b *doublerBatch) Summary() string  { return "doublerBatch" }
func (b *doublerBatch) Concurrency() int { return 1 }
func (b *doublerBatch) AppendState()     {}
func (b *doublerBatch) Prepare() error   { return nil }
func (b *doublerBatch) BeforeRun() error { return nil }
func (b *doublerBatch) AfterRun() error  { return nil }
func (b *doublerBatch) Close() error     { return nil }

func (b *doublerBatch) Exec(items []storage.MapEntry) ([]storage.MapEntry, int64, error) {
	produced := make([]storage.MapEntry, 0, len(items))
	for _, item := range items {
		id, _ := item["id"].(int)
		produced = append(produced, storage.MapEntry{"id": id * 2})
	}
	return produced, int64(len(produced)), nil
}

// ExampleBatch demonstrates a producer.Batch processor that processes items in
// bulk and produces output forwarded to a destination.
func ExampleBatch() {
	var b producer.Batch[storage.MapEntry, storage.MapEntry] = &doublerBatch{}

	items := []storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}}
	out, affected, err := b.Exec(items)
	if err != nil {
		fmt.Println("exec error:", err)
		return
	}

	for _, item := range out {
		fmt.Println("produced id:", item["id"])
	}
	fmt.Println("affected:", affected)
	// Output:
	// produced id: 2
	// produced id: 4
	// produced id: 6
	// affected: 3
}

// splitterItem is a producer.Item implementation that produces two items per input.
type splitterItem struct {
	producer.Processor
}

func (it *splitterItem) Summary() string  { return "splitterItem" }
func (it *splitterItem) Concurrency() int { return 1 }
func (it *splitterItem) AppendState()     {}
func (it *splitterItem) Prepare() error   { return nil }
func (it *splitterItem) BeforeRun() error { return nil }
func (it *splitterItem) AfterRun() error  { return nil }
func (it *splitterItem) Close() error     { return nil }

func (it *splitterItem) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	id, _ := item["id"].(int)
	produced := []storage.MapEntry{
		{"id": id, "tag": "original"},
		{"id": id, "tag": "copy"},
	}
	return produced, true, nil
}

// ExampleItem demonstrates a producer.Item processor that processes items one
// by one, producing output forwarded to a destination.
func ExampleItem() {
	var it producer.Item[storage.MapEntry, storage.MapEntry] = &splitterItem{}

	item := storage.MapEntry{"id": 1}
	out, ok, err := it.Exec(item)
	if err != nil {
		fmt.Println("exec error:", err)
		return
	}

	fmt.Println("ok:", ok)
	for _, p := range out {
		fmt.Println("produced:", p["id"], p["tag"])
	}
	// Output:
	// ok: true
	// produced: 1 original
	// produced: 1 copy
}
