package processor_test

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/processor"
	"github.com/auho/go-toolkit-flow/v3/processor/consumer"
	"github.com/auho/go-toolkit-flow/v3/processor/producer"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// mockBatch implements consumer.Batch[storage.MapEntry]
type mockBatch struct {
	processor.BaseProcessor
}

func (m *mockBatch) Summary() string  { return "mock-batch" }
func (m *mockBatch) Prepare() error   { return nil }
func (m *mockBatch) BeforeRun() error { return nil }
func (m *mockBatch) AfterRun() error  { return nil }
func (m *mockBatch) Close() error     { return nil }
func (m *mockBatch) AppendState()     {}
func (m *mockBatch) Concurrency() int { return 1 }
func (m *mockBatch) Exec(items []storage.MapEntry) (int64, error) {
	return int64(len(items)), nil
}

// compile-time assertion
var _ consumer.Batch[storage.MapEntry] = (*mockBatch)(nil)

// mockConsumerItem implements consumer.Item[storage.MapEntry]
type mockConsumerItem struct {
	processor.BaseProcessor
}

func (m *mockConsumerItem) Summary() string  { return "mock-consumer-item" }
func (m *mockConsumerItem) Prepare() error   { return nil }
func (m *mockConsumerItem) BeforeRun() error { return nil }
func (m *mockConsumerItem) AfterRun() error  { return nil }
func (m *mockConsumerItem) Close() error     { return nil }
func (m *mockConsumerItem) AppendState()     {}
func (m *mockConsumerItem) Concurrency() int { return 1 }
func (m *mockConsumerItem) Exec(item storage.MapEntry) (bool, error) {
	return true, nil
}

var _ consumer.Item[storage.MapEntry] = (*mockConsumerItem)(nil)

// mockProducerBatch implements producer.Batch[storage.MapEntry, storage.MapEntry]
type mockProducerBatch struct {
	processor.BaseProcessor
}

func (m *mockProducerBatch) Summary() string  { return "mock-producer-batch" }
func (m *mockProducerBatch) Prepare() error   { return nil }
func (m *mockProducerBatch) BeforeRun() error { return nil }
func (m *mockProducerBatch) AfterRun() error  { return nil }
func (m *mockProducerBatch) Close() error     { return nil }
func (m *mockProducerBatch) AppendState()     {}
func (m *mockProducerBatch) Concurrency() int { return 1 }
func (m *mockProducerBatch) Exec(items []storage.MapEntry) ([]storage.MapEntry, int64, error) {
	return items, int64(len(items)), nil
}

var _ producer.Batch[storage.MapEntry, storage.MapEntry] = (*mockProducerBatch)(nil)

// mockProducerItem implements producer.Item[storage.MapEntry, storage.MapEntry]
type mockProducerItem struct {
	processor.BaseProcessor
}

func (m *mockProducerItem) Summary() string  { return "mock-producer-item" }
func (m *mockProducerItem) Prepare() error   { return nil }
func (m *mockProducerItem) BeforeRun() error { return nil }
func (m *mockProducerItem) AfterRun() error  { return nil }
func (m *mockProducerItem) Close() error     { return nil }
func (m *mockProducerItem) AppendState()     {}
func (m *mockProducerItem) Concurrency() int { return 1 }
func (m *mockProducerItem) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	return []storage.MapEntry{item}, true, nil
}

var _ producer.Item[storage.MapEntry, storage.MapEntry] = (*mockProducerItem)(nil)

// Test functions

func TestMockBatch_Exec(t *testing.T) {
	m := &mockBatch{}
	n, err := m.Exec([]storage.MapEntry{{"key": "value"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestMockConsumerItem_Exec(t *testing.T) {
	m := &mockConsumerItem{}
	ok, err := m.Exec(storage.MapEntry{"key": "value"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
}

func TestMockProducerBatch_Exec(t *testing.T) {
	m := &mockProducerBatch{}
	items, n, err := m.Exec([]storage.MapEntry{{"key": "value"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
	if len(items) != 1 {
		t.Errorf("expected 1 item, got %d", len(items))
	}
}

func TestMockProducerItem_Exec(t *testing.T) {
	m := &mockProducerItem{}
	items, ok, err := m.Exec(storage.MapEntry{"key": "value"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected true")
	}
	if len(items) != 1 {
		t.Errorf("expected 1 item, got %d", len(items))
	}
}
