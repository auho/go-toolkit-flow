package source

import (
	"context"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

// mockDialect implements dialect.Dialect for unit tests that don't need a real database.
type mockDialect struct{}

func (mockDialect) DBName() string                                           { return "mock" }
func (mockDialect) FetchIDBounds() (int64, int64, error)                     { return 0, 0, nil }
func (mockDialect) QueryMapByRange(int64, int64) (storage.MapEntries, error) { return nil, nil }
func (mockDialect) Close() error                                             { return nil }

func TestSection_Prepare_NegativeMaxItems(t *testing.T) {
	s := newSection[storage.MapEntry](nil, mockDialect{}, SectionConfig{
		MaxItems: -1,
		PageSize: 100,
	})
	err := s.Prepare(context.Background())
	if err == nil {
		t.Fatal("expected error for negative MaxItems, got nil")
	}
}
