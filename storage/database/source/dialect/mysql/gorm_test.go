package mysql

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage/database/source/dialect"
)

func TestNewDialectGorm_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config dialect.ScanConfig
	}{
		{
			name: "invalid table name with semicolon",
			config: dialect.ScanConfig{
				TableName:     "users; DROP TABLE users",
				SegmentIDName: "id",
			},
		},
		{
			name: "invalid segment id name with backtick",
			config: dialect.ScanConfig{
				TableName:     "users",
				SegmentIDName: "id` --",
			},
		},
		{
			name: "empty table name",
			config: dialect.ScanConfig{
				TableName:     "",
				SegmentIDName: "id",
			},
		},
		{
			name: "empty segment id name",
			config: dialect.ScanConfig{
				TableName:     "users",
				SegmentIDName: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDialectGorm(tt.config, nil)
			if err == nil {
				t.Fatal("expected error for invalid config, got nil")
			}
		})
	}
}
