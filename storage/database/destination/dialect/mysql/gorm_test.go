package mysql

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
)

func TestNewDialectGorm_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config dialect.WriteConfig
	}{
		{
			name:   "invalid table name with semicolon",
			config: dialect.WriteConfig{TableName: "users; DROP TABLE users"},
		},
		{
			name:   "invalid with backtick",
			config: dialect.WriteConfig{TableName: "users` --"},
		},
		{
			name:   "empty table name",
			config: dialect.WriteConfig{TableName: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDialectGorm(nil, tt.config)
			if err == nil {
				t.Fatal("expected error for invalid config, got nil")
			}
		})
	}
}
