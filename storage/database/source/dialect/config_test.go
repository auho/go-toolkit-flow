package dialect

import (
	"testing"
)

func TestScanConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ScanConfig
		wantErr bool
	}{
		{
			name: "valid simple identifiers",
			config: ScanConfig{
				TableName:     "users",
				SegmentIDName: "id",
			},
			wantErr: false,
		},
		{
			name: "valid with underscore and digits",
			config: ScanConfig{
				TableName:     "user_data_2",
				SegmentIDName: "user_id",
			},
			wantErr: false,
		},
		{
			name: "valid starting with underscore",
			config: ScanConfig{
				TableName:     "_users",
				SegmentIDName: "_id",
			},
			wantErr: false,
		},
		{
			name: "valid mixed case",
			config: ScanConfig{
				TableName:     "UsersTable",
				SegmentIDName: "Id",
			},
			wantErr: false,
		},
		{
			name: "invalid empty table name",
			config: ScanConfig{
				TableName:     "",
				SegmentIDName: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid empty segment id name",
			config: ScanConfig{
				TableName:     "users",
				SegmentIDName: "",
			},
			wantErr: true,
		},
		{
			name: "invalid table name with semicolon (injection attempt)",
			config: ScanConfig{
				TableName:     "users; DROP TABLE users; --",
				SegmentIDName: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid segment id name with backtick",
			config: ScanConfig{
				TableName:     "users",
				SegmentIDName: "id` --",
			},
			wantErr: true,
		},
		{
			name: "invalid table name starting with digit",
			config: ScanConfig{
				TableName:     "1users",
				SegmentIDName: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid table name with hyphen",
			config: ScanConfig{
				TableName:     "user-data",
				SegmentIDName: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid table name with space",
			config: ScanConfig{
				TableName:     "user data",
				SegmentIDName: "id",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
