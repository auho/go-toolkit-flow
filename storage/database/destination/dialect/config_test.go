package dialect

import (
	"testing"
)

func TestWriteConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  WriteConfig
		wantErr bool
	}{
		{
			name:    "valid simple identifier",
			config:  WriteConfig{TableName: "users"},
			wantErr: false,
		},
		{
			name:    "valid with underscore and digits",
			config:  WriteConfig{TableName: "user_data_2"},
			wantErr: false,
		},
		{
			name:    "valid starting with underscore",
			config:  WriteConfig{TableName: "_users"},
			wantErr: false,
		},
		{
			name:    "valid mixed case",
			config:  WriteConfig{TableName: "UsersTable"},
			wantErr: false,
		},
		{
			name:    "invalid empty",
			config:  WriteConfig{TableName: ""},
			wantErr: true,
		},
		{
			name:    "invalid with semicolon (injection attempt)",
			config:  WriteConfig{TableName: "users; DROP TABLE users; --"},
			wantErr: true,
		},
		{
			name:    "invalid with backtick",
			config:  WriteConfig{TableName: "users` --"},
			wantErr: true,
		},
		{
			name:    "invalid starting with digit",
			config:  WriteConfig{TableName: "1users"},
			wantErr: true,
		},
		{
			name:    "invalid with hyphen",
			config:  WriteConfig{TableName: "user-data"},
			wantErr: true,
		},
		{
			name:    "invalid with space",
			config:  WriteConfig{TableName: "user data"},
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
