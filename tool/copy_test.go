package tool

import (
	"reflect"
	"testing"
)

func TestCopySliceMap(t *testing.T) {
	tests := []struct {
		name  string
		items []map[string]int
	}{
		{"normal", []map[string]int{{"a": 1, "b": 2}, {"c": 3}}},
		{"single", []map[string]int{{"a": 1}}},
		{"empty", []map[string]int{}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CopySliceMap(tt.items)

			// nil 输入应返回空切片（非 nil），符合函数实现
			if tt.items == nil {
				if got == nil {
					t.Fatalf("expected non-nil empty slice, got nil")
				}
				if len(got) != 0 {
					t.Fatalf("expected len 0, got %d", len(got))
				}
				return
			}

			if len(got) != len(tt.items) {
				t.Fatalf("len mismatch: got %d, want %d", len(got), len(tt.items))
			}

			if !reflect.DeepEqual(got, tt.items) {
				t.Fatalf("content mismatch: got %v, want %v", got, tt.items)
			}
		})
	}
}

func TestCopySliceMap_Isolation(t *testing.T) {
	original := []map[string]int{{"a": 1}, {"b": 2}}
	got := CopySliceMap(original)

	// 修改原 map，副本不应受影响
	original[0]["a"] = 100
	original[1]["b"] = 200

	if got[0]["a"] != 1 {
		t.Errorf("isolation failed: got[0][a] = %d, want 1", got[0]["a"])
	}
	if got[1]["b"] != 2 {
		t.Errorf("isolation failed: got[1][b] = %d, want 2", got[1]["b"])
	}
}

func TestCopySliceSlice(t *testing.T) {
	tests := []struct {
		name  string
		items [][]int
	}{
		{"normal", [][]int{{1, 2}, {3, 4}}},
		{"single", [][]int{{1}}},
		{"empty", [][]int{}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CopySliceSlice(tt.items)

			// nil 输入应返回空切片（非 nil），符合函数实现
			if tt.items == nil {
				if got == nil {
					t.Fatalf("expected non-nil empty slice, got nil")
				}
				if len(got) != 0 {
					t.Fatalf("expected len 0, got %d", len(got))
				}
				return
			}

			if len(got) != len(tt.items) {
				t.Fatalf("len mismatch: got %d, want %d", len(got), len(tt.items))
			}

			if !reflect.DeepEqual(got, tt.items) {
				t.Fatalf("content mismatch: got %v, want %v", got, tt.items)
			}
		})
	}
}

func TestCopySliceSlice_Isolation(t *testing.T) {
	original := [][]int{{1, 2}, {3, 4}}
	got := CopySliceSlice(original)

	// 修改原内层 slice，副本不应受影响
	original[0][0] = 100
	original[1][1] = 200

	if got[0][0] != 1 {
		t.Errorf("isolation failed: got[0][0] = %d, want 1", got[0][0])
	}
	if got[1][1] != 4 {
		t.Errorf("isolation failed: got[1][1] = %d, want 4", got[1][1])
	}
}
