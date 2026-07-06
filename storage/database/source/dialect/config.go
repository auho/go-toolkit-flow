package dialect

import (
	"fmt"
	"regexp"
)

var identifierRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ScanConfig holds the scan configuration for a dialect.
type ScanConfig struct {
	TableName     string   // table name
	SegmentIDName string   // name of the ID field used for segmentation
	Where         string   // "field1 = ? and field2 = ?"
	Order         string   // "field1 desc"
	SelectFields  []string // list of fields to SELECT
	WhereArgs     []any    // arguments for the Where clause
}

// Validate checks that TableName and SegmentIDName are valid SQL identifiers
// (only [A-Za-z_][A-Za-z0-9_]* allowed) to prevent SQL identifier injection.
func (c ScanConfig) Validate() error {
	if !identifierRegex.MatchString(c.TableName) {
		return fmt.Errorf("invalid table name %q: must match [A-Za-z_][A-Za-z0-9_]*", c.TableName)
	}
	if !identifierRegex.MatchString(c.SegmentIDName) {
		return fmt.Errorf("invalid segment id name %q: must match [A-Za-z_][A-Za-z0-9_]*", c.SegmentIDName)
	}
	return nil
}
