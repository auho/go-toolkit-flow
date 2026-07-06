package dialect

import (
	"fmt"
	"regexp"
)

var identifierRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// WriteConfig holds the write configuration.
type WriteConfig struct {
	TableName string
}

// Validate checks that TableName is a valid SQL identifier
// (only [A-Za-z_][A-Za-z0-9_]* allowed) to prevent SQL identifier injection.
func (c WriteConfig) Validate() error {
	if !identifierRegex.MatchString(c.TableName) {
		return fmt.Errorf("invalid table name %q: must match [A-Za-z_][A-Za-z0-9_]*", c.TableName)
	}
	return nil
}
