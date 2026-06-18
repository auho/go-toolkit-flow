package dialect

// ScanConfig holds the scan configuration for a dialect.
type ScanConfig struct {
	TableName     string   // table name
	SegmentIDName string   // name of the ID field used for segmentation
	Where         string   // "field1 = ? and field2 = ?"
	Order         string   // "field1 desc"
	SelectFields  []string // list of fields to SELECT
	WhereArgs     []any    // arguments for the Where clause
}
