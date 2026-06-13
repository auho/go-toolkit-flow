package dialect

// ScanConfig 扫描配置
type ScanConfig struct {
	TableName     string   // 表名
	SegmentIDName string   // 用于分段的 ID 字段名
	Where         string   // "field1 = ? and field2 = ?"
	Order         string   // "field1 desc"
	SelectFields  []string // 要 SELECT 的字段列表
	WhereArgs     []any    // Where 子句参数
}
