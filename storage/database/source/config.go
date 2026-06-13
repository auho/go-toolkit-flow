package source

// SectionConfig 分段扫描基础配置
type SectionConfig struct {
	Concurrency int
	MaxItems    int64 // 最多读取的记录数，0 表示不限制
	StartID     int64 // 起始 ID（闭区间）
	EndID       int64 // 结束 ID（闭区间），0 表示自动检测
	PageSize    int64 // 每页大小
}
