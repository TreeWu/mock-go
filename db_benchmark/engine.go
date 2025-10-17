package main

import "time"

type BenchmarkEngine interface {
	Init()
	Insert(data []Resource, batchSize int) []BenchmarkResult
	ClearData()
	Search(testData []Resource) []BenchmarkResult
	Close()
	Name() string
}

var (
	ci_type = []int{0, 1, 2, 3, 4, 5, 6, 7}

	Operation_Insert      = "插入"
	Operation_Search      = "搜索"
	Operation_InsertTotal = "插入总耗时"
)

type Resource struct {
	ResourceId   string                 `json:"resource_id" bson:"resource_id"`
	ParentId     string                 `json:"parent_id" bson:"parent_id"`
	Version      int                    `json:"version" bson:"version"`
	Deleted      int                    `json:"deleted" bson:"deleted"`
	Attributes   map[string]interface{} `json:"attributes" bson:"attributes"`
	AttributeStr []byte                 `json:"-" bson:"-"`
	ResourceStr  []byte                 `json:"-"`
}

// 性能测试结果
type BenchmarkResult struct {
	Operation  string        // 操作
	Database   string        // 数据库名
	Duration   time.Duration // 耗时
	Records    int           //插入、搜索条数
	Throughput float64       // 记录数/秒
	Mark       string
}
