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
	cities      = []string{"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"}
	departments = []string{"技术部", "销售部", "市场部", "人事部", "财务部", "产品部"}
	positions   = []string{"工程师", "经理", "总监", "专员", "助理", "主管"}
	levels      = []string{"初级", "中级", "高级", "资深"}
	tagsPool    = []string{"活跃", "新用户", "VIP", "优质", "普通", "沉默", "流失"}

	ci_type = []int{0, 1, 2, 3, 4, 5, 6, 7}

	Operation_Insert      = "插入"
	Operation_Search      = "搜索"
	Operation_InsertTotal = "插入总耗时"
)

// 测试数据结构
type User struct {
	ID        int                    `json:"id" bson:"id"`     //索引
	Name      string                 `json:"name" bson:"name"` //索引
	Email     string                 `json:"email" bson:"email"`
	Age       int                    `json:"age" bson:"age"`
	City      string                 `json:"city" bson:"city"`
	Salary    float64                `json:"salary" bson:"salary"`
	CreatedAt time.Time              `json:"created_at" bson:"created_at"`
	Tags      []string               `json:"tags" bson:"tags"`
	Metadata  map[string]interface{} `json:"metadata" bson:"metadata"`
	UserStr   []byte                 `json:"-" bson:"-"`
}

type Resource struct {
	ResourceId string `json:"resource_id" bson:"resource_id"`
	ParentId   string `json:"parent_id" bson:"parent_id"`
	Version    int    `json:"version" bson:"version"`
	Deleted    int    `json:"deleted" bson:"deleted"`
	Attributes string `json:"attributes" bson:"attributes"`
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
