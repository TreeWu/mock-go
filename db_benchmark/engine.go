package main

import "time"

type BenchmarkEngine interface {
	Init()
	Insert(data []User, batchSize int) []BenchmarkResult
	ClearData()
	Search(testData []User) []BenchmarkResult
	Close()
	Name() string
}

var (
	cities      = []string{"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"}
	departments = []string{"技术部", "销售部", "市场部", "人事部", "财务部", "产品部"}
	positions   = []string{"工程师", "经理", "总监", "专员", "助理", "主管"}
	levels      = []string{"初级", "中级", "高级", "资深"}
	tagsPool    = []string{"活跃", "新用户", "VIP", "优质", "普通", "沉默", "流失"}

	Operation_Insert      = "插入"
	Operation_Search      = "搜索"
	Operation_InsertTotal = "插入总耗时"
)

// 测试数据结构
type User struct {
	ID        int       `json:"id" bson:"id"`
	Name      string    `json:"name" bson:"name"`
	Email     string    `json:"email" bson:"email"`
	Age       int       `json:"age" bson:"age"`
	City      string    `json:"city" bson:"city"`
	Salary    float64   `json:"salary" bson:"salary"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
	Tags      []string  `json:"tags" bson:"tags"`
	Metadata  struct {
		Department string `json:"department" bson:"department"`
		Position   string `json:"position" bson:"position"`
		Level      string `json:"level" bson:"level"`
	} `json:"metadata" bson:"metadata"`
}

// 性能测试结果
type BenchmarkResult struct {
	Operation  string
	Database   string
	Duration   time.Duration
	Records    int
	Throughput float64 // 记录数/秒
}
