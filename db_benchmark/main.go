package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

var (
	totalRecords = 1_000_000
	batchSize    = 1000
	sampleSize   = 1000
)

func main() {
	fmt.Println("开始数据库性能对比测试...")
	fmt.Printf("测试数据量: %d 条记录\n", totalRecords)

	// 初始化数据库引擎
	engines := []BenchmarkEngine{
		// NewMySQL("root:123456@tcp(localhost:3306)/?charset=utf8mb4&parseTime=true"),
		NewPostgreSQL("postgres://root:123456@localhost:5432/benchmark_db?sslmode=disable"),
		NewMongoDB("mongodb://root:123456@localhost:27017", "benchmark_db"),
		NewElasticsearch(ElasticsearchConfig{
			Addresses: []string{"http://localhost:9200"},
			Username:  "", // 如果有认证
			Password:  "", // 如果有认证
			IndexName: "users_benchmark",
		}),
	}

	// 初始化所有引擎
	for _, engine := range engines {
		engine.Init()
		defer engine.Close()
	}

	// 生成测试数据
	fmt.Println("\n生成测试数据...")
	testData := generateTestData(totalRecords)
	searchTestData := generateTestData(sampleSize)

	// 执行性能测试
	var allResults []BenchmarkResult

	for _, engine := range engines {
		fmt.Printf("\n=== %s 测试 ===\n", engine.Name())

		// 清理数据
		engine.ClearData()

		// 插入测试
		insertResults := engine.Insert(testData, batchSize)
		allResults = append(allResults, insertResults...)

		// 搜索测试
		searchResults := engine.Search(searchTestData)
		allResults = append(allResults, searchResults...)
	}

	// 输出结果
	printResults(allResults, engines)
}

// 辅助函数
func generateTestData(count int) []User {
	rand.Seed(time.Now().UnixNano())
	var users []User

	for i := 0; i < count; i++ {
		user := generateUser(i + 1)
		users = append(users, user)
	}

	return users
}

func generateUser(id int) User {
	rand.Seed(time.Now().UnixNano() + int64(id))

	user := User{
		ID:        id,
		Name:      fmt.Sprintf("用户%d", id),
		Email:     fmt.Sprintf("user%d@example.com", id),
		Age:       rand.Intn(50) + 18,
		City:      cities[rand.Intn(len(cities))],
		Salary:    float64(rand.Intn(50000) + 30000),
		CreatedAt: time.Now().Add(-time.Duration(rand.Intn(365)) * 24 * time.Hour),
	}

	// 添加标签
	tagCount := rand.Intn(3) + 1
	for j := 0; j < tagCount; j++ {
		user.Tags = append(user.Tags, tagsPool[rand.Intn(len(tagsPool))])
	}

	// 添加元数据
	user.Metadata.Department = departments[rand.Intn(len(departments))]
	user.Metadata.Position = positions[rand.Intn(len(positions))]
	user.Metadata.Level = levels[rand.Intn(len(levels))]

	return user
}

func printResults(results []BenchmarkResult, engines []BenchmarkEngine) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("性能测试结果汇总")
	fmt.Println(strings.Repeat("=", 100))

	fmt.Printf("\n%-20s %-15s %-12s %-10s %-15s\n",
		"操作", "数据库", "耗时", "记录数", "吞吐量(记录/秒)")
	fmt.Println(strings.Repeat("-", 100))

	for _, result := range results {
		fmt.Printf("%-20s %-15s %-12v %-10d %-15.2f\n",
			result.Operation,
			result.Database,
			result.Duration,
			result.Records,
			result.Throughput)
	}

	// 计算性能对比
	fmt.Println("\n性能对比分析:")
	analyzePerformance(results, engines)
}

func analyzePerformance(results []BenchmarkResult, engines []BenchmarkEngine) {
	// 收集各数据库的插入和搜索性能
	insertTimes := make(map[string]time.Duration)
	searchTimes := make(map[string]time.Duration)
	searchCounts := make(map[string]int)

	for _, result := range results {
		if strings.Contains(result.Operation, Operation_InsertTotal) {
			insertTimes[result.Database] = result.Duration
		} else if !strings.Contains(result.Operation, Operation_Insert) {
			searchTimes[result.Database] += result.Duration
			searchCounts[result.Database]++
		}
	}

	// 计算平均搜索时间
	avgSearchTimes := make(map[string]time.Duration)
	for db, totalTime := range searchTimes {
		if count := searchCounts[db]; count > 0 {
			avgSearchTimes[db] = totalTime / time.Duration(count)
		}
	}

	// 输出性能对比
	fmt.Println("\n插入性能排名:")
	rankDatabases(insertTimes, "时间越短越好")

	fmt.Println("\n搜索性能排名:")
	rankDatabases(avgSearchTimes, "时间越短越好")
}

func rankDatabases(times map[string]time.Duration, criteria string) {
	type dbPerformance struct {
		name     string
		duration time.Duration
	}

	var performances []dbPerformance
	for db, duration := range times {
		performances = append(performances, dbPerformance{db, duration})
	}

	// 按耗时排序
	for i := 0; i < len(performances)-1; i++ {
		for j := i + 1; j < len(performances); j++ {
			if performances[i].duration > performances[j].duration {
				performances[i], performances[j] = performances[j], performances[i]
			}
		}
	}

	for i, perf := range performances {
		fmt.Printf("%d. %s: %v\n", i+1, perf.name, perf.duration)
	}
	fmt.Printf("(%s)\n", criteria)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
