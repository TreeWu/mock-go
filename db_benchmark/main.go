package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/TreeWu/mock-go/value"
)

var (
	totalRecords = 100
	batchSize    = 100
	sampleSize   = 10
	bigMapCount  = 1000000
	bigMap       map[string]interface{}
	bigMapInsert = false
	valHandler   = value.NewValueHandler()
)

func init() {
	bigMap = make(map[string]interface{})
	for i := range bigMapCount {
		bigMap[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}
	marshal, _ := json.MarshalIndent(bigMap, "", "  ")
	fmt.Println("bigmap zise mb", len(marshal)/1024/1024)
}

func main() {

	fmt.Println("开始数据库性能对比测试...")
	fmt.Printf("测试数据量: %d 条记录\n", totalRecords)
	fmt.Println("\n生成测试数据...")
	var testData []Resource
	var fileName string
	if bigMapInsert {
		fileName = fmt.Sprintf("bigmap_%d.json", totalRecords)
	} else {
		fileName = fmt.Sprintf("%d.json", totalRecords)
	}

	if bs, err := os.ReadFile(fileName); err == nil {
		err := json.Unmarshal(bs, &testData)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		for i := 0; i*batchSize < totalRecords; i++ {
			for i2 := 1; i2 <= batchSize; i2++ {
				testData = append(testData, generateResource(i, i2, bigMapInsert))
			}
		}
		marshal, _ := json.Marshal(testData)
		os.WriteFile(fileName, marshal, os.ModePerm)
	}

	searchTestData := testData[:min(sampleSize, totalRecords)]

	es, _ := NewElasticsearchEngine(&ElasticsearchConfig{
		Addresses:   []string{"http://localhost:9200"},
		Username:    "", // 如果有认证
		Password:    "", // 如果有认证
		IndexName:   "users_benchmark",
		WithRefresh: "true",
	})
	pg, _ := NewPostgresqlEngine(&PostgresqlConfig{
		Host:            "localhost",
		Port:            5432,
		User:            "root",
		Password:        "123456",
		DBName:          "benchmark_db",
		TableName:       "benchmark_db",
		SSLMode:         "disable",
		MaxConns:        10,
		MinConns:        10,
		MaxConnLifetime: time.Minute,
	})

	// 初始化数据库引擎
	var engines []BenchmarkEngine

	engines = append(engines,
		es,
		NewMongoDB("mongodb://root:123456@localhost:27017", "benchmark_db", "resource"),
		pg)

	// 初始化所有引擎
	for _, engine := range engines {
		engine.Init()
		defer engine.Close()
	}

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

func printResults(results []BenchmarkResult, engines []BenchmarkEngine) {

	var bs bytes.Buffer

	bs.WriteString(fmt.Sprintf("\n" + strings.Repeat("=", 20)))
	bs.WriteString(fmt.Sprintf("性能测试结果汇总"))
	bs.WriteString(fmt.Sprintf(strings.Repeat("=", 20)))

	bs.WriteString(fmt.Sprintf("\n%-20s %-15s %-12s %-10s %-15s\n",
		"操作", "数据库", "耗时", "记录数", "吞吐量(记录/秒)"))
	bs.WriteString(fmt.Sprintf(strings.Repeat("=", 50)))
	bs.WriteString("\n")

	for _, result := range results {
		if !strings.Contains(result.Operation, "插入") {
			bs.WriteString(fmt.Sprintf("%-15s %-30s 耗时 %-15v,匹配记录: %d\n", result.Database, result.Operation, result.Duration, result.Records))
		}
	}

	bs.WriteString(fmt.Sprintf(strings.Repeat("=", 50)))
	bs.WriteString("\n")

	for _, result := range results {
		if result.Operation == Operation_InsertTotal {
			bs.WriteString(fmt.Sprintf("%15s 插入完成: %15d 条记录, 耗时: %10v, 吞吐量: %.2f 记录/秒\n",
				result.Database, result.Records, result.Duration, result.Throughput))
		}
	}

	// 计算性能对比
	fmt.Println("\n性能对比分析:")
	analyzePerformance(results, engines, &bs)

	filename := fmt.Sprintf("%s_%d.txt", time.Now().Format("20060102_150405"), totalRecords)
	if bigMapInsert {
		filename = fmt.Sprintf("big_map_%s_%d.txt", time.Now().Format("20060102_150405"), totalRecords)
	}
	info := bs.Bytes()
	fmt.Println(string(info))
	err := os.WriteFile(filename, info, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
}

func analyzePerformance(results []BenchmarkResult, engines []BenchmarkEngine, bs *bytes.Buffer) {
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
	bs.WriteString("\n插入性能排名:")
	rankDatabases(insertTimes, "时间越短越好", bs)

	bs.WriteString("\n搜索性能排名:")
	rankDatabases(avgSearchTimes, "时间越短越好", bs)

}

func rankDatabases(times map[string]time.Duration, criteria string, bs *bytes.Buffer) {
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
		bs.WriteString(fmt.Sprintf("%d. %s: %v\n", i+1, perf.name, perf.duration))
	}
	bs.WriteString(fmt.Sprintf("(%s)\n", criteria))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func generateResource(pid, id int, bigM bool) Resource {

	res := Resource{
		ResourceId: fmt.Sprintf("%d_%d", pid, id),
		ParentId:   fmt.Sprintf("%d", pid),
		Version:    0,
		Deleted:    0,
		Attributes: "{}",
	}

	m := make(map[string]interface{})
	m["id"] = fmt.Sprintf("%d", id)
	m["resource_id"] = fmt.Sprintf("%d_%d", pid, id)
	m["parent_id"] = fmt.Sprintf("%d", pid)
	m["location"] = "@randString"
	m["input_param"] = "@randString"
	m["name"] = "tom"
	m["value_type"] = "@randString"
	m["spot_type"] = "@randString"
	m["unit"] = "@randString"
	m["precision"] = "@randString"
	m["codec"] = "@randString"
	m["codecex"] = "@randString"
	m["filter"] = "@randString"
	m["compressor"] = "@randString"
	m["mapper"] = "@randString"
	m["converter"] = "@randString"
	m["storag"] = "@randString"
	m["alias"] = "@randString"
	m["ci_type"] = ci_type[rand.Intn(len(ci_type))]
	m["grou"] = "@randString"
	m["data_source"] = "@randString"
	m["privilege"] = "@randString"
	m["aggregato"] = "@randString"
	m["ci_version"] = "@randString"
	if bigMapInsert {
		m["bigmap"] = bigMap
	}
	v := valHandler.ProcessDynamicValues(m)

	bs, _ := json.Marshal(v)
	res.Attributes = string(bs)
	return res
}
