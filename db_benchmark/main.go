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
	totalRecords = 10
	batchSize    = 1
	sampleSize   = 1000
	bigmapSize   = 10 * 1024 * 1024 // 10m
	bigMap       map[string]interface{}
	bigMapInsert = false
	valHandler   = value.NewValueHandler()
)

func init() {
	if !bigMapInsert {
		return
	}
	bigMap = generateLargeAttributes(bigmapSize)
}

func main() {

	fmt.Println("开始数据库性能对比测试...")
	fmt.Printf("测试数据量: %d 条记录\n", totalRecords)
	fmt.Println("\n生成测试数据...")
	var testData []Resource

	for i := 0; i*batchSize < totalRecords; i++ {
		for i2 := 1; i2 <= batchSize; i2++ {
			testData = append(testData, generateResource(i, i2, bigMapInsert))
		}
	}

	for i := range testData {
		resource := testData[i]
		resource.AttributeStr, _ = json.Marshal(resource.Attributes)
		resource.ResourceStr, _ = json.Marshal(resource)
		testData[i] = resource
	}

	searchTestData := testData[:min(sampleSize, totalRecords)]

	es, _ := NewElasticsearchEngine(&ElasticsearchConfig{
		Addresses:   []string{"http://localhost:9200"},
		Username:    "", // 如果有认证
		Password:    "", // 如果有认证
		IndexName:   "benchmark",
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

	mongoDB := NewMongoDB("mongodb://root:123456@localhost:27017", "benchmark_db", "resource")

	log.Println(es.Name(), pg.Name(), mongoDB.Name())
	// 初始化数据库引擎
	var engines []BenchmarkEngine

	engines = append(engines,
		es,
	)

	// 执行性能测试
	var allResults []BenchmarkResult

	for _, engine := range engines {
		fmt.Printf("\n=== %s 测试 ===\n", engine.Name())
		engine.Init()

		engine.ClearData()

		insertResults := engine.Insert(testData, batchSize)
		allResults = append(allResults, insertResults...)

		time.Sleep(10 * time.Second)

		searchResults := engine.Search(searchTestData)
		allResults = append(allResults, searchResults...)

		engine.Close()

		time.Sleep(10 * time.Second)
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
		if result.Operation == Operation_InsertTotal {
			bs.WriteString(fmt.Sprintf("%15s 插入完成: %15d 条记录, 耗时: %10v, 吞吐量: %.2f 记录/秒\n",
				result.Database, result.Records, result.Duration, result.Throughput))
		}
	}

	bs.WriteString(fmt.Sprintf(strings.Repeat("=", 50)))
	bs.WriteString("\n")

	for _, result := range results {
		if !strings.Contains(result.Operation, "插入") {
			bs.WriteString(fmt.Sprintf("%-15s %-30s 耗时 %-15v,匹配记录: %d\n", result.Database, result.Operation, result.Duration, result.Records))
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
		Attributes: make(map[string]interface{}),
	}

	m := make(map[string]interface{})
	m["id"] = fmt.Sprintf("%d", id)
	m["resource_id"] = fmt.Sprintf("%d_%d", pid, id)
	m["parent_id"] = fmt.Sprintf("%d", pid)
	m["location"] = fmt.Sprintf("project_root/%d/%d", pid, id)
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
	m["rand_string"] = "@randString"
	if bigMapInsert {
		m["bigmap"] = bigMap
	}
	res.Attributes = valHandler.ProcessDynamicMap(m)
	return res
}

func generateLargeAttributes(targetBytes int) map[string]interface{} {
	root := make(map[string]interface{})
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// helper to create a random string of length n
	randStr := func(n int) string {
		letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, n)
		for i := range b {
			b[i] = letters[rnd.Intn(len(letters))]
		}
		return string(b)
	}

	// create many nested entries
	total := 0
	idx := 0
	for total < targetBytes {
		// create a nested map with several fields
		level1 := fmt.Sprintf("node_%04d", idx)
		nm := make(map[string]interface{})
		nm["meta"] = map[string]interface{}{
			"title":       fmt.Sprintf("Title %d", idx),
			"description": randStr(1024), // 1KB
			"tags":        []string{"big", "test", fmt.Sprintf("idx_%d", idx)},
		}
		// add a deep nested object
		deep := make(map[string]interface{})
		for j := 0; j < 3; j++ {
			deep[fmt.Sprintf("deep_%d", j)] = map[string]interface{}{
				"text": randStr(2048), // 2KB each
				"num":  j,
			}
		}
		nm["deep"] = deep

		// add a large blob-like string to increase size
		blobSize := 16*1024 + rnd.Intn(16*1024) // 16KB ~ 32KB
		nm["blob"] = randStr(blobSize)

		root[level1] = nm

		total += len(level1) + 1024 + 3*(2048+10) + blobSize
		idx++
		// safety upper bound
		if idx > 2000 {
			break
		}
	}
	return root
}
