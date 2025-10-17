// elasticsearch_engine.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"golang.org/x/sync/errgroup"
	"log"
	"strings"
	"time"
)

var _ BenchmarkEngine = (*ElasticsearchEngine)(nil)

// ElasticsearchEngine 结构体
type ElasticsearchEngine struct {
	client    *elasticsearch.Client
	config    *ElasticsearchConfig
	indexName string
}

func (e *ElasticsearchEngine) Insert(data []Resource, batchSize int) []BenchmarkResult {

	// 创建索引
	e.createIndex()

	var results []BenchmarkResult
	start := time.Now()
	group := errgroup.Group{}
	group.SetLimit(6)

	for i := 0; i < len(data); i += batchSize {
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		// 使用 Bulk API 进行批量插入
		group.Go(func() error {
			log.Printf("%s 批量插入数据开始: %d 条记录", e.Name(), batchEnd)
			return e.BulkInsert(batch)
		})
	}
	err := group.Wait()
	if err != nil {
		log.Printf("%s 批量插入数据失败: %v", e.Name(), err)
		return nil
	}
	totalDuration := time.Since(start)
	totalResult := BenchmarkResult{
		Operation:  Operation_InsertTotal,
		Database:   e.Name(),
		Duration:   totalDuration,
		Records:    len(data),
		Throughput: float64(len(data)) / totalDuration.Seconds(),
	}

	fmt.Printf("%s 插入完成: %d 条记录, 耗时: %v, 吞吐量: %.2f 记录/秒\n",
		e.Name(), len(data), totalDuration, totalResult.Throughput)

	return append(results, totalResult)
}

// ElasticsearchConfig 配置
type ElasticsearchConfig struct {
	Addresses   []string
	IndexName   string
	Username    string
	Password    string
	WithRefresh string
}

func (e *ElasticsearchEngine) Init() {
	cfg := elasticsearch.Config{
		Addresses: e.config.Addresses,
		Username:  e.config.Username,
		Password:  e.config.Password,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("创建 Elasticsearch 客户端失败: %v", err)
	}

	e.client = client

	// 检查连接
	res, err := e.client.Ping()
	if err != nil {
		log.Fatalf("Elasticsearch 连接失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Elasticsearch 连接异常: %s", res.String())
	}

	fmt.Println("Elasticsearch 初始化成功")
}

// NewElasticsearchEngine 创建新的引擎实例
func NewElasticsearchEngine(config *ElasticsearchConfig) (*ElasticsearchEngine, error) {
	cfg := elasticsearch.Config{
		Addresses: config.Addresses,
		Username:  config.Username,
		Password:  config.Password,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	engine := &ElasticsearchEngine{
		client:    client,
		config:    config,
		indexName: config.IndexName,
	}

	return engine, nil
}

// createIndex 创建索引
func (e *ElasticsearchEngine) createIndex() {

	// delete old index if exists (for testing convenience)
	e.client.Indices.Delete([]string{e.indexName})

	settings := map[string]interface{}{
		"settings": map[string]interface{}{
			"index.mapping.total_fields.limit": 20000,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"resource_id": map[string]interface{}{"type": "keyword"},
				"parent_id":   map[string]interface{}{"type": "keyword"},
				"version":     map[string]interface{}{"type": "integer"},
				"deleted":     map[string]interface{}{"type": "integer"},
				"attributes": map[string]interface{}{
					"type":    "object",
					"dynamic": true, // 允许自动生成子字段
				},
			},
		},
	}

	body, _ := json.Marshal(settings)
	res, err := e.client.Indices.Create(e.indexName, e.client.Indices.Create.WithBody(bytes.NewReader(body)))
	if err != nil {
		log.Fatalf("创建索引失败: %v", err)
	}
	defer res.Body.Close()
	fmt.Println("index created with high field limit (20000)")

}

// BulkInsert 批量插入数据
func (e *ElasticsearchEngine) BulkInsert(resources []Resource) error {
	var buf bytes.Buffer

	for _, resource := range resources {

		// 构建批量请求
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": e.indexName,
				"_id":    resource.ResourceId,
			},
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')
		buf.Write(resource.ResourceStr)
		buf.WriteByte('\n')
	}

	// 执行批量插入
	res, err := e.client.Bulk(
		strings.NewReader(buf.String()),
		e.client.Bulk.WithRefresh(e.config.WithRefresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("批量插入错误: %s", res.String())
	}

	return nil
}

// Search 执行搜索测试，多次执行取平均值
func (e *ElasticsearchEngine) Search(test []Resource) []BenchmarkResult {
	var results []BenchmarkResult

	var randStr []string
	for t := range test {
		randStr = append(randStr, test[t].Attributes["rand_string"].(string))
	}

	// 定义测试用例
	testCases := []struct {
		name        string
		description string
		query       map[string]interface{}
	}{
		{
			name:        "resource_id精准匹配",
			description: "根据resource_id精确匹配特定资源",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"term": map[string]interface{}{
						"resource_id": test[0].ResourceId,
					},
				},
			},
		},
		{
			name:        "resource_id模糊匹配",
			description: "使用通配符匹配resource_id，如%0_1_0%",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"wildcard": map[string]interface{}{
						"resource_id": "*" + test[0].ResourceId + "*",
					},
				},
			},
		},
		{
			name:        "attributes.ci_type精准匹配",
			description: "根据attributes中的ci_type字段精确匹配",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"term": map[string]interface{}{
						"attributes.ci_type": 2,
					},
				},
			},
		},
		{
			name:        "attributes.ci_type包含多个值",
			description: "匹配attributes.ci_type在指定数组中的资源",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"terms": map[string]interface{}{
						"attributes.ci_type": []int{2, 3, 4},
					},
				},
			},
		},
		{
			name:        "attributes.ci_type不包含多个值",
			description: "匹配attributes.ci_type不在指定数组中的资源",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must_not": map[string]interface{}{
							"terms": map[string]interface{}{
								"attributes.ci_type": []int{2, 3, 4},
							},
						},
					},
				},
			},
		},
		{
			name:        "attributes.location like 搜索",
			description: "attributes.location like 搜索",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"wildcard": map[string]interface{}{
						"attributes.location": "*project_root*",
					},
				},
			},
		},

		{
			name:        "attributes.rand_string in 搜索",
			description: "attributes.rand_string in 搜索",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"terms": map[string]interface{}{
						"attributes.rand_string.keyword": randStr,
					},
				},
			},
		},
	}

	// 执行每个测试用例，多次执行取平均值
	for _, tc := range testCases {
		const executionCount = 5 // 每个测试用例执行5次
		var totalDuration time.Duration
		var totalRecord int
		var lastError error
		var successCount int

		// 执行多次搜索
		for i := 0; i < executionCount; i++ {
			start := time.Now()

			queryJSON, err := json.Marshal(tc.query)
			if err != nil {
				lastError = err
				continue
			}

			res, err := e.client.Count(
				e.client.Count.WithIndex(e.indexName),
				e.client.Count.WithBody(strings.NewReader(string(queryJSON))),
			)

			duration := time.Since(start)

			if err != nil {
				lastError = err
				continue
			}

			var searchResult map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
				lastError = err
				res.Body.Close()
				continue
			}

			res.Body.Close()

			// 提取命中数量
			var hitCount int
			if _, ok := searchResult["count"].(float64); ok {
				hitCount = int(searchResult["count"].(float64))
			}

			totalDuration += duration
			totalRecord += hitCount
			successCount++
		}

		// 计算平均值
		var avgDuration time.Duration
		var avgRecords int
		var throughput float64
		mark := "成功"

		if successCount > 0 {
			avgDuration = totalDuration / time.Duration(successCount)
			avgRecords = totalRecord / successCount
			if avgDuration > 0 {
				throughput = float64(avgRecords) / avgDuration.Seconds()
			}
		} else {
			mark = fmt.Sprintf("所有执行都失败: %v", lastError)
		}

		// 添加成功率信息
		if successCount < executionCount {
			mark = fmt.Sprintf("部分成功 (%d/%d)", successCount, executionCount)
			if lastError != nil {
				mark += fmt.Sprintf("，最后错误: %v", lastError)
			}
		}

		results = append(results, BenchmarkResult{
			Operation:  tc.name,
			Database:   e.Name(),
			Duration:   avgDuration,
			Records:    avgRecords,
			Throughput: throughput,
			Mark:       mark,
		})
	}

	return results
}

func (e *ElasticsearchEngine) ClearData() {

	res, err := e.client.Indices.Delete([]string{e.config.IndexName})
	if err != nil {
		return
	}

	if err != nil {
		log.Printf("%s 清理数据失败: %v", e.Name(), err)
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("%s 清理数据错误: %s", e.Name(), res.String())
		return
	}

	fmt.Printf("%s 数据清理完成\n", e.Name())
}

func (e *ElasticsearchEngine) Close() {
}

func (e *ElasticsearchEngine) Name() string {
	return "Elasticsearch"
}
