// elasticsearch_engine.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"log"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"
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

	for i := 0; i < len(data); i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		// 使用 Bulk API 进行批量插入
		err := e.BulkInsert(batch)
		if err != nil {
			log.Printf("Elasticsearch 批量插入失败: %v", err)
			continue
		}

		batchDuration := time.Since(batchStart)
		batchResult := BenchmarkResult{
			Operation:  Operation_Insert,
			Database:   e.Name(),
			Duration:   batchDuration,
			Records:    len(batch),
			Throughput: float64(len(batch)) / batchDuration.Seconds(),
		}
		results = append(results, batchResult)

		if i%1000 == 0 {
			fmt.Printf("%s 已插入 %d 条记录\n", e.Name(), batchEnd)
		}
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

	// 创建索引
	engine.createIndex()
	return engine, nil
}

// createIndex 创建索引
func (e *ElasticsearchEngine) createIndex() {
	// 检查索引是否存在
	res, err := e.client.Indices.Exists([]string{e.indexName})
	if err != nil {
		log.Fatalf("检查索引存在失败: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		// 索引已存在，清理数据
		_, err := e.client.DeleteByQuery([]string{e.indexName}, strings.NewReader(`{"query":{"match_all":{}}}`))
		if err != nil {
			log.Printf("清理现有索引数据失败: %v", err)
		}
		return
	}

	// 索引映射配置
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"refresh_interval": "1s"
		},
		"mappings": {
			"properties": {
				"resource_id": {"type": "keyword"},
				"parent_id": {"type": "keyword"},
				"version": {"type": "integer"},
				"deleted": {"type": "integer"},
				"attributes": {
					"type": "object",
					"dynamic": true
				},
			}
		}
	}`

	req := esapi.IndicesCreateRequest{
		Index: e.indexName,
		Body:  strings.NewReader(mapping),
	}

	res, err = req.Do(context.Background(), e.client)
	if err != nil {
		log.Fatalf("创建索引失败: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("创建索引错误: %s", res.String())
	}
}

// BulkInsert 批量插入数据
func (e *ElasticsearchEngine) BulkInsert(resources []Resource) error {
	var buf bytes.Buffer

	for _, resource := range resources {

		// 构建文档
		document := map[string]interface{}{
			"resource_id": resource.ResourceId,
			"parent_id":   resource.ParentId,
			"version":     resource.Version,
			"deleted":     resource.Deleted,
			"attributes":  resource.Attributes,
		}

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

		docJSON, err := json.Marshal(document)
		if err != nil {
			return err
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')
		buf.Write(docJSON)
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

			res, err := e.client.Search(
				e.client.Search.WithIndex(e.indexName),
				e.client.Search.WithBody(strings.NewReader(string(queryJSON))),
				e.client.Search.WithSize(1_000),
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
			if hits, ok := searchResult["hits"].(map[string]interface{}); ok {
				if total, ok := hits["total"].(map[string]interface{}); ok {
					if value, ok := total["value"].(float64); ok {
						hitCount = int(value)
					}
				}
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
	// 使用 DeleteByQuery 删除所有文档
	query := `{"query":{"match_all":{}}}`

	res, err := e.client.DeleteByQuery(
		[]string{e.config.IndexName},
		strings.NewReader(query),
		e.client.DeleteByQuery.WithRefresh(true),
	)
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
