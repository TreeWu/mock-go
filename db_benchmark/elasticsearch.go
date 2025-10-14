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

// ElasticsearchEngine Elasticsearch 实现
type ElasticsearchEngine struct {
	client *elasticsearch.Client
	config ElasticsearchConfig
	name   string
}

type ElasticsearchConfig struct {
	Addresses []string
	Username  string
	Password  string
	IndexName string
}

func NewElasticsearch(config ElasticsearchConfig) *ElasticsearchEngine {
	return &ElasticsearchEngine{
		config: config,
		name:   "Elasticsearch",
	}
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

	// 创建索引
	e.createIndex()

	fmt.Println("Elasticsearch 初始化成功")
}
func (e *ElasticsearchEngine) createIndex() {
	// 检查索引是否存在
	res, err := e.client.Indices.Exists([]string{e.config.IndexName})
	if err != nil {
		log.Fatalf("检查索引存在失败: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		// 索引已存在，清理数据
		_, err := e.client.DeleteByQuery([]string{e.config.IndexName}, strings.NewReader(`{"query":{"match_all":{}}}`))
		if err != nil {
			log.Printf("清理现有索引数据失败: %v", err)
		}
		return
	}

	// 创建与 PostgreSQL 结构对应的索引映射
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"refresh_interval": "1s",
			"translog": {
				"durability": "request"  // 每次请求都刷写事务日志
			}
		},
		"mappings": {
			"properties": {
				"id": {"type": "long"},
				"name": {"type": "text", "fielddata": true},
				"email": {"type": "text"},
				"age": {"type": "integer"},
				"city": {"type": "text"},
				"salary": {"type": "double"},
				"created_at": {"type": "date"},
				"datastr": {"type": "text"}
			}
		}
	}`

	req := esapi.IndicesCreateRequest{
		Index: e.config.IndexName,
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

func (e *ElasticsearchEngine) Insert(data []User, batchSize int) []BenchmarkResult {
	var results []BenchmarkResult
	start := time.Now()

	for i := 0; i < len(data); i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		// 使用 Bulk API 进行批量插入
		err := e.bulkInsert(batch)
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

		if i%100000 == 0 {
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

func (e *ElasticsearchEngine) bulkInsert(users []User) error {
	var buf bytes.Buffer

	for _, user := range users {
		// 将完整用户数据序列化为 JSON 字符串，模拟 PostgreSQL 的 datastr JSONB 字段

		// 创建与 PostgreSQL 对应的文档结构
		document := map[string]interface{}{
			"id":         user.ID,
			"name":       user.Name,
			"email":      user.Email,
			"age":        user.Age,
			"city":       user.City,
			"salary":     user.Salary,
			"created_at": user.CreatedAt,
			"datastr":    user.UserStr,
		}

		// 元数据行
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": e.config.IndexName,
				"_id":    user.ID,
			},
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')

		// 数据行
		docJSON, err := json.Marshal(document)
		if err != nil {
			return err
		}

		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	// 执行批量插入
	res, err := e.client.Bulk(
		strings.NewReader(buf.String()),
		e.client.Bulk.WithRefresh("true"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("批量插入错误: %s", res.String())
	}

	// 解析响应检查错误
	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return err
	}

	if response["errors"] != nil && response["errors"].(bool) {
		// 记录错误但不停止执行
		log.Printf("批量插入中存在部分错误: %v", response)
	}

	return nil
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

func (e *ElasticsearchEngine) Search(testData []User) []BenchmarkResult {
	var results []BenchmarkResult

	// 测试不同的搜索场景 - 与 PostgreSQL 保持一致
	searchTests := []struct {
		name        string
		description string
		query       map[string]interface{}
	}{
		{
			name:        "精确匹配搜索",
			description: "按姓名精确匹配",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"term": map[string]interface{}{
						"name": testData[0].Name,
					},
				},
			},
		},
		{
			name:        "范围搜索",
			description: "年龄在25-35岁之间",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"range": map[string]interface{}{
						"age": map[string]interface{}{
							"gte": 25,
							"lte": 35,
						},
					},
				},
			},
		},
		{
			name:        "城市筛选",
			description: "按城市筛选",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"term": map[string]interface{}{
						"city": cities[0],
					},
				},
			},
		},
		{
			name:        "薪资范围搜索",
			description: "薪资在40000-60000之间",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"range": map[string]interface{}{
						"salary": map[string]interface{}{
							"gte": 40000,
							"lte": 60000,
						},
					},
				},
			},
		},

		{
			name:        "JSON字段搜索",
			description: "搜索datastr JSON字段中的内容",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"match_phrase": map[string]interface{}{
						"datastr": departments[0], // 搜索 datastr 字段中的部门信息
					},
				},
			},
		},
		{
			name:        "复杂条件搜索",
			description: "城市+年龄+薪资组合查询",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{
								"term": map[string]interface{}{
									"city": cities[0],
								},
							},
							{
								"range": map[string]interface{}{
									"age": map[string]interface{}{
										"gt": 30,
									},
								},
							},
							{
								"range": map[string]interface{}{
									"salary": map[string]interface{}{
										"gt": 50000,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "全文搜索",
			description: "全文搜索",
			query: map[string]interface{}{
				"query": map[string]interface{}{
					"match": map[string]interface{}{
						"datastr": "用户",
					},
				},
			},
		},
	}

	for _, test := range searchTests {
		start := time.Now()
		var count int64

		// 执行多次取平均值
		iterations := 10
		for i := 0; i < iterations; i++ {
			// 序列化查询
			queryJSON, err := json.Marshal(test.query)
			if err != nil {
				log.Printf("%s 序列化查询失败: %v", e.Name(), err)
				continue
			}

			// 执行搜索
			res, err := e.client.Count(
				e.client.Count.WithIndex(e.config.IndexName),
				e.client.Count.WithBody(strings.NewReader(string(queryJSON))),
			)
			if err != nil {
				log.Printf("%s 搜索失败: %v", e.Name(), err)
				continue
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("%s 搜索错误: %s", e.Name(), res.String())
				continue
			}

			// 解析结果
			var result map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
				log.Printf("%s 解析结果失败: %v", e.Name(), err)
				continue
			}

			if cnt, ok := result["count"].(float64); ok {
				count = int64(cnt)
			}
		}

		duration := time.Since(start) / time.Duration(iterations)
		result := BenchmarkResult{
			Operation:  test.name,
			Database:   e.Name(),
			Duration:   duration,
			Records:    int(count),
			Throughput: 1.0 / duration.Seconds(), // 查询/秒
		}
		results = append(results, result)

		fmt.Printf("%s %s: 耗时 %v, 匹配记录: %d\n", e.Name(), test.name, duration, count)
	}

	return results
}
func (e *ElasticsearchEngine) Close() {
}

func (e *ElasticsearchEngine) Name() string {
	return e.name
}
