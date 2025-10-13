package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

var _ BenchmarkEngine = (*MongoDB)(nil)

type MongoDB struct {
	db     string
	uri    string
	client *mongo.Client
}

func (m *MongoDB) Name() string {
	return "MongoDB"
}

func NewMongoDB(uri, db string) BenchmarkEngine {
	return &MongoDB{
		uri: uri,
		db:  db,
	}
}

func (m *MongoDB) Init() {
	clientOptions := options.Client().ApplyURI(m.uri)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal("连接 MongoDB 失败:", err)
	}
	// 检查连接
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal("MongoDB 连接测试失败:", err)
	}
	fmt.Println("MongoDB 连接成功")
	m.client = client
}

func (m *MongoDB) Insert(users []User, batchSize int) []BenchmarkResult {
	var results []BenchmarkResult
	collection := m.client.Database(m.db).Collection("users")
	start := time.Now()

	// 创建索引
	_, err := collection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{Keys: bson.D{{"id", 1}}},
		{Keys: bson.D{{"name", 1}}},
		{Keys: bson.D{{"metadata.department", 1}}},
	})
	if err != nil {
		log.Printf("创建 MongoDB 索引失败: %v", err)
	}
	total := len(users)

	for i := 0; i < total; i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, total)

		batch := users[i:batchEnd]
		var documents = make([]interface{}, len(batch))
		for i2 := range batch {
			documents[i2] = batch[i2]
		}

		_, err := collection.InsertMany(context.Background(), documents)
		if err != nil {
			log.Printf("MongoDB 批量插入失败: %v", err)
			continue
		}

		batchDuration := time.Since(batchStart)
		batchResult := BenchmarkResult{
			Operation:  Operation_Insert,
			Database:   m.Name(),
			Duration:   batchDuration,
			Records:    batchEnd - i,
			Throughput: float64(batchEnd-i) / batchDuration.Seconds(),
		}
		results = append(results, batchResult)

		if i%100000 == 0 {
			fmt.Printf("MongoDB 已插入 %d 条记录\n", batchEnd)
		}
	}

	totalDuration := time.Since(start)
	totalResult := BenchmarkResult{
		Operation:  Operation_InsertTotal,
		Database:   m.Name(),
		Duration:   totalDuration,
		Records:    total,
		Throughput: float64(total) / totalDuration.Seconds(),
	}

	fmt.Printf("MongoDB 插入完成: %d 条记录, 耗时: %v, 吞吐量: %.2f 记录/秒\n",
		total, totalDuration, totalResult.Throughput)

	return append(results, totalResult)
}

func (m *MongoDB) ClearData() {
	fmt.Println("MongoDB,清理测试数据...")

	// 清理 MongoDB
	collection := m.client.Database(m.db).Collection("users")
	err := collection.Drop(context.Background())
	if err != nil {
		log.Printf("清理 MongoDB 数据失败: %v", err)
	}
}

func (m *MongoDB) Search(testData []User) []BenchmarkResult {
	var results []BenchmarkResult
	collection := m.client.Database(m.db).Collection("users")

	// 测试不同的搜索场景
	searchTests := []struct {
		name     string
		pipeline []bson.D
	}{
		{
			name: "姓名搜索_索引",
			pipeline: []bson.D{
				{{"$match", bson.D{{"name", testData[0].Name}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "年龄范围搜索_索引",
			pipeline: []bson.D{
				{{"$match", bson.D{{"age", bson.D{{"$gte", 25}, {"$lte", 35}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "城市筛选",
			pipeline: []bson.D{
				{{"$match", bson.D{{"city", cities[0]}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "薪资范围搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{{"salary", bson.D{{"$gte", 40000}, {"$lte", 60000}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "JSON字段搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{{"metadata.department", departments[0]}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "复杂条件搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{
					{"city", cities[0]},
					{"age", bson.D{{"$gt", 30}}},
					{"salary", bson.D{{"$gt", 50000}}},
				}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "全文搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{{"name", bson.D{{"$regex", "用户"}}}}}},
				{{"$count", "total"}},
			},
		},
	}

	for _, test := range searchTests {
		start := time.Now()
		var count int64

		// 执行多次取平均值
		iterations := 10
		for i := 0; i < iterations; i++ {
			cursor, err := collection.Aggregate(context.Background(), test.pipeline)
			if err != nil {
				log.Printf("MongoDB 搜索失败: %v", err)
				continue
			}

			var result []bson.M
			if err = cursor.All(context.Background(), &result); err != nil {
				log.Printf("MongoDB 解析结果失败: %v", err)
				continue
			}

			if len(result) > 0 {
				if totalVal, ok := result[0]["total"]; ok {
					switch v := totalVal.(type) {
					case int32:
						count = int64(v)
					case int64:
						count = v
					case float64:
						count = int64(v)
					case int:
						count = int64(v)
					}
				}
			}
			cursor.Close(context.Background())
		}

		duration := time.Since(start) / time.Duration(iterations)
		result := BenchmarkResult{
			Operation:  test.name,
			Database:   m.Name(),
			Duration:   duration,
			Records:    int(count),
			Throughput: 1.0 / duration.Seconds(), // 查询/秒
		}
		results = append(results, result)

		fmt.Printf("MongoDB %s: 耗时 %v, 匹配记录: %d\n", test.name, duration, count)
	}

	return results
}

func (m *MongoDB) Close() {
	m.client.Disconnect(context.Background())
}
