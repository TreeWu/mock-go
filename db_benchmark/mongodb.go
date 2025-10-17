package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

var _ BenchmarkEngine = (*MongoDB)(nil)

type MongoDB struct {
	db         string
	uri        string
	client     *mongo.Client
	Collection string
}

func (m *MongoDB) Name() string {
	return "MongoDB"
}

func NewMongoDB(uri, db, Collection string) BenchmarkEngine {
	return &MongoDB{
		uri:        uri,
		db:         db,
		Collection: Collection,
	}
}

func (m *MongoDB) Test() {

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

func (m *MongoDB) Insert(data []Resource, batchSize int) []BenchmarkResult {

	collection := m.client.Database(m.db).Collection(m.Collection)

	_, err := collection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{Keys: bson.D{{"resource_id", 1}}},
		{
			Keys: bson.D{
				{"resource_id", "text"},
				{"parent_id", "text"},
				{"attributes", "text"},
			},
		},
	})
	if err != nil {
		log.Printf("创建 MongoDB 索引失败: %v", err)
	}
	var results []BenchmarkResult
	start := time.Now()

	collection = m.client.Database(m.db).Collection(m.Collection)

	group := errgroup.Group{}
	group.SetLimit(6)

	for i := 0; i < len(data); i += batchSize {
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		group.Go(func() error {
			log.Printf("%s 批量插入数据开始: %d 条记录", m.Name(), batchEnd)

			var documents []interface{}
			for _, resource := range batch {
				doc := bson.M{
					"resource_id": resource.ResourceId,
					"parent_id":   resource.ParentId,
					"version":     resource.Version,
					"deleted":     resource.Deleted,
					"attributes":  resource.Attributes,
				}
				documents = append(documents, doc)
			}

			_, err := collection.InsertMany(context.Background(), documents)
			if err != nil {
				log.Printf("MongoDB 批量插入失败: %v", err)
			}
			return err
		})
	}
	err = group.Wait()
	if err != nil {
		log.Printf("MongoDB 批量插入失败: %v", err)
		return nil
	}
	totalDuration := time.Since(start)
	totalResult := BenchmarkResult{
		Operation:  Operation_InsertTotal,
		Database:   m.Name(),
		Duration:   totalDuration,
		Records:    len(data),
		Throughput: float64(len(data)) / totalDuration.Seconds(),
	}

	fmt.Printf("%s 插入完成: %d 条记录, 耗时: %v, 吞吐量: %.2f 记录/秒\n",
		m.Name(), len(data), totalDuration, totalResult.Throughput)

	return append(results, totalResult)
}

func (m *MongoDB) ClearData() {
	collection := m.client.Database(m.db).Collection(m.Collection)
	_, err := collection.DeleteMany(context.Background(), bson.D{})
	if err != nil {
		log.Printf("MongoDB 清理数据失败: %v", err)
	}
}

func (m *MongoDB) Search(test []Resource) []BenchmarkResult {
	var results []BenchmarkResult
	collection := m.client.Database(m.db).Collection(m.Collection)

	var randStr []string
	for t := range test {
		randStr = append(randStr, test[t].Attributes["rand_string"].(string))
	}

	searchTests := []struct {
		name     string
		pipeline []bson.D
	}{
		{
			name: "resource_id精准匹配",
			pipeline: []bson.D{
				{{"$match", bson.D{{"resource_id", test[0].ResourceId}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "resource_id模糊匹配",
			pipeline: []bson.D{
				{{"$match", bson.D{{"resource_id", bson.D{{"$regex", test[0].ResourceId}, {"$options", "i"}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "attributes.ci_type精准匹配",
			pipeline: []bson.D{
				{{"$match", bson.D{{"attributes.ci_type", 2}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "attributes.ci_type包含多个值",
			pipeline: []bson.D{
				{{"$match", bson.D{{"attributes.ci_type", bson.D{{"$in", []int{2, 3, 4}}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "attributes.ci_type不包含多个值",
			pipeline: []bson.D{
				{{"$match", bson.D{{"attributes.ci_type", bson.D{{"$nin", []int{2, 3, 4}}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "attributes.location like 搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{{"attributes.location", bson.D{{"$regex", "project_root"}, {"$options", "i"}}}}}},
				{{"$count", "total"}},
			},
		},
		{
			name: "attributes.rand_string in 搜索",
			pipeline: []bson.D{
				{{"$match", bson.D{{"attributes.rand_string", bson.D{{"$in", randStr}}}}}},
				{{"$count", "total"}},
			},
		},
	}

	for _, searchTest := range searchTests {
		const executionCount = 5
		var totalDuration time.Duration
		var totalRecords int64
		var successCount int
		var lastError error

		for i := 0; i < executionCount; i++ {
			start := time.Now()

			cursor, err := collection.Aggregate(context.Background(), searchTest.pipeline)
			if err != nil {
				lastError = err
				continue
			}

			var result []bson.M
			if err = cursor.All(context.Background(), &result); err != nil {
				lastError = err
				cursor.Close(context.Background())
				continue
			}

			// 提取计数
			var count int64
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
			duration := time.Since(start)

			totalDuration += duration
			totalRecords += count
			successCount++
		}

		// 计算平均值
		var avgDuration time.Duration
		var avgRecords int64
		var throughput float64
		mark := "成功"

		if successCount > 0 {
			avgDuration = totalDuration / time.Duration(successCount)
			avgRecords = totalRecords / int64(successCount)
			if avgDuration > 0 {
				throughput = float64(avgRecords) / avgDuration.Seconds()
			}
		} else {
			mark = fmt.Sprintf("所有执行都失败: %v", lastError)
		}

		if successCount < executionCount {
			mark = fmt.Sprintf("部分成功 (%d/%d)", successCount, executionCount)
			if lastError != nil {
				mark += fmt.Sprintf("，最后错误: %v", lastError)
			}
		}

		result := BenchmarkResult{
			Operation:  searchTest.name,
			Database:   m.Name(),
			Duration:   avgDuration,
			Records:    int(avgRecords),
			Throughput: throughput,
			Mark:       mark,
		}
		results = append(results, result)

		fmt.Printf("%-12s | %-30s | %-18v | %-10d | %s\n",
			m.Name(), searchTest.name, avgDuration, int(avgRecords), mark)
	}

	return results
}

func (m *MongoDB) Close() {
	m.client.Disconnect(context.Background())
}
