package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"time"
)

var _ BenchmarkEngine = (*PostGreSql)(nil)

type PostGreSql struct {
	dns  string
	pool *pgxpool.Pool
}

func (p *PostGreSql) Name() string {
	return "PostgreSql"
}

func (p *PostGreSql) Close() {
	p.pool.Close()
}

func NewPostgreSQL(dns string) BenchmarkEngine {
	return &PostGreSql{dns: dns}
}

func (p *PostGreSql) Init() {
	config, err := pgxpool.ParseConfig(p.dns)
	if err != nil {
		log.Fatal("解析 PostgreSQL DSN 失败:", err)
	}

	config.MaxConns = 50
	config.MinConns = 5

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal("连接 PostgreSQL 失败:", err)
	}

	// 创建表 - 修复生成列的问题
	_, err = pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS users;
		
		CREATE TABLE users (
			id BIGSERIAL PRIMARY KEY,
			name TEXT,
			email TEXT,
			age INTEGER,
			city TEXT,
			salary DECIMAL,
			created_at TIMESTAMP,
			datastr JSONB NOT NULL
		);

		CREATE INDEX idx_users_name ON users(name);
		CREATE INDEX idx_users_age ON users(age);
		CREATE INDEX idx_datastr_gin ON users USING GIN (datastr);
	`)

	if err != nil {
		log.Fatal("创建 PostgreSQL 表失败:", err)
	}

	fmt.Println("PostgreSQL 连接和表初始化成功")
	p.pool = pool
}

func (p *PostGreSql) Insert(data []User, batchSize int) []BenchmarkResult {
	var results []BenchmarkResult
	start := time.Now()
	total := len(data)
	for i := 0; i < total; i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, total)

		// 准备批量插入
		batch := data[i:batchEnd]
		// 使用 COPY 命令进行真正的批量插入
		err := p.bulkInsertPostgreSQL(batch)
		if err != nil {
			log.Printf("PostgreSQL 批量插入失败: %v", err)
			continue
		}

		batchDuration := time.Since(batchStart)
		batchResult := BenchmarkResult{
			Operation:  Operation_Insert,
			Database:   p.Name(),
			Duration:   batchDuration,
			Records:    batchEnd - i,
			Throughput: float64(batchEnd-i) / batchDuration.Seconds(),
		}
		results = append(results, batchResult)

		if i%100000 == 0 {
			fmt.Printf("PostgreSQL 已插入 %d 条记录\n", batchEnd)
		}
	}

	totalDuration := time.Since(start)
	totalResult := BenchmarkResult{
		Operation:  Operation_InsertTotal,
		Database:   p.Name(),
		Duration:   totalDuration,
		Records:    total,
		Throughput: float64(total) / totalDuration.Seconds(),
	}

	fmt.Printf("PostgreSQL 插入完成: %d 条记录, 耗时: %v, 吞吐量: %.2f 记录/秒\n",
		total, totalDuration, totalResult.Throughput)

	return append(results, totalResult)
}

// 使用 COPY 命令进行高效批量插入
func (p *PostGreSql) bulkInsertPostgreSQL(users []User) error {
	ctx := context.Background()

	// 开始事务
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 使用 COPY FROM 进行批量插入
	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"users"},
		[]string{"name", "email", "age", "city", "salary", "created_at", "datastr"},
		pgx.CopyFromSlice(len(users), func(i int) ([]interface{}, error) {
			user := users[i]
			return []interface{}{
				user.Name,
				user.Email,
				user.Age,
				user.City,
				user.Salary,
				user.CreatedAt,
				user.UserStr,
			}, nil
		}),
	)

	if err != nil {
		return err
	}

	if copyCount != int64(len(users)) {
		return fmt.Errorf("插入记录数量不匹配: 期望 %d, 实际 %d", len(users), copyCount)
	}

	return tx.Commit(ctx)
}

func (p *PostGreSql) ClearData() {
	// 清理 PostgreSQL
	_, err := p.pool.Exec(context.Background(), "TRUNCATE TABLE users RESTART IDENTITY")
	if err != nil {
		log.Printf("清理 PostgreSQL 数据失败: %v", err)
	}
}

func (p *PostGreSql) Search(testData []User) []BenchmarkResult {
	var results []BenchmarkResult

	// 测试不同的搜索场景
	searchTests := []struct {
		name string
		sql  string
		args []interface{}
	}{
		{
			name: "姓名搜索_索引",
			sql:  "SELECT COUNT(*) FROM users WHERE name = $1",
		},
		{
			name: "年龄范围搜索_索引",
			sql:  "SELECT COUNT(*) FROM users WHERE age BETWEEN $1 AND $2",
		},
		{
			name: "城市筛选",
			sql:  "SELECT COUNT(*) FROM users WHERE city = $1",
		},
		{
			name: "薪资范围搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE salary BETWEEN $1 AND $2",
		},
		{
			name: "JSON字段搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE datastr->'metadata'->>'department' = $1",
		},
		{
			name: "复杂条件搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE city = $1 AND age > $2 AND salary > $3",
		},
		{
			name: "全文搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE datastr::text LIKE $1",
		},
	}

	for _, test := range searchTests {
		start := time.Now()
		var count int64

		// 执行多次取平均值
		iterations := 10
		for i := 0; i < iterations; i++ {
			var args []interface{}
			switch test.name {
			case "姓名搜索_索引":
				args = []interface{}{testData[0].Name}
			case "年龄范围搜索_索引":
				args = []interface{}{25, 35}
			case "城市筛选":
				args = []interface{}{cities[0]}
			case "薪资范围搜索":
				args = []interface{}{40000, 60000}
			case "JSON字段搜索":
				args = []interface{}{departments[0]}
			case "复杂条件搜索":
				args = []interface{}{cities[0], 30, 50000}
			case "全文搜索":
				args = []interface{}{"用户"}
			}

			err := p.pool.QueryRow(context.Background(), test.sql, args...).Scan(&count)
			if err != nil {
				log.Printf("PostgreSQL 搜索失败: %v", err)
				continue
			}
		}

		duration := time.Since(start) / time.Duration(iterations)
		result := BenchmarkResult{
			Operation:  test.name,
			Database:   p.Name(),
			Duration:   duration,
			Records:    int(count),
			Throughput: 1.0 / duration.Seconds(), // 查询/秒
		}
		results = append(results, result)

		fmt.Printf("PostgreSQL %s: 耗时 %v, 匹配记录: %d\n", test.name, duration, count)
	}

	return results
}
