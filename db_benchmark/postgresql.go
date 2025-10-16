// postgresql_engine.go
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"log"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

var _ BenchmarkEngine = (*PostgresqlEngine)(nil)

// PostgresqlEngine 结构体
type PostgresqlEngine struct {
	pool      *pgxpool.Pool
	config    *PostgresqlConfig
	tableName string
}

func (p *PostgresqlEngine) Insert(data []Resource, batchSize int) []BenchmarkResult {
	// 创建表
	if err := p.createTable(); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	var results []BenchmarkResult
	start := time.Now()

	for i := 0; i < len(data); i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		// 使用 COPY 进行批量插入
		err := p.BulkInsert(batch)
		if err != nil {
			log.Printf("PostgreSQL 批量插入失败: %v", err)
			continue
		}

		batchDuration := time.Since(batchStart)
		batchResult := BenchmarkResult{
			Operation:  Operation_Insert,
			Database:   p.Name(),
			Duration:   batchDuration,
			Records:    len(batch),
			Throughput: float64(len(batch)) / batchDuration.Seconds(),
		}
		results = append(results, batchResult)

		if i%1000 == 0 {
			fmt.Printf("%s 已插入 %d 条记录\n", p.Name(), batchEnd)
		}
	}

	totalDuration := time.Since(start)
	totalResult := BenchmarkResult{
		Operation:  Operation_InsertTotal,
		Database:   p.Name(),
		Duration:   totalDuration,
		Records:    len(data),
		Throughput: float64(len(data)) / totalDuration.Seconds(),
	}

	fmt.Printf("%s 插入完成: %d 条记录, 耗时: %v, 吞吐量: %.2f 记录/秒\n",
		p.Name(), len(data), totalDuration, totalResult.Throughput)

	return append(results, totalResult)
}

// PostgresqlConfig 配置
type PostgresqlConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	TableName       string
	SSLMode         string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
}

func (p *PostgresqlEngine) Init() {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.config.User, p.config.Password, p.config.Host, p.config.Port,
		p.config.DBName, p.config.SSLMode)

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("解析 PostgreSQL 配置失败: %v", err)
	}

	config.MaxConns = p.config.MaxConns
	config.MinConns = p.config.MinConns
	config.MaxConnLifetime = p.config.MaxConnLifetime

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("创建 PostgreSQL 连接池失败: %v", err)
	}

	// 测试连接
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("PostgreSQL 连接测试失败: %v", err)
	}

	p.pool = pool

	fmt.Println("PostgreSQL 初始化成功")
}

// NewPostgresqlEngine 创建新的引擎实例
func NewPostgresqlEngine(config *PostgresqlConfig) (*PostgresqlEngine, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.User, config.Password, config.Host, config.Port,
		config.DBName, config.SSLMode)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = config.MaxConns
	poolConfig.MinConns = config.MinConns
	poolConfig.MaxConnLifetime = config.MaxConnLifetime

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	engine := &PostgresqlEngine{
		pool:      pool,
		config:    config,
		tableName: config.TableName,
	}

	// 创建表
	if err := engine.createTable(); err != nil {
		pool.Close()
		return nil, err
	}

	return engine, nil
}

// createTable 创建表
func (p *PostgresqlEngine) createTable() error {
	// 清理现有表数据
	_, err := p.pool.Exec(context.Background(),
		fmt.Sprintf("TRUNCATE TABLE %s", p.tableName))
	if err != nil {
		// 表可能不存在，继续创建
		log.Printf("清理表数据失败（可能表不存在）: %v", err)
	}

	// 创建表结构
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			resource_id TEXT PRIMARY KEY,
			parent_id TEXT,
			version INTEGER,
			deleted INTEGER,
			attributes JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, p.tableName)

	_, err = p.pool.Exec(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 创建索引以提高查询性能
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_resource_id ON %s(resource_id)", p.tableName, p.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_attributes_gin ON %s USING gin(attributes)", p.tableName, p.tableName),
	}

	for _, indexSQL := range indexes {
		_, err = p.pool.Exec(context.Background(), indexSQL)
		if err != nil {
			log.Printf("创建索引失败: %v", err)
		}
	}

	return nil
}

// BulkInsert 使用 COPY FROM 进行高性能批量插入
func (p *PostgresqlEngine) BulkInsert(resources []Resource) error {
	ctx := context.Background()

	// 开始事务
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback(ctx)

	// 使用 CopyFrom 进行批量插入
	columnNames := []string{"resource_id", "parent_id", "version", "deleted", "attributes"}

	copyCount, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{p.tableName},
		columnNames,
		pgx.CopyFromSlice(len(resources), func(i int) ([]interface{}, error) {
			resource := resources[i]
			return []interface{}{
				resource.ResourceId,
				resource.ParentId,
				resource.Version,
				resource.Deleted,
				[]byte(resource.AttributeStr),
			}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("COPY FROM 插入失败: %v", err)
	}

	if copyCount != int64(len(resources)) {
		return fmt.Errorf("插入记录数量不匹配: 期望 %d, 实际 %d", len(resources), copyCount)
	}

	// 提交事务
	return tx.Commit(ctx)
}

// Search 执行搜索测试，多次执行取平均值
func (p *PostgresqlEngine) Search(test []Resource) []BenchmarkResult {
	var results []BenchmarkResult
	ctx := context.Background()

	// 定义测试用例 - 与 Elasticsearch 保持一致
	testCases := []struct {
		name        string
		description string
		queryFunc   func() (string, []interface{})
	}{
		{
			name:        "resource_id精准匹配",
			description: "根据resource_id精确匹配特定资源",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE resource_id = $1", p.tableName),
					[]interface{}{test[0].ResourceId}
			},
		},
		{
			name:        "resource_id模糊匹配",
			description: "使用通配符匹配resource_id，如%%0_1_0%%",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE resource_id LIKE $1", p.tableName),
					[]interface{}{"%" + test[0].ResourceId + "%"}
			},
		},
		{
			name:        "attributes.ci_type精准匹配",
			description: "根据attributes中的ci_type字段精确匹配",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE attributes->>'ci_type' = $1", p.tableName),
					[]interface{}{"2"}
			},
		},
		{
			name:        "attributes.ci_type包含多个值",
			description: "匹配attributes.ci_type在指定数组中的资源",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE attributes->>'ci_type' IN ($1, $2, $3)", p.tableName),
					[]interface{}{"2", "3", "4"}
			},
		},
		{
			name:        "attributes.ci_type不包含多个值",
			description: "匹配attributes.ci_type不在指定数组中的资源",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE attributes->>'ci_type' NOT IN ($1, $2, $3)", p.tableName),
					[]interface{}{"2", "3", "4"}
			},
		},
		{
			name:        "attributes.location like 搜索",
			description: "attributes.location like 搜索",
			queryFunc: func() (string, []interface{}) {
				return fmt.Sprintf(`SELECT COUNT(*)
FROM %s 
WHERE attributes->>'location' ILIKE $1`, p.tableName), []interface{}{"%project_root%"}
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

		query, args := tc.queryFunc()

		// 执行多次搜索
		for i := 0; i < executionCount; i++ {
			start := time.Now()

			var count int
			err := p.pool.QueryRow(ctx, query, args...).Scan(&count)

			duration := time.Since(start)

			if err != nil {
				lastError = err
				continue
			}

			totalDuration += duration
			totalRecord += count
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
			Database:   p.Name(),
			Duration:   avgDuration,
			Records:    avgRecords,
			Throughput: throughput,
			Mark:       mark,
		})
	}

	return results
}

func (p *PostgresqlEngine) ClearData() {
	ctx := context.Background()
	_, err := p.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", p.tableName))
	if err != nil {
		log.Printf("%s 清理数据失败: %v", p.Name(), err)
		return
	}

	fmt.Printf("%s 数据清理完成\n", p.Name())
}

func (p *PostgresqlEngine) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresqlEngine) Name() string {
	return "PostgreSQL"
}
