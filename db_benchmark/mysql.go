package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/rand"

	"time"
)

type MySQLEngine struct {
	db   *sql.DB
	dsn  string
	name string
}

func NewMySQL(dsn string) *MySQLEngine {
	return &MySQLEngine{
		dsn:  dsn,
		name: "MySQL",
	}
}

func (m *MySQLEngine) Init() {
	var err error
	m.db, err = sql.Open("mysql", m.dsn)
	if err != nil {
		log.Fatalf("MySQL 连接失败: %v", err)
	}

	// 设置连接池参数
	m.db.SetMaxOpenConns(50)
	m.db.SetMaxIdleConns(10)
	m.db.SetConnMaxLifetime(time.Hour)

	// 测试连接
	if err = m.db.Ping(); err != nil {
		log.Fatalf("MySQL 连接测试失败: %v", err)
	}

	// 创建数据库和表
	m.createDatabaseAndTable()
	fmt.Println("MySQL 初始化成功")
}

func (m *MySQLEngine) createDatabaseAndTable() {
	// 创建数据库
	_, err := m.db.Exec("CREATE DATABASE IF NOT EXISTS benchmark_db")
	if err != nil {
		log.Fatalf("创建 MySQL 数据库失败: %v", err)
	}

	// 使用数据库
	_, err = m.db.Exec("USE benchmark_db")
	if err != nil {
		log.Fatalf("使用 MySQL 数据库失败: %v", err)
	}

	// 创建表
	_, err = m.db.Exec(`
		 CREATE TABLE IF NOT EXISTS users (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        data JSON NOT NULL,
        name VARCHAR(255),
        email VARCHAR(255),
        age INT,
        city VARCHAR(100),
        salary DECIMAL(10,2),
        created_at DATETIME,
        department VARCHAR(100),
        INDEX idx_name (name),
        INDEX idx_age (age),
        INDEX idx_city (city),
        INDEX idx_salary (salary),
        INDEX idx_department (department),
        INDEX idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		log.Fatalf("创建 MySQL 表失败: %v", err)
	}
}

func (m *MySQLEngine) Insert(data []User, batchSize int) []BenchmarkResult {
	var results []BenchmarkResult
	start := time.Now()

	for i := 0; i < len(data); i += batchSize {
		batchStart := time.Now()
		batchEnd := min(i+batchSize, len(data))
		batch := data[i:batchEnd]

		// 使用事务进行批量插入
		err := m.bulkInsertMySQL(batch)
		if err != nil {
			log.Printf("MySQL 批量插入失败: %v", err)
			continue
		}

		batchDuration := time.Since(batchStart)
		batchResult := BenchmarkResult{
			Operation:  Operation_Insert,
			Database:   m.Name(),
			Duration:   batchDuration,
			Records:    len(batch),
			Throughput: float64(len(batch)) / batchDuration.Seconds(),
		}
		results = append(results, batchResult)

		if i%100000 == 0 {
			fmt.Printf("%s 已插入 %d 条记录\n", m.Name(), batchEnd)
		}
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

func (m *MySQLEngine) bulkInsertMySQL(users []User) error {
	// 开始事务
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 准备批量插入语句
	stmt, err := tx.Prepare(`
		    INSERT INTO users (data, name, email, age, city, salary, created_at, department)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// 执行批量插入
	for _, user := range users {
		userJSON, err := json.Marshal(user)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(userJSON, user.Name, user.Email, user.Age, user.City,
			user.Salary, user.CreatedAt, user.Metadata.Department)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (m *MySQLEngine) ClearData() {
	_, err := m.db.Exec("TRUNCATE TABLE users")
	if err != nil {
		log.Printf("MySQL 清理数据失败: %v", err)
	}
	fmt.Printf("%s 数据清理完成\n", m.Name())
}

func (m *MySQLEngine) Search(testData []User) []BenchmarkResult {
	var results []BenchmarkResult

	// 测试不同的搜索场景
	searchTests := []struct {
		name string
		sql  string
		args []interface{}
	}{
		{
			name: "精确匹配搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE name = ?",
		},
		{
			name: "范围搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE age BETWEEN ? AND ?",
		},
		{
			name: "城市筛选",
			sql:  "SELECT COUNT(*) FROM users WHERE city = ?",
		},
		{
			name: "薪资范围搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE salary BETWEEN ? AND ?",
		},
		{
			name: "部门筛选",
			sql:  "SELECT COUNT(*) FROM users WHERE department = ?",
		},
		{
			name: "JSON字段搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE JSON_EXTRACT(data, '$.metadata.position') = ?",
		},
		{
			name: "复杂条件搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE city = ? AND age > ? AND salary > ?",
		},
		{
			name: "全文搜索",
			sql:  "SELECT COUNT(*) FROM users WHERE name LIKE ?",
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
			case "精确匹配搜索":
				args = []interface{}{testData[rand.Intn(len(testData))].Name}
			case "范围搜索":
				args = []interface{}{25, 35}
			case "城市筛选":
				args = []interface{}{cities[rand.Intn(len(cities))]}
			case "薪资范围搜索":
				args = []interface{}{40000, 60000}
			case "部门筛选":
				args = []interface{}{departments[rand.Intn(len(departments))]}
			case "JSON字段搜索":
				args = []interface{}{positions[rand.Intn(len(positions))]}
			case "复杂条件搜索":
				args = []interface{}{cities[rand.Intn(len(cities))], 30, 50000}
			case "全文搜索":
				args = []interface{}{"%用户%"}
			}

			err := m.db.QueryRow(test.sql, args...).Scan(&count)
			if err != nil {
				log.Printf("%s 搜索失败: %v", m.Name(), err)
				continue
			}
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

		fmt.Printf("%s %s: 耗时 %v, 匹配记录: %d\n", m.Name(), test.name, duration, count)
	}

	return results
}

func (m *MySQLEngine) Close() {
	if m.db != nil {
		m.db.Close()
	}
	fmt.Printf("%s 连接已关闭\n", m.Name())
}

func (m *MySQLEngine) Name() string {
	return m.name
}
