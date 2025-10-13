package main

import "github.com/TreeWu/mock-go/http_mock"

func main() {

	httpHandler := http_mock.NewHttpMockHandler(":8080", "D:\\code\\mock-go\\http.json")
	httpHandler.Start()
}

//	pgDSN = "postgres://gj:xbrother123@localhost:5432/benchmark_db?sslmode=disable"
//
//	// MongoDB 配置
//	mongoURI      = "mongodb://gj:xbrother123@localhost:27017"
//	mongoDatabase = "benchmark_db"
