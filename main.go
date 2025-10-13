package main

import "github.com/TreeWu/mock-go/http_mock"

func main() {

	httpHandler := http_mock.NewHttpMockHandler(":8080", "D:\\code\\mock-go\\http.json")
	httpHandler.Start()
}
