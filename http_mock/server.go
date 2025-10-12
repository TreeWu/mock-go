package http_mock

import (
	"encoding/json"
	"github.com/TreeWu/mock-go/value"
	"github.com/gin-gonic/gin"
	"log"
	"os"
	"strings"
)

type HttpMockHandler struct {
	port         string
	path         []string
	valueHandler *value.Handler
}

func NewHttpMockHandler(port string, path ...string) *HttpMockHandler {

	return &HttpMockHandler{
		valueHandler: value.NewValueHandler(),
		port:         port,
		path:         path,
	}
}

func (h *HttpMockHandler) Start() {
	var mockConfigs []MockConfig

	for _, path := range h.path {
		// 读取配置文件
		configFile, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("读取配置文件失败: %v", err)
		}

		var mcs []MockConfig
		err = json.Unmarshal(configFile, &mcs)
		if err != nil {
			log.Fatalf("解析配置文件失败: %v", err)
		}

		mockConfigs = append(mockConfigs, mcs...)
	}

	// 创建 Gin 路由
	router := gin.Default()
	router.Use(gin.Recovery())
	// 注册 mock 处理器

	// 为每个配置项注册路由
	for _, config := range mockConfigs {
		switch strings.ToUpper(config.Method) {
		case "GET":
			router.GET(config.URL, h.HandleMock(config))
		case "POST":
			router.POST(config.URL, h.HandleMock(config))
		case "PUT":
			router.PUT(config.URL, h.HandleMock(config))
		case "DELETE":
			router.DELETE(config.URL, h.HandleMock(config))
		case "PATCH":
			router.PATCH(config.URL, h.HandleMock(config))
		default:
			log.Printf("不支持的 HTTP 方法: %s", config.Method)
		}

		log.Println("注册路由: ", config.Method, config.URL)
	}

	// 启动服务器
	log.Println("Mock 服务器启动在端口", h.port)
	if err := router.Run(h.port); err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
}

func (h *HttpMockHandler) HandleMock(mockConfig MockConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		var paramStr, reqStr []byte
		params := make(map[string]string)
		if err := c.ShouldBindQuery(&params); err != nil {
			log.Println("query 参数解析失败: ", err)
		} else {
			paramStr, _ = json.Marshal(params)
		}

		req := make(map[string]interface{})
		if err := c.ShouldBindJSON(&req); err != nil {
			log.Println("body  参数解析失败: ", err)
		} else {
			reqStr, _ = json.Marshal(req)
		}

		log.Printf("param: %s, req: %s \n", string(paramStr), string(reqStr))

		processedBody := h.valueHandler.ProcessDynamicValues(mockConfig.Response.Body)

		c.JSON(mockConfig.Response.StatusCode, processedBody)
	}
}
