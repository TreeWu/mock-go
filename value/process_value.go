package value

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

func NewValueHandler() *Handler {
	// 设置随机种子
	rand.Seed(time.Now().UnixNano())
	gofakeit.Seed(0)
	return &Handler{
		fake: gofakeit.New(0),
		r:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type Handler struct {
	fake *gofakeit.Faker
	r    *rand.Rand
}

// ProcessDynamicValues 处理动态值占位符
func (h *Handler) ProcessDynamicValues(body interface{}) interface{} {

	switch v := body.(type) {
	case string:
		return h.generateDynamicValue(v)
	case map[string]interface{}:
		return h.ProcessDynamicMap(v)
	case []interface{}:
		return h.processArray(v)
	default:
		return body
	}
}

func (h *Handler) ProcessDynamicMap(mapValue map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range mapValue {
		result[k] = h.ProcessDynamicValues(v)
	}
	return result
}

// processArray 处理数组类型的值
func (h *Handler) processArray(arr []interface{}) []interface{} {
	result := make([]interface{}, len(arr))
	for i, item := range arr {
		result[i] = h.ProcessDynamicValues(item)
	}
	return result
}

// generateDynamicValue 根据占位符生成动态值
func (h *Handler) generateDynamicValue(placeholder string) interface{} {

	// 分割指令和参数
	parts := strings.SplitN(placeholder, ":", 2)
	directive := parts[0]

	var args string
	if len(parts) > 1 {
		args = parts[1]
	}

	switch directive {
	case "@randInt":
		return h.generateRandomInt(args)
	case "@randString":
		return h.GenerateRandomString(args)
	case "@email":
		return h.fake.Email()
	case "@name":
		return h.fake.Name()
	case "@word":
		return h.fake.Word()
	case "@sentence":
		return h.fake.Sentence(5)
	case "@uuid":
		return h.fake.UUID()
	case "@timestamp":
		return time.Now().Unix()
	case "@date":
		return h.fake.Date().Format("2006-01-02")
	case "@datetime":
		return h.fake.Date().Format("2006-01-02 15:04:05")
	case "@bool":
		return h.fake.Bool()
	case "@float":
		return h.fake.Float64Range(0, 1000)
	default:
		return placeholder
	}
}

// generateRandomInt 生成随机整数
func (h *Handler) generateRandomInt(args string) int64 {
	if args == "" {
		return h.fake.Int64()
	}

	// 解析数字位数
	if digit, err := strconv.Atoi(args); err == nil {
		m := 1
		for i := 1; i < digit; i++ {
			m *= 10
		}
		i := m*10 - 1
		return int64(h.fake.IntRange(m, i))
	}

	return h.fake.Int64()
}

// 生成随机字符串
func (h *Handler) GenerateRandomString(args string) string {
	var length int = 10
	if long, err := strconv.Atoi(args); err == nil {
		length = long
	}
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[h.r.Intn(len(charset))]
	}
	return string(b)
}
