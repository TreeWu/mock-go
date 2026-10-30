package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// toolFunc 是工具的本地实现：入参是模型给的 JSON 字符串，返回给模型看的文本结果。
type toolFunc func(args string) (string, error)

// Registry 把「工具声明」（给模型看）和「本地实现」（真正执行）绑在一起。
type Registry struct {
	defs  []Tool
	funcs map[string]toolFunc
}

func NewRegistry() *Registry {
	r := &Registry{funcs: map[string]toolFunc{}}
	r.register(
		ToolSchema{
			Name:        "get_current_time",
			Description: "获取当前日期和时间。用户问现在几点、今天几号时调用。",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		getCurrentTime,
	)
	r.register(
		ToolSchema{
			Name:        "add",
			Description: "计算两个数字之和。需要做加法时调用，不要心算。",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"a": map[string]any{"type": "number", "description": "第一个加数"},
					"b": map[string]any{"type": "number", "description": "第二个加数"},
				},
				"required": []string{"a", "b"},
			},
		},
		add,
	)
	return r
}

// register 登记一个工具，同时生成给模型看的声明。
func (r *Registry) register(schema ToolSchema, fn toolFunc) {
	r.defs = append(r.defs, Tool{Type: "function", Function: schema})
	r.funcs[schema.Name] = fn
}

// Defs 返回工具声明列表，直接塞进请求的 tools 字段。
func (r *Registry) Defs() []Tool { return r.defs }

// Exec 执行模型点名的一个工具，并统一把错误转成文本结果回给模型，
// 这样模型不会因为某个工具失败而中断对话。
func (r *Registry) Exec(call ToolCall) string {
	fn, ok := r.funcs[call.Function.Name]
	if !ok {
		return fmt.Sprintf("执行失败：不存在名为 %q 的工具", call.Function.Name)
	}
	out, err := fn(call.Function.Arguments)
	if err != nil {
		return fmt.Sprintf("执行失败：%v", err)
	}
	return out
}

func getCurrentTime(string) (string, error) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.Local
	}
	return time.Now().In(loc).Format("2006-01-02 15:04:05 Monday"), nil
}

func add(args string) (string, error) {
	var in struct {
		A float64 `json:"a"`
		B float64 `json:"b"`
	}
	if err := json.Unmarshal([]byte(args), &in); err != nil {
		return "", fmt.Errorf("参数不是合法 JSON: %w", err)
	}
	sum := in.A + in.B
	// 用 %g 避免把 1+2=3 打印成 3.000000
	return strconv.FormatFloat(sum, 'g', -1, 64), nil
}
