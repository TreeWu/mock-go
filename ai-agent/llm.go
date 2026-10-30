package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Message 是 OpenAI 兼容协议里的一条对话消息。
// 同一个结构体承担四种角色：
//   - system：设定人设/规则
//   - user：用户输入
//   - assistant：模型回复，可能带 ToolCalls
//   - tool：工具执行结果，必须带 ToolCallID 指回是哪个调用
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall 是模型请求调用某个工具。Arguments 是 JSON 字符串，需要自己解析。
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

// FunctionCall 是 ToolCall 内部的具体函数名与参数。
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Tool 是注册给模型的工具声明（JSON Schema 描述参数）。
type Tool struct {
	Type     string     `json:"type"`
	Function ToolSchema `json:"function"`
}

// ToolSchema 描述一个工具的名称、用途和参数结构。
type ToolSchema struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Tools    []Tool    `json:"tools,omitempty"`

	// 下面两个参数是可选的，先保持默认值即可。
	Temperature float64 `json:"temperature,omitempty"`
	Stream      bool    `json:"stream,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message      Message `json:"message"`
		FinishReason string  `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// LLM 是一个极简的 OpenAI 兼容客户端。官方 API、公司网关、Ollama 都是这套协议，
// 差别只在网关地址、鉴权头和模型名，所以这些都做成可配置项。
// 只依赖标准库，不需要引入 SDK。
type LLM struct {
	BaseURL string // 例如 https://gw.company.com/v1，也可以直接写到 /chat/completions
	APIKey  string
	Model   string
	HTTP    *http.Client

	// 鉴权方式，默认 Authorization: Bearer <key>。
	// 有些网关用 X-Api-Key: <key>（此时把 AuthPrefix 设为空串）。
	AuthHeader string
	AuthPrefix string

	// 网关要求额外请求头时放这里，例如 {"X-Dept": "data"}。
	ExtraHeaders map[string]string
}

// NewLLM 创建客户端；BaseURL 末尾的斜杠会被去掉，避免拼出双斜杠。
func NewLLM(baseURL, apiKey, model string) *LLM {
	return &LLM{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		Model:      model,
		HTTP:       &http.Client{Timeout: 120 * time.Second},
		AuthHeader: "Authorization",
		AuthPrefix: "Bearer ",
	}
}

// Endpoint 拼出真正的请求地址。
// 网关地址通常有两种写法，这两种都支持：
//   - https://gw.company.com/v1               → /v1/chat/completions
//   - https://gw.company.com/v1/chat/completions → 原样使用
func (l *LLM) Endpoint() string {
	base := strings.TrimRight(l.BaseURL, "/")
	if strings.HasSuffix(base, "/chat/completions") {
		return base
	}
	return base + "/chat/completions"
}

// applyHeaders 写入鉴权头和自定义头。
func (l *LLM) applyHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if l.APIKey != "" && l.AuthHeader != "" {
		req.Header.Set(l.AuthHeader, l.AuthPrefix+l.APIKey)
	}
	for k, v := range l.ExtraHeaders {
		req.Header.Set(k, v)
	}
}

// Chat 发一次请求，返回模型这条回复。整个 agent 的“大脑”就在这里。
func (l *LLM) Chat(ctx context.Context, messages []Message, tools []Tool) (*Message, error) {
	body, err := json.Marshal(chatRequest{
		Model:    l.Model,
		Messages: messages,
		Tools:    tools,
	})
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.Endpoint(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	l.applyHeaders(req)

	resp, err := l.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求大模型失败: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("大模型返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var cr chatResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w, 原始内容: %s", err, truncate(string(raw), 500))
	}
	if cr.Error != nil {
		return nil, fmt.Errorf("大模型返回错误: %s", cr.Error.Message)
	}
	if len(cr.Choices) == 0 {
		return nil, fmt.Errorf("大模型没有返回任何 choice: %s", truncate(string(raw), 500))
	}

	msg := cr.Choices[0].Message
	return &msg, nil
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
