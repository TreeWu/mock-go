package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 用一个假的 /chat/completions 服务模拟 DeepSeek：
// 第一次要求调用 add(17,25)，第二次看到工具结果后给出最终回答。
// 这样即使没有 API Key，也能验证整个工具调用循环是否正确。
func TestAgentToolLoop(t *testing.T) {
	var requests []chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("未预期的路径: %s", r.URL.Path)
		}
		var req chatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("解析请求失败: %v", err)
		}
		requests = append(requests, req)

		w.Header().Set("Content-Type", "application/json")
		if len(requests) == 1 {
			_, _ = w.Write([]byte(`{
				"choices": [{
					"message": {
						"role": "assistant",
						"content": "",
						"tool_calls": [{
							"id": "call_1",
							"type": "function",
							"function": {"name": "add", "arguments": "{\"a\":17,\"b\":25}"}
						}]
					},
					"finish_reason": "tool_calls"
				}]
			}`))
			return
		}
		_, _ = w.Write([]byte(`{
			"choices": [{
				"message": {"role": "assistant", "content": "17 + 25 = 42"},
				"finish_reason": "stop"
			}]
		}`))
	}))
	defer srv.Close()

	agent := NewAgent(NewLLM(srv.URL, "test-key", "deepseek-chat"), NewRegistry(), "你是助手")
	answer, err := agent.Run(context.Background(), "17+25 等于多少？")
	if err != nil {
		t.Fatalf("Run 出错: %v", err)
	}
	if answer != "17 + 25 = 42" {
		t.Fatalf("最终回答不符合预期: %q", answer)
	}
	if len(requests) != 2 {
		t.Fatalf("期望 2 次大模型请求，实际 %d 次", len(requests))
	}

	// 第一次请求里应带上工具声明
	if len(requests[0].Tools) != 2 {
		t.Fatalf("期望注册 2 个工具，实际 %d 个", len(requests[0].Tools))
	}

	// 第二次请求里应带上 assistant 的 tool_calls 和 role=tool 的执行结果
	second := requests[1].Messages
	if len(second) < 3 {
		t.Fatalf("第二次请求消息太少: %+v", second)
	}
	toolMsg := second[len(second)-1]
	if toolMsg.Role != "tool" {
		t.Fatalf("最后一条消息应为 tool，实际是 %q", toolMsg.Role)
	}
	if toolMsg.ToolCallID != "call_1" {
		t.Fatalf("tool_call_id 不匹配: %q", toolMsg.ToolCallID)
	}
	if strings.TrimSpace(toolMsg.Content) != "42" {
		t.Fatalf("工具结果不符合预期: %q", toolMsg.Content)
	}
}

// 模型服务出错时要能拿到可读的错误信息，而不是死循环。
func TestAgentHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"Invalid API key","type":"authentication_error"}}`))
	}))
	defer srv.Close()

	agent := NewAgent(NewLLM(srv.URL, "bad-key", "deepseek-chat"), NewRegistry(), "")
	_, err := agent.Run(context.Background(), "你好")
	if err == nil {
		t.Fatal("期望返回错误，实际成功")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("错误信息应包含状态码: %v", err)
	}
}

// 工具参数非法、工具不存在时，应作为文本结果回给模型，而不是 panic。
func TestRegistryExec(t *testing.T) {
	r := NewRegistry()
	if got := r.Exec(ToolCall{Function: FunctionCall{Name: "add", Arguments: `{"a":1,"b":2}`}}); got != "3" {
		t.Fatalf("add 结果错误: %q", got)
	}
	if got := r.Exec(ToolCall{Function: FunctionCall{Name: "add", Arguments: `{oops}`}}); !strings.Contains(got, "执行失败") {
		t.Fatalf("非法参数应返回失败文本: %q", got)
	}
	if got := r.Exec(ToolCall{Function: FunctionCall{Name: "nope"}}); !strings.Contains(got, "不存在") {
		t.Fatalf("未知工具应返回失败文本: %q", got)
	}
	if got := r.Exec(ToolCall{Function: FunctionCall{Name: "get_current_time"}}); !strings.Contains(got, ":") {
		t.Fatalf("时间工具结果异常: %q", got)
	}
}
