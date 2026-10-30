package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 网关地址有两种常见写法，两种都要能拼出正确的请求地址。
func TestEndpoint(t *testing.T) {
	cases := map[string]string{
		"https://gw.company.com/v1":                  "https://gw.company.com/v1/chat/completions",
		"https://gw.company.com/v1/":                 "https://gw.company.com/v1/chat/completions",
		"https://gw.company.com/v1/chat/completions": "https://gw.company.com/v1/chat/completions",
		"https://gw.company.com/openai/v1/":          "https://gw.company.com/openai/v1/chat/completions",
	}
	for base, want := range cases {
		if got := NewLLM(base, "k", "m").Endpoint(); got != want {
			t.Errorf("base=%q 期望 %q，实际 %q", base, want, got)
		}
	}
}

// 网关用自定义鉴权头时，不能带默认的 Authorization: Bearer。
func TestCustomAuthHeader(t *testing.T) {
	var gotHeader http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer srv.Close()

	llm := NewLLM(srv.URL+"/v1", "company-token", "gw-model")
	llm.AuthHeader = "X-Api-Key"
	llm.AuthPrefix = ""
	llm.ExtraHeaders = map[string]string{"X-Dept": "data"}

	agent := NewAgent(llm, NewRegistry(), "")
	answer, err := agent.Run(context.Background(), "你好")
	if err != nil {
		t.Fatalf("Run 出错: %v", err)
	}
	if answer != "ok" {
		t.Fatalf("回答异常: %q", answer)
	}
	if got := gotHeader.Get("X-Api-Key"); got != "company-token" {
		t.Fatalf("X-Api-Key 不符合预期: %q", got)
	}
	if got := gotHeader.Get("Authorization"); got != "" {
		t.Fatalf("不应发送 Authorization，实际: %q", got)
	}
	if got := gotHeader.Get("X-Dept"); got != "data" {
		t.Fatalf("额外请求头未生效: %q", got)
	}
}

// 请求体里必须是网关配置的模型名，而不是写死的 deepseek-chat。
func TestRequestUsesConfiguredModel(t *testing.T) {
	var body chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer srv.Close()

	agent := NewAgent(NewLLM(srv.URL, "k", "qwen-internal-72b"), NewRegistry(), "")
	if _, err := agent.Run(context.Background(), "hi"); err != nil {
		t.Fatalf("Run 出错: %v", err)
	}
	if body.Model != "qwen-internal-72b" {
		t.Fatalf("模型名不是配置值: %q", body.Model)
	}
}

func TestParseHeaders(t *testing.T) {
	got := parseHeaders("X-A: v1, X-B:v2,,bad")
	if got["X-A"] != "v1" || got["X-B"] != "v2" {
		t.Fatalf("解析结果异常: %#v", got)
	}
	if len(got) != 2 {
		t.Fatalf("应只解析出 2 个头: %#v", got)
	}
	if empty := parseHeaders(""); len(empty) != 0 {
		t.Fatalf("空输入应返回空 map: %#v", empty)
	}
}

func TestMaskToken(t *testing.T) {
	masked := maskToken("Bearer sk-1234567890abcdef")
	if strings.Contains(masked, "1234567890") {
		t.Fatalf("token 主体不应泄露: %q", masked)
	}
	if !strings.Contains(masked, "****") {
		t.Fatalf("应包含掩码: %q", masked)
	}
}
