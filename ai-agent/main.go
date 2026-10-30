// 最小 agent 示例：调用 OpenAI 兼容的大模型网关（公司网关 / 官方 API / Ollama 均可），
// 演示工具调用循环。
//
//	go run ./ai-agent -q "现在几点？再算一下 17+25"
//
// 环境变量：
//
//	AI_BASE_URL      必填，例如 https://gw.company.com/v1
//	AI_API_KEY       必填，网关下发的 token
//	AI_MODEL         必填，网关上的模型名
//	AI_AUTH_HEADER   选填，默认 Authorization
//	AI_AUTH_PREFIX   选填，默认 "Bearer "；设成空串则原样发送 token
//	AI_EXTRA_HEADERS 选填，额外请求头，格式 "X-A: v1,X-B: v2"
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

const systemPrompt = `你是一个简洁的中文助手，回答尽量短。
遇到需要知道当前时间、或需要计算加法的问题，必须调用工具完成，不要自己编造结果。
工具返回后，用自然语言把结果告诉用户。`

func main() {
	question := flag.String("q", "", "只回答一个问题后退出；不传则进入交互模式")
	baseFlag := flag.String("base", "", "网关地址（覆盖 AI_BASE_URL）")
	modelFlag := flag.String("model", "deepseek-flash", "模型名（覆盖 AI_MODEL）")
	apiKeyF := flag.String("api", "sk-", "模型名（覆盖 AI_MODEL）")
	dumpURL := flag.Bool("dump-url", false, "只打印将请求的地址和鉴权头后退出，用于排查网关配置")
	flag.Parse()

	baseURL := firstNonEmpty(*baseFlag, os.Getenv("AI_BASE_URL"))
	apiKey := *apiKeyF
	model := firstNonEmpty(*modelFlag, os.Getenv("AI_MODEL"))

	if baseURL == "" || apiKey == "" || model == "" {
		missing := []string{}
		if baseURL == "" {
			missing = append(missing, "AI_BASE_URL")
		}
		if apiKey == "" {
			missing = append(missing, "AI_API_KEY")
		}
		if model == "" {
			missing = append(missing, "AI_MODEL")
		}
		fmt.Fprintf(os.Stderr, "缺少配置：%s\n", strings.Join(missing, "、"))
		fmt.Fprintln(os.Stderr, `PowerShell 示例：`)
		fmt.Fprintln(os.Stderr, `  $env:AI_BASE_URL = "https://gw.company.com/v1"`)
		fmt.Fprintln(os.Stderr, `  $env:AI_API_KEY  = "<网关下发的 token>"`)
		fmt.Fprintln(os.Stderr, `  $env:AI_MODEL    = "<网关上的模型名>"`)
		os.Exit(1)
	}

	llm := NewLLM(baseURL, apiKey, model)
	llm.AuthHeader = envOr("AI_AUTH_HEADER", llm.AuthHeader)
	// 鉴权前缀的优先级：显式配置 > 自定义头默认不加前缀 > 默认 Bearer。
	// 注意 PowerShell 里把变量设成空串等于删除变量，所以用 none 表示“不加前缀”。
	switch prefix := os.Getenv("AI_AUTH_PREFIX"); {
	case prefix == "none":
		llm.AuthPrefix = ""
	case prefix != "":
		llm.AuthPrefix = prefix
	case llm.AuthHeader != "Authorization":
		llm.AuthPrefix = ""
	}
	llm.ExtraHeaders = parseHeaders(os.Getenv("AI_EXTRA_HEADERS"))
	if t := envOr("AI_TIMEOUT", ""); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			llm.HTTP = &http.Client{Timeout: d}
		}
	}

	if *dumpURL {
		fmt.Printf("POST %s\n", llm.Endpoint())
		fmt.Printf("%s: %s\n", llm.AuthHeader, maskToken(llm.AuthPrefix+apiKey))
		fmt.Printf("model: %s\n", model)
		for k, v := range llm.ExtraHeaders {
			fmt.Printf("%s: %s\n", k, v)
		}
		return
	}

	agent := NewAgent(llm, NewRegistry(), systemPrompt)
	agent.Logf = func(format string, args ...any) { fmt.Printf(format, args...) }

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *question != "" {
		answer, err := agent.Run(ctx, *question)
		if err != nil {
			fmt.Fprintf(os.Stderr, "出错: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("助手> %s\n", answer)
		return
	}

	fmt.Printf("已连接 %s / %s，输入问题开始对话；/reset 清空上下文，/exit 退出。\n", llm.Endpoint(), model)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for {
		fmt.Print("你> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		switch line {
		case "":
			continue
		case "/exit", "/quit", "exit", "quit":
			return
		case "/reset":
			agent.Reset(systemPrompt)
			fmt.Println("上下文已清空。")
			continue
		}

		answer, err := agent.Run(ctx, line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "出错: %v\n", err)
			continue
		}
		fmt.Printf("助手> %s\n", answer)
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// parseHeaders 解析 "X-A: v1,X-B: v2" 形式的额外请求头。
func parseHeaders(s string) map[string]string {
	headers := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		key, value, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		headers[key] = strings.TrimSpace(value)
	}
	return headers
}

// maskToken 打印时隐藏 token 主体，只留首尾，避免日志泄露。
func maskToken(token string) string {
	r := []rune(token)
	if len(r) <= 10 {
		return strings.Repeat("*", len(r))
	}
	return string(r[:6]) + "****" + string(r[len(r)-4:])
}
