package main

import (
	"context"
	"fmt"
)

// Agent 维护完整的对话历史（messages），并把「模型请求工具 → 本地执行 → 结果回填」
// 这个循环跑完。这就是所谓 agent 的核心，其余都是外围。
type Agent struct {
	llm      *LLM
	tools    *Registry
	messages []Message
	maxSteps int
	Logf     func(format string, args ...any) // 可选的进度输出，测试时可置空
}

func NewAgent(llm *LLM, tools *Registry, systemPrompt string) *Agent {
	a := &Agent{llm: llm, tools: tools, maxSteps: 8, Logf: func(string, ...any) {}}
	a.Reset(systemPrompt)
	return a
}

// Reset 清空历史，只保留 system 提示词。
func (a *Agent) Reset(systemPrompt string) {
	a.messages = a.messages[:0]
	if systemPrompt != "" {
		a.messages = append(a.messages, Message{Role: "system", Content: systemPrompt})
	}
}

// Run 处理一轮用户输入，返回模型的最终文本回答。
func (a *Agent) Run(ctx context.Context, input string) (string, error) {
	a.messages = append(a.messages, Message{Role: "user", Content: input})

	for step := 0; step < a.maxSteps; step++ {
		msg, err := a.llm.Chat(ctx, a.messages, a.tools.Defs())
		if err != nil {
			return "", err
		}
		// 模型的这条回复必须先进入历史，后续工具结果才有上下文可依附。
		a.messages = append(a.messages, *msg)

		// 没有工具调用：这就是最终答案。
		if len(msg.ToolCalls) == 0 {
			return msg.Content, nil
		}

		// 有工具调用：逐个执行，把结果作为 role=tool 的消息追加回去，再问一次模型。
		for _, call := range msg.ToolCalls {
			a.Logf("→ 调用工具 %s(%s)\n", call.Function.Name, call.Function.Arguments)
			result := a.tools.Exec(call)
			a.Logf("← 工具返回 %s\n", result)
			a.messages = append(a.messages, Message{
				Role:       "tool",
				Content:    result,
				ToolCallID: call.ID,
			})
		}
	}

	return "", fmt.Errorf("超过最大工具调用轮数 %d，已中止", a.maxSteps)
}
