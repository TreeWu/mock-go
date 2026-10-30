# ai-agent

一个最小的、能调用大模型并执行工具的 Go agent，用来演示 agent 的完整流程。
只依赖标准库，面向 **OpenAI 兼容接口**：公司网关、OpenAI 官方、DeepSeek、Ollama 都能用，
换地址和鉴权方式只改环境变量，不改代码。

## 调用流程

```
用户输入
   │
   ▼
messages = [system, ...历史, user]  ──POST──▶ {base}/chat/completions
   │                                              │
   │                                              ▼
   │                                    模型返回 assistant 消息
   │                                    ├─ 没有 tool_calls ──▶ 直接输出，结束
   │                                    └─ 有 tool_calls
   │                                              │
   │◀───────── 本地执行工具，结果作为 role=tool 消息追加 ─┘
   │
   └──▶ 带着完整历史再请求一次（循环，最多 maxSteps 轮）
```

关键点：

1. **每轮请求都带全部历史**：模型本身无状态，上下文靠 `messages` 自己维护。
2. **assistant 的 `tool_calls` 必须先入历史**，`role=tool` 的结果靠 `tool_call_id` 对号入座，否则接口会报错。
3. **工具结果永远是文本**，错误也转成文本回给模型，让模型自己决定怎么解释，而不是中断对话。
4. 循环有上限（`maxSteps = 8`），防止模型反复调用工具停不下来。

## 文件结构

| 文件 | 作用 |
| --- | --- |
| `main.go` | 读环境变量/参数，命令行交互或 `-q` 单次提问 |
| `llm.go` | 极简 OpenAI 兼容客户端：请求/响应结构、地址拼接、鉴权头 |
| `agent.go` | agent 主循环：请求模型 → 执行工具 → 回填结果 |
| `tools.go` | 工具声明（JSON Schema）与本地实现，示例是取时间和加法 |
| `agent_test.go` | 用 `httptest` 假模型服务验证工具调用循环，无需真实网关 |
| `llm_test.go` | 验证地址拼接、自定义鉴权头、额外请求头、模型名透传 |

## 配置公司网关

先找网关管理员要三样东西：**OpenAI 兼容的 base URL**、**token**、**可用的模型名**。

```powershell
$env:AI_BASE_URL = "https://gw.company.com/openai/v1"
$env:AI_API_KEY  = "<网关下发的 token>"
$env:AI_MODEL    = "<网关上的模型名>"

# 先只检查地址和鉴权头，不发请求
go run ./ai-agent -dump-url
```

输出示例：

```
POST https://gw.company.com/openai/v1/chat/completions
Authorization: Bearer****abcd
model: qwen-internal-72b
```

确认无误后再发请求：

```powershell
go run ./ai-agent -q "现在几点？再算一下 17+25"
go run ./ai-agent          # 交互模式，/reset 清空上下文，/exit 退出
```

### 环境变量

| 变量 | 必填 | 说明 |
| --- | --- | --- |
| `AI_BASE_URL` | 是 | 写到 `/v1` 或直接写完整 `/chat/completions` 都行，代码自动判断 |
| `AI_API_KEY` | 是 | 网关 token（不需要可填任意占位值） |
| `AI_MODEL` | 是 | 网关上的模型名 |
| `AI_AUTH_HEADER` | 否 | 默认 `Authorization`，网关若要求 `X-Api-Key` 等就改这里 |
| `AI_AUTH_PREFIX` | 否 | 默认 `Bearer `；自定义头默认不加前缀；写 `none` 表示明确不加 |
| `AI_EXTRA_HEADERS` | 否 | 额外请求头，格式 `X-A: v1,X-B: v2` |
| `AI_TIMEOUT` | 否 | 请求超时，如 `60s`，默认 120s |

命令行参数 `-base`、`-model` 可临时覆盖同名环境变量。

## 排查常见问题

| 现象 | 原因与处理 |
| --- | --- |
| `401` / `403` | 鉴权方式不对。用 `-dump-url` 确认头名；很多内网网关是 `X-Api-Key: <token>` 且**不加** `Bearer`，设 `AI_AUTH_HEADER=X-Api-Key` 即可 |
| `404` | 路径拼错。base 只需写到 `/v1`，不要重复带 `/chat/completions`；若网关路径是 `/openai/v1/chat/completions`，base 写到 `/openai/v1` 即可 |
| `400 tool_calls 相关报错` | 网关/模型不支持 function calling。先去掉 `tools` 试纯对话（临时把 `NewRegistry()` 换成空工具集），或换支持工具调用的模型 |
| `invalid model` | 模型名要用网关侧的名字，不能用 `deepseek-chat`/`gpt-4o` 这类官方名 |
| TLS 证书错误 | 内网自签 CA 时设置 `SSL_CERT_FILE=<ca.pem>`（或 `SSL_CERT_DIR`）后重试，不要直接关校验 |
| 卡住不返回 | 网关可能有首包超时或需要 `stream: false`；先用 `AI_TIMEOUT=30s` 试，仍不行就用 curl 对比 |

最省事的定位方式是用 curl 打同一地址，对比 ai-agent 发的请求：

```powershell
curl.exe -sS -X POST "$env:AI_BASE_URL/chat/completions" `
  -H "Authorization: Bearer $env:AI_API_KEY" -H "Content-Type: application/json" `
  -d "{\"model\":\"$env:AI_MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"
```

## 加一个工具

在 `tools.go` 的 `NewRegistry` 里加一段注册即可，模型会自己决定何时调用：

```go
r.register(
    ToolSchema{
        Name:        "weather",
        Description: "查询指定城市的天气。",
        Parameters: map[string]any{
            "type": "object",
            "properties": map[string]any{
                "city": map[string]any{"type": "string", "description": "城市名，如 杭州"},
            },
            "required": []string{"city"},
        },
    },
    func(args string) (string, error) {
        var in struct{ City string `json:"city"` }
        if err := json.Unmarshal([]byte(args), &in); err != nil {
            return "", err
        }
        return in.City + "：晴，26°C", nil
    },
)
```

## 继续往下扩展的方向

- **流式输出**：把 `Chat()` 改成解析 SSE（`stream: true` + `data: {...}` 逐行）。
- **多轮记忆**：现在历史只存在进程内存里，可以落盘或按 session ID 存 Redis。
- **并发工具**：一轮里模型返回多个 `tool_calls` 时可以并发执行。
- **可观测性**：每次请求记录 token 用量与耗时（响应里的 `usage` 字段）。
- **Key 管理**：不要提交到仓库；本仓库 `.gitignore` 已忽略 `.env`。
