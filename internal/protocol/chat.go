package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const defaultChatTimeout = 10 * time.Minute

// ChatRequest 定义向 AutoClaw 上游发起对话补全的参数。
type ChatRequest struct {
	BaseURL string
	Token   string
	Model   string
	Body    []byte
}

// ChatCompletionAggregated 执行非流式对话补全，返回 OpenAI 兼容的完整 JSON 响应。
//
// 上游侧实际以 stream:true 发起（内部聚合）：流式响应头会立刻返回且数据持续流动，
// 避免上游网关/CDN 因"长时间零字节等待完整生成"对非流式请求掐断返回 504；
// 聚合过程完全在网关内部完成，客户端仍拿到与非流式语义一致的完整 JSON，
// 且流式中途失败可正常触发切号重试。
func ChatCompletionAggregated(ctx context.Context, client *http.Client, req ChatRequest) ([]byte, error) {
	if client == nil {
		client = &http.Client{Timeout: defaultChatTimeout}
	}
	// 请求体切换为内部流式（PrepareOutboundChatBody 会补齐 stream_options.include_usage）
	var payload map[string]any
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		payload = map[string]any{}
	}
	payload["stream"] = true
	modified, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		modified = req.Body
	}

	resp, err := ChatCompletionStream(ctx, client, ChatRequest{
		BaseURL: req.BaseURL,
		Token:   req.Token,
		Model:   req.Model,
		Body:    modified,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, errRead := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if errRead != nil {
		return nil, &UpstreamError{
			HTTPStatus: resp.StatusCode,
			Kind:       ErrorKindTransient,
			Message:    "读取上游流式响应失败: " + errRead.Error(),
		}
	}
	return AssembleChatCompletionFromSSE(raw, req.Model)
}

// AssembleChatCompletionFromSSE 将上游 SSE 数据流聚合为完整 OpenAI chat.completion JSON。
// 若数据流中不存在任何 SSE data 行，则整体按非流式 JSON 响应处理（兼容上游忽略 stream 参数的情况）。
func AssembleChatCompletionFromSSE(raw []byte, fallbackModel string) ([]byte, error) {
	var (
		contentBuf     strings.Builder
		reasoningBuf   strings.Builder
		finishReason   string
		usageRaw       json.RawMessage
		firstID        string
		firstCreated   int64
		firstModel     string
		sawDataPayload bool
	)
	toolAcc := make(map[int]*struct {
		id   string
		name string
		args strings.Builder
	})

	for _, line := range bytes.Split(raw, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(trimmed[len("data:"):])
		if len(payload) == 0 {
			continue
		}
		sawDataPayload = true
		if bytes.Equal(payload, []byte("[DONE]")) {
			break
		}
		// chunk 结构体必须在循环内声明：复用结构体时 json.Unmarshal 不会清空
		// JSON 中缺席的字段，上一条 delta 的 content/reasoning 会错误残留翻倍
		var chunk struct {
			ID      string `json:"id"`
			Created int64  `json:"created"`
			Model   string `json:"model"`
			Choices []struct {
				FinishReason string `json:"finish_reason"`
				Delta        struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					Reasoning        string `json:"reasoning"`
					ToolCalls        []struct {
						Index    *int   `json:"index"`
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if err := json.Unmarshal(payload, &chunk); err != nil {
			continue
		}
		if firstID == "" && chunk.ID != "" {
			firstID = chunk.ID
		}
		if firstCreated == 0 && chunk.Created > 0 {
			firstCreated = chunk.Created
		}
		if firstModel == "" && chunk.Model != "" {
			firstModel = chunk.Model
		}
		if len(chunk.Usage) > 0 && string(chunk.Usage) != "null" {
			usageRaw = append(json.RawMessage(nil), chunk.Usage...)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		ch := chunk.Choices[0]
		if ch.FinishReason != "" {
			finishReason = ch.FinishReason
		}
		contentBuf.WriteString(ch.Delta.Content)
		if ch.Delta.ReasoningContent != "" {
			reasoningBuf.WriteString(ch.Delta.ReasoningContent)
		} else if ch.Delta.Reasoning != "" {
			reasoningBuf.WriteString(ch.Delta.Reasoning)
		}
		for _, tc := range ch.Delta.ToolCalls {
			idx := 0
			if tc.Index != nil {
				idx = *tc.Index
			}
			acc, ok := toolAcc[idx]
			if !ok {
				acc = &struct {
					id   string
					name string
					args strings.Builder
				}{}
				toolAcc[idx] = acc
			}
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			acc.args.WriteString(tc.Function.Arguments)
		}
	}

	// 上游忽略 stream 参数时返回的是普通 JSON：无 SSE data 行时按原始响应处理
	if !sawDataPayload {
		if len(bytes.TrimSpace(raw)) == 0 {
			return nil, &UpstreamError{
				HTTPStatus: 0,
				Kind:       ErrorKindTransient,
				Message:    "上游流式响应为空",
			}
		}
		if code, msg, isErr := DetectEnvelopeError(raw); isErr {
			return nil, ClassifyError(0, code, msg, 0)
		}
		return raw, nil
	}

	// 流异常中断（无 finish_reason）时不返回半截答案，交由上层切号重试
	if finishReason == "" {
		return nil, &UpstreamError{
			Kind:    ErrorKindTransient,
			Message: "上游流式响应提前中断（未收到 finish_reason），已放弃本次聚合",
		}
	}

	toolIndexes := make([]int, 0, len(toolAcc))
	for idx := range toolAcc {
		toolIndexes = append(toolIndexes, idx)
	}
	sort.Ints(toolIndexes)
	toolCalls := make([]map[string]any, 0, len(toolAcc))
	for _, idx := range toolIndexes {
		acc := toolAcc[idx]
		id := strings.TrimSpace(acc.id)
		if id == "" {
			id = "call_" + RandomHex(12)
		}
		args := acc.args.String()
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		toolCalls = append(toolCalls, map[string]any{
			"id":   id,
			"type": "function",
			"function": map[string]any{
				"name":      acc.name,
				"arguments": args,
			},
		})
	}

	content := contentBuf.String()
	msg := map[string]any{"role": "assistant", "content": content}
	if reasoningBuf.Len() > 0 {
		msg["reasoning_content"] = reasoningBuf.String()
	}
	if len(toolCalls) > 0 {
		if content == "" {
			msg["content"] = nil
		}
		msg["tool_calls"] = toolCalls
	}

	created := firstCreated
	if created <= 0 {
		created = time.Now().Unix()
	}
	model := firstModel
	if model == "" {
		model = fallbackModel
	}
	id := firstID
	if id == "" {
		id = "chatcmpl-" + RandomHex(12)
	}
	choice := map[string]any{
		"index":         0,
		"message":       msg,
		"finish_reason": finishReason,
	}
	out := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": []any{choice},
	}
	if len(usageRaw) > 0 {
		out["usage"] = json.RawMessage(usageRaw)
	} else {
		out["usage"] = map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, &UpstreamError{Kind: ErrorKindTransient, Message: "聚合上游流式响应失败: " + err.Error()}
	}
	return encoded, nil
}

// ChatCompletionStream 发起流式 SSE 对话请求，成功时返回开放的 *http.Response（由调用方关闭 Body）。
func ChatCompletionStream(ctx context.Context, client *http.Client, req ChatRequest) (*http.Response, error) {
	if client == nil {
		client = &http.Client{}
	}
	endpoint := req.BaseURL + PathChat
	prepared := PrepareOutboundChatBody(req.Body, req.Model)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(prepared))
	if err != nil {
		return nil, fmt.Errorf("构造流式对话请求失败: %w", err)
	}
	AttachChatHeaders(httpReq, req.Token, req.Model)

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, &UpstreamError{
			Kind:    ErrorKindTransient,
			Message: "请求上游流式接口失败: " + err.Error(),
		}
	}
	retryAfter := ParseRetryAfter(resp.Header.Get("Retry-After"))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, ClassifyError(resp.StatusCode, 0, string(raw), retryAfter)
	}
	// 检查上游是否以 HTTP 200 返回了 JSON 业务错误信封（非 text/event-stream）
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !bytes.Contains([]byte(ct), []byte("event-stream")) {
		raw, errRead := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if errRead != nil {
			return nil, &UpstreamError{
				HTTPStatus: resp.StatusCode,
				Kind:       ErrorKindTransient,
				Message:    "读取上游非流式错误响应失败: " + errRead.Error(),
			}
		}
		if code, msg, isErr := DetectEnvelopeError(raw); isErr {
			return nil, ClassifyError(resp.StatusCode, code, msg, retryAfter)
		}
		// 即使 Content-Type 不是 event-stream，若内容是合法流式或 JSON，包装回 Body 给调用方处理
		resp.Body = io.NopCloser(bytes.NewReader(raw))
	}
	return resp, nil
}
