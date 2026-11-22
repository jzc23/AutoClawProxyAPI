package translator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"autoclaw2api/internal/protocol"
)

// ResponsesToOpenAIRequest 将 OpenAI /v1/responses 请求转换为 /v1/chat/completions 请求。
func ResponsesToOpenAIRequest(raw []byte) (outBody []byte, model string, stream bool, err error) {
	var req map[string]any
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, "", false, fmt.Errorf("解析 Responses 请求失败: %w", err)
	}

	model, _ = req["model"].(string)
	model = strings.TrimSpace(model)
	stream, _ = req["stream"].(bool)

	// 网关无会话存储，previous_response_id 有状态用法会静默丢失上下文，明确报错比静默丢弃更安全
	if prevID, ok := req["previous_response_id"].(string); ok && strings.TrimSpace(prevID) != "" {
		return nil, "", false, fmt.Errorf("本网关为无状态代理，暂不支持 previous_response_id 有状态用法；请在 input 中携带完整历史（store:false 无状态模式）")
	}

	messages := make([]map[string]any, 0, 8)

	// 1. instructions -> system
	if inst, ok := req["instructions"].(string); ok && strings.TrimSpace(inst) != "" {
		messages = append(messages, map[string]any{
			"role":    "system",
			"content": inst,
		})
	}

	// 2. 解析 input（支持纯字符串或对象项数组）
	if input, exists := req["input"]; exists && input != nil {
		switch v := input.(type) {
		case string:
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": v,
			})
		case []any:
			messages = append(messages, convertResponsesInputItems(v)...)
		}
	}

	out := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   stream,
	}
	if stream {
		out["stream_options"] = map[string]any{"include_usage": true}
	}
	if v, ok := req["max_output_tokens"]; ok && v != nil {
		out["max_tokens"] = v
	} else if v, ok := req["max_tokens"]; ok && v != nil {
		out["max_tokens"] = v
	}
	if v, ok := req["temperature"]; ok && v != nil {
		out["temperature"] = v
	}
	if v, ok := req["top_p"]; ok && v != nil {
		out["top_p"] = v
	}

	// 3. 转换 tools（兼容 Responses 扁平 function 定义与标准嵌套定义）
	if rawTools, ok := req["tools"].([]any); ok && len(rawTools) > 0 {
		oaiTools := make([]map[string]any, 0, len(rawTools))
		for _, item := range rawTools {
			t, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := t["type"].(string)
			if typ != "" && typ != "function" {
				continue
			}
			if fn, ok := t["function"].(map[string]any); ok {
				oaiTools = append(oaiTools, map[string]any{
					"type":     "function",
					"function": fn,
				})
				continue
			}
			name, _ := t["name"].(string)
			if strings.TrimSpace(name) == "" {
				continue
			}
			fn := map[string]any{"name": name}
			if desc, ok := t["description"].(string); ok && desc != "" {
				fn["description"] = desc
			}
			if params, ok := t["parameters"]; ok && params != nil {
				fn["parameters"] = params
			} else {
				fn["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			oaiTools = append(oaiTools, map[string]any{
				"type":     "function",
				"function": fn,
			})
		}
		if len(oaiTools) > 0 {
			out["tools"] = oaiTools
		}
	}
	if tc, ok := req["tool_choice"]; ok && tc != nil {
		out["tool_choice"] = tc
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, "", false, err
	}
	return encoded, model, stream, nil
}

func convertResponsesInputItems(items []any) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			if s, okStr := raw.(string); okStr {
				out = append(out, map[string]any{"role": "user", "content": s})
			}
			continue
		}
		typ, _ := item["type"].(string)
		switch typ {
		case "function_call":
			callID, _ := item["call_id"].(string)
			if callID == "" {
				callID, _ = item["id"].(string)
			}
			if callID == "" {
				callID = "call_" + protocol.RandomHex(12)
			}
			name, _ := item["name"].(string)
			args, _ := item["arguments"].(string)
			if args == "" {
				args = "{}"
			}
			out = append(out, map[string]any{
				"role":    "assistant",
				"content": nil,
				"tool_calls": []map[string]any{
					{
						"id":   callID,
						"type": "function",
						"function": map[string]any{
							"name":      name,
							"arguments": args,
						},
					},
				},
			})
		case "function_call_output":
			callID, _ := item["call_id"].(string)
			outputStr := ""
			switch ov := item["output"].(type) {
			case string:
				outputStr = ov
			case nil:
			default:
				b, _ := json.Marshal(ov)
				outputStr = string(b)
			}
			out = append(out, map[string]any{
				"role":         "tool",
				"tool_call_id": callID,
				"content":      outputStr,
			})
		default:
			role, _ := item["role"].(string)
			if role == "" {
				role = "user"
			}
			out = append(out, map[string]any{
				"role":    role,
				"content": convertResponsesMessageContent(item["content"]),
			})
		}
	}
	return out
}

func convertResponsesMessageContent(raw any) any {
	switch c := raw.(type) {
	case string:
		return c
	case []any:
		parts := make([]map[string]any, 0, len(c))
		allText := true
		var sb strings.Builder
		for _, item := range c {
			p, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := p["type"].(string)
			switch typ {
			case "input_text", "output_text", "text":
				txt, _ := p["text"].(string)
				parts = append(parts, map[string]any{"type": "text", "text": txt})
				if sb.Len() > 0 {
					sb.WriteString("\n")
				}
				sb.WriteString(txt)
			case "input_image":
				allText = false
				imgURL, _ := p["image_url"].(string)
				if imgURL == "" {
					if m, ok := p["image_url"].(map[string]any); ok {
						imgURL, _ = m["url"].(string)
					}
				}
				if imgURL != "" {
					parts = append(parts, map[string]any{
						"type":      "image_url",
						"image_url": map[string]any{"url": imgURL},
					})
				}
			}
		}
		if allText {
			return sb.String()
		}
		return parts
	default:
		return ""
	}
}

// OpenAIToResponsesResponse 将 OpenAI Chat Completions 非流式响应转换为 OpenAI Responses 对象。
func OpenAIToResponsesResponse(raw []byte, fallbackModel string) ([]byte, error) {
	var resp struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content          any    `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Reasoning        string `json:"reasoning"`
				ToolCalls        []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("解析 OpenAI 响应失败: %w", err)
	}

	respID := resp.ID
	if respID == "" {
		respID = "resp_" + protocol.RandomHex(16)
	} else if !strings.HasPrefix(respID, "resp_") {
		respID = "resp_" + respID
	}
	created := resp.Created
	if created == 0 {
		created = NowUnix()
	}
	model := resp.Model
	if model == "" {
		model = fallbackModel
	}

	outputs := make([]map[string]any, 0, 3)
	outputText := ""
	status := "completed"
	var incompleteDetails any

	if len(resp.Choices) > 0 {
		ch := resp.Choices[0]
		if ch.FinishReason == "length" {
			status = "incomplete"
			incompleteDetails = map[string]any{"reason": "max_output_tokens"}
		} else if ch.FinishReason == "content_filter" {
			status = "incomplete"
			incompleteDetails = map[string]any{"reason": "content_filter"}
		}

		thinking := ch.Message.ReasoningContent
		if thinking == "" {
			thinking = ch.Message.Reasoning
		}
		if thinking != "" {
			outputs = append(outputs, map[string]any{
				"id":     "rs_" + protocol.RandomHex(12),
				"type":   "reasoning",
				"status": "completed",
				"summary": []map[string]any{
					{"type": "summary_text", "text": thinking},
				},
			})
		}

		outputText = extractOpenAIContentText(ch.Message.Content)
		if outputText != "" || len(ch.Message.ToolCalls) == 0 {
			outputs = append(outputs, map[string]any{
				"id":     "msg_" + protocol.RandomHex(12),
				"type":   "message",
				"status": "completed",
				"role":   "assistant",
				"content": []map[string]any{
					{
						"type":        "output_text",
						"text":        outputText,
						"annotations": []any{},
					},
				},
			})
		}

		for _, tc := range ch.Message.ToolCalls {
			callID := tc.ID
			if callID == "" {
				callID = "call_" + protocol.RandomHex(12)
			}
			outputs = append(outputs, map[string]any{
				"id":        "fc_" + protocol.RandomHex(12),
				"type":      "function_call",
				"status":    "completed",
				"call_id":   callID,
				"name":      tc.Function.Name,
				"arguments": tc.Function.Arguments,
			})
		}
	}

	total := resp.Usage.TotalTokens
	if total == 0 {
		total = resp.Usage.PromptTokens + resp.Usage.CompletionTokens
	}

	result := map[string]any{
		"id":                 respID,
		"object":             "response",
		"created_at":         created,
		"status":             status,
		"incomplete_details": incompleteDetails,
		"model":              model,
		"output":             outputs,
		"output_text":        outputText,
		"usage": map[string]any{
			"input_tokens":  resp.Usage.PromptTokens,
			"output_tokens": resp.Usage.CompletionTokens,
			"total_tokens":  total,
		},
	}
	return json.Marshal(result)
}

// ResponsesStreamConverter 将 OpenAI Chat Completions SSE 转换为 OpenAI Responses SSE 事件流。
type ResponsesStreamConverter struct {
	respID       string
	model        string
	created      int64
	seq          int
	started      bool
	completed    bool
	nextOutputIx int

	reasoningID    string
	reasoningIx    int
	reasoningAdded bool
	reasoningBuf   strings.Builder

	msgID    string
	msgIx    int
	msgAdded bool
	msgBuf   strings.Builder

	toolCalls    map[int]*responsesToolState
	finishReason string

	inputTokens  int64
	outputTokens int64
	totalTokens  int64
}

type responsesToolState struct {
	itemID   string
	callID   string
	name     string
	outputIx int
	added    bool
	argsBuf  strings.Builder
}

// NewResponsesStreamConverter 创建 Responses 流式状态机。
func NewResponsesStreamConverter(model string) *ResponsesStreamConverter {
	return &ResponsesStreamConverter{
		respID:      "resp_" + protocol.RandomHex(16),
		model:       model,
		created:     NowUnix(),
		reasoningIx: -1,
		msgIx:       -1,
		toolCalls:   make(map[int]*responsesToolState),
	}
}

func (c *ResponsesStreamConverter) nextSeq() int {
	c.seq++
	return c.seq
}

func (c *ResponsesStreamConverter) baseResponseObj(status string, output []map[string]any) map[string]any {
	obj := map[string]any{
		"id":         c.respID,
		"object":     "response",
		"created_at": c.created,
		"status":     status,
		"model":      c.model,
		"output":     output,
	}
	if status == "completed" || status == "incomplete" {
		total := c.totalTokens
		if total == 0 {
			total = c.inputTokens + c.outputTokens
		}
		obj["usage"] = map[string]any{
			"input_tokens":  c.inputTokens,
			"output_tokens": c.outputTokens,
			"total_tokens":  total,
		}
		obj["output_text"] = c.msgBuf.String()
	}
	return obj
}

// ProcessSSEData 处理单条 OpenAI Chat Completions SSE 的 data 载荷，返回 Responses SSE 事件帧列表。
func (c *ResponsesStreamConverter) ProcessSSEData(data []byte) [][]byte {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}
	if trimmed == "[DONE]" {
		return c.Finish()
	}

	var chunk struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Created int64  `json:"created"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Delta        struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Reasoning        string `json:"reasoning"`
				ToolCalls        []struct {
					Index    *int   `json:"index"`
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(trimmed), &chunk); err != nil {
		return nil
	}

	if chunk.ID != "" && !c.started {
		c.respID = "resp_" + strings.TrimPrefix(chunk.ID, "resp_")
	}
	if chunk.Model != "" && !c.started {
		c.model = chunk.Model
	}
	if chunk.Created > 0 && !c.started {
		c.created = chunk.Created
	}
	if chunk.Usage != nil {
		if chunk.Usage.PromptTokens > 0 {
			c.inputTokens = chunk.Usage.PromptTokens
		}
		if chunk.Usage.CompletionTokens > 0 {
			c.outputTokens = chunk.Usage.CompletionTokens
		}
		if chunk.Usage.TotalTokens > 0 {
			c.totalTokens = chunk.Usage.TotalTokens
		}
	}

	var frames [][]byte
	if !c.started {
		frames = append(frames, c.emitStart()...)
	}

	if len(chunk.Choices) > 0 {
		ch := chunk.Choices[0]
		if ch.FinishReason != "" {
			c.finishReason = ch.FinishReason
		}

		// 1. Reasoning 增量
		reasoning := ch.Delta.ReasoningContent
		if reasoning == "" {
			reasoning = ch.Delta.Reasoning
		}
		if reasoning != "" {
			if !c.reasoningAdded {
				c.reasoningAdded = true
				c.reasoningID = "rs_" + protocol.RandomHex(12)
				c.reasoningIx = c.nextOutputIx
				c.nextOutputIx++
				frames = append(frames, formatSSE("response.output_item.added", map[string]any{
					"type":            "response.output_item.added",
					"sequence_number": c.nextSeq(),
					"output_index":    c.reasoningIx,
					"item": map[string]any{
						"id":      c.reasoningID,
						"type":    "reasoning",
						"status":  "in_progress",
						"summary": []any{},
					},
				}))
			}
			c.reasoningBuf.WriteString(reasoning)
			frames = append(frames, formatSSE("response.reasoning_summary_text.delta", map[string]any{
				"type":            "response.reasoning_summary_text.delta",
				"sequence_number": c.nextSeq(),
				"item_id":         c.reasoningID,
				"output_index":    c.reasoningIx,
				"summary_index":   0,
				"delta":           reasoning,
			}))
		}

		// 2. Output Text 增量
		if ch.Delta.Content != "" {
			if !c.msgAdded {
				c.msgAdded = true
				c.msgID = "msg_" + protocol.RandomHex(12)
				c.msgIx = c.nextOutputIx
				c.nextOutputIx++
				frames = append(frames, formatSSE("response.output_item.added", map[string]any{
					"type":            "response.output_item.added",
					"sequence_number": c.nextSeq(),
					"output_index":    c.msgIx,
					"item": map[string]any{
						"id":      c.msgID,
						"type":    "message",
						"status":  "in_progress",
						"role":    "assistant",
						"content": []any{},
					},
				}))
				frames = append(frames, formatSSE("response.content_part.added", map[string]any{
					"type":            "response.content_part.added",
					"sequence_number": c.nextSeq(),
					"item_id":         c.msgID,
					"output_index":    c.msgIx,
					"content_index":   0,
					"part": map[string]any{
						"type":        "output_text",
						"text":        "",
						"annotations": []any{},
					},
				}))
			}
			c.msgBuf.WriteString(ch.Delta.Content)
			frames = append(frames, formatSSE("response.output_text.delta", map[string]any{
				"type":            "response.output_text.delta",
				"sequence_number": c.nextSeq(),
				"item_id":         c.msgID,
				"output_index":    c.msgIx,
				"content_index":   0,
				"delta":           ch.Delta.Content,
			}))
		}

		// 3. Function Call 增量
		for i, tc := range ch.Delta.ToolCalls {
			idx := i
			if tc.Index != nil {
				idx = *tc.Index
			}
			st, ok := c.toolCalls[idx]
			if !ok {
				st = &responsesToolState{
					itemID: "fc_" + protocol.RandomHex(12),
				}
				c.toolCalls[idx] = st
			}
			if tc.ID != "" {
				st.callID = tc.ID
			}
			if tc.Function.Name != "" {
				st.name = tc.Function.Name
			}
			if !st.added && (st.name != "" || st.callID != "") {
				if st.callID == "" {
					st.callID = fmt.Sprintf("call_%s_%d", protocol.RandomHex(8), idx)
				}
				st.added = true
				st.outputIx = c.nextOutputIx
				c.nextOutputIx++
				frames = append(frames, formatSSE("response.output_item.added", map[string]any{
					"type":            "response.output_item.added",
					"sequence_number": c.nextSeq(),
					"output_index":    st.outputIx,
					"item": map[string]any{
						"id":        st.itemID,
						"type":      "function_call",
						"status":    "in_progress",
						"call_id":   st.callID,
						"name":      st.name,
						"arguments": "",
					},
				}))
			}
			if st.added && tc.Function.Arguments != "" {
				st.argsBuf.WriteString(tc.Function.Arguments)
				frames = append(frames, formatSSE("response.function_call_arguments.delta", map[string]any{
					"type":            "response.function_call_arguments.delta",
					"sequence_number": c.nextSeq(),
					"item_id":         st.itemID,
					"output_index":    st.outputIx,
					"delta":           tc.Function.Arguments,
				}))
			}
		}
	}
	return frames
}

// Finish 结束 Responses 流并发送所有 done 事件与最终 response.completed 事件。
func (c *ResponsesStreamConverter) Finish() [][]byte {
	if c.completed {
		return nil
	}
	c.completed = true

	var frames [][]byte
	if !c.started {
		frames = append(frames, c.emitStart()...)
	}

	finalOutputs := make([]map[string]any, 0, 3)

	if c.reasoningAdded {
		rsText := c.reasoningBuf.String()
		item := map[string]any{
			"id":     c.reasoningID,
			"type":   "reasoning",
			"status": "completed",
			"summary": []map[string]any{
				{"type": "summary_text", "text": rsText},
			},
		}
		frames = append(frames, formatSSE("response.reasoning_summary_text.done", map[string]any{
			"type":            "response.reasoning_summary_text.done",
			"sequence_number": c.nextSeq(),
			"item_id":         c.reasoningID,
			"output_index":    c.reasoningIx,
			"summary_index":   0,
			"text":            rsText,
		}))
		frames = append(frames, formatSSE("response.output_item.done", map[string]any{
			"type":            "response.output_item.done",
			"sequence_number": c.nextSeq(),
			"output_index":    c.reasoningIx,
			"item":            item,
		}))
		finalOutputs = append(finalOutputs, item)
	}

	if c.msgAdded {
		txt := c.msgBuf.String()
		part := map[string]any{
			"type":        "output_text",
			"text":        txt,
			"annotations": []any{},
		}
		item := map[string]any{
			"id":      c.msgID,
			"type":    "message",
			"status":  "completed",
			"role":    "assistant",
			"content": []map[string]any{part},
		}
		frames = append(frames, formatSSE("response.output_text.done", map[string]any{
			"type":            "response.output_text.done",
			"sequence_number": c.nextSeq(),
			"item_id":         c.msgID,
			"output_index":    c.msgIx,
			"content_index":   0,
			"text":            txt,
		}))
		frames = append(frames, formatSSE("response.content_part.done", map[string]any{
			"type":            "response.content_part.done",
			"sequence_number": c.nextSeq(),
			"item_id":         c.msgID,
			"output_index":    c.msgIx,
			"content_index":   0,
			"part":            part,
		}))
		frames = append(frames, formatSSE("response.output_item.done", map[string]any{
			"type":            "response.output_item.done",
			"sequence_number": c.nextSeq(),
			"output_index":    c.msgIx,
			"item":            item,
		}))
		finalOutputs = append(finalOutputs, item)
	}

	if len(c.toolCalls) > 0 {
		keys := make([]int, 0, len(c.toolCalls))
		for k := range c.toolCalls {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			st := c.toolCalls[k]
			if !st.added {
				continue
			}
			args := st.argsBuf.String()
			item := map[string]any{
				"id":        st.itemID,
				"type":      "function_call",
				"status":    "completed",
				"call_id":   st.callID,
				"name":      st.name,
				"arguments": args,
			}
			frames = append(frames, formatSSE("response.function_call_arguments.done", map[string]any{
				"type":            "response.function_call_arguments.done",
				"sequence_number": c.nextSeq(),
				"item_id":         st.itemID,
				"output_index":    st.outputIx,
				"arguments":       args,
			}))
			frames = append(frames, formatSSE("response.output_item.done", map[string]any{
				"type":            "response.output_item.done",
				"sequence_number": c.nextSeq(),
				"output_index":    st.outputIx,
				"item":            item,
			}))
			finalOutputs = append(finalOutputs, item)
		}
	}

	status := "completed"
	if c.finishReason == "length" || c.finishReason == "content_filter" {
		status = "incomplete"
	}
	frames = append(frames, formatSSE("response.completed", map[string]any{
		"type":            "response.completed",
		"sequence_number": c.nextSeq(),
		"response":        c.baseResponseObj(status, finalOutputs),
	}))
	return frames
}

func (c *ResponsesStreamConverter) emitStart() [][]byte {
	c.started = true
	return [][]byte{
		formatSSE("response.created", map[string]any{
			"type":            "response.created",
			"sequence_number": c.nextSeq(),
			"response":        c.baseResponseObj("in_progress", []map[string]any{}),
		}),
		formatSSE("response.in_progress", map[string]any{
			"type":            "response.in_progress",
			"sequence_number": c.nextSeq(),
			"response":        c.baseResponseObj("in_progress", []map[string]any{}),
		}),
	}
}
