package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// deriveStickySessionKey 为多轮对话推导短时会话粘性键（同时覆盖 /v1/chat/completions、/v1/responses 与 /v1/messages）：
//  1. 优先使用客户端显式会话/缓存标识（prompt_cache_key、conversation / conversation_id、previous_response_id、session_id、metadata.user_id）；
//  2. 若未显式携带，则基于「模型 + 首条 system/developer 文本 + 首条 user 文本」计算稳定哈希，
//     确保同一多轮长上下文对话稳定命中同一账号以提升上游 Prompt Cache 命中率。
func deriveStickySessionKey(requestedModel string, rawReqBody, chatBody []byte) string {
	model := strings.ToLower(strings.TrimSpace(requestedModel))
	if model == "" {
		return ""
	}

	if explicit := extractExplicitSessionKey(rawReqBody); explicit != "" {
		return hashStickyParts(model, "explicit", explicit)
	}

	sysPrefix, userPrefix := extractConversationPrefix(chatBody)
	if sysPrefix == "" && userPrefix == "" {
		return ""
	}
	return hashStickyParts(model, "prefix", sysPrefix, userPrefix)
}

func hashStickyParts(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func extractExplicitSessionKey(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	for _, field := range []string{"prompt_cache_key", "conversation_id", "previous_response_id", "session_id"} {
		if s := stringField(payload[field]); s != "" {
			return field + ":" + s
		}
	}
	if conv, ok := payload["conversation"]; ok {
		if s := stringField(conv); s != "" {
			return "conversation:" + s
		}
		if obj, ok := conv.(map[string]any); ok {
			if s := stringField(obj["id"]); s != "" {
				return "conversation:" + s
			}
		}
	}
	if meta, ok := payload["metadata"].(map[string]any); ok {
		if s := stringField(meta["user_id"]); s != "" {
			return "meta_user:" + s
		}
		if s := stringField(meta["session_id"]); s != "" {
			return "meta_session:" + s
		}
	}
	return ""
}

func extractConversationPrefix(chatBody []byte) (firstSys, firstUser string) {
	if len(chatBody) == 0 {
		return "", ""
	}
	var payload map[string]any
	if err := json.Unmarshal(chatBody, &payload); err != nil {
		return "", ""
	}
	msgs, ok := payload["messages"].([]any)
	if !ok {
		return "", ""
	}
	for _, item := range msgs {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role := strings.ToLower(strings.TrimSpace(stringField(m["role"])))
		text := extractMessageText(m["content"])
		if text == "" {
			continue
		}
		if len(text) > 2048 {
			text = text[:2048]
		}
		switch role {
		case "system", "developer":
			if firstSys == "" {
				firstSys = text
			}
		case "user":
			if firstUser == "" {
				firstUser = text
			}
		}
		if firstSys != "" && firstUser != "" {
			break
		}
	}
	return firstSys, firstUser
}

func extractMessageText(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		var parts []string
		for _, p := range v {
			obj, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if s := stringField(obj["text"]); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	default:
		return ""
	}
}

func stringField(v any) string {
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}
