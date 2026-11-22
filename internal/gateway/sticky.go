package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/protocol"
)

// 会话粘性参数：同一会话在短时间内绑定到同一账号，提升上游 Prompt Cache 命中率，
// 显著降低多轮长上下文对话在多账号间来回跳导致的积分翻倍消耗。
const (
	stickyTTL        = 15 * time.Minute
	stickyMaxEntries = 1024
)

// stickyEntry 记录一条会话到账号的粘性绑定。
type stickyEntry struct {
	accID    string
	region   protocol.Region
	lastSeen time.Time
}

// stickyTable 维护会话键 → 账号的短时绑定表（惰性淘汰，容量上限防膨胀）。
type stickyTable struct {
	mu      sync.Mutex
	entries map[string]stickyEntry
}

func newStickyTable() *stickyTable {
	return &stickyTable{entries: make(map[string]stickyEntry)}
}

// lookup 查询会话当前绑定的账号；绑定已过期则顺带清除并返回空。
func (t *stickyTable) lookup(key string) (string, protocol.Region, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[key]
	if !ok {
		return "", "", false
	}
	if time.Since(e.lastSeen) > stickyTTL {
		delete(t.entries, key)
		return "", "", false
	}
	return e.accID, e.region, true
}

// bind 写入或刷新会话绑定，容量超限时先淘汰过期项再按最久未使用淘汰。
func (t *stickyTable) bind(key, accID string, region protocol.Region) {
	if key == "" || accID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.entries[key]; !exists && len(t.entries) >= stickyMaxEntries {
		now := time.Now()
		for k, e := range t.entries {
			if now.Sub(e.lastSeen) > stickyTTL {
				delete(t.entries, k)
			}
		}
		for len(t.entries) >= stickyMaxEntries {
			oldestKey := ""
			var oldest time.Time
			for k, e := range t.entries {
				if oldestKey == "" || e.lastSeen.Before(oldest) {
					oldestKey, oldest = k, e.lastSeen
				}
			}
			if oldestKey == "" {
				break
			}
			delete(t.entries, oldestKey)
		}
	}
	t.entries[key] = stickyEntry{accID: accID, region: region, lastSeen: time.Now()}
}

// unbind 解除会话绑定（粘性账号调用失败切号时调用，后续请求改绑新账号）。
func (t *stickyTable) unbind(key string) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, key)
}

// sessionKeyForChatBody 计算短时会话粘性键（对三种入站协议统一在转换后的
// OpenAI chat 请求体上计算）。优先取客户端显式提供的会话标识；缺省时回退为
// 模型 + 首条 system/developer/user 消息内容的哈希（多轮对话中该前缀稳定不变）。
func sessionKeyForChatBody(requestedModel string, rawBody []byte) string {
	if len(rawBody) == 0 {
		return ""
	}
	var probe struct {
		PromptCacheKey     string          `json:"prompt_cache_key"`
		SafetyIdentifier   string          `json:"safety_identifier"`
		User               string          `json:"user"`
		Conversation       string          `json:"conversation"`
		PreviousResponseID string          `json:"previous_response_id"`
		SessionID          string          `json:"session_id"`
		Metadata           json.RawMessage `json:"metadata"`
		Messages           []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(rawBody, &probe); err != nil {
		return ""
	}

	// 1) 显式会话标识（请求级 ID）优先，天然跨轮稳定
	for _, v := range []string{probe.PromptCacheKey, probe.SafetyIdentifier, probe.SessionID, probe.Conversation, probe.PreviousResponseID, probe.User} {
		if s := strings.TrimSpace(v); s != "" {
			return requestedModel + "|e:" + s
		}
	}
	if len(probe.Metadata) > 0 {
		var md map[string]any
		if json.Unmarshal(probe.Metadata, &md) == nil {
			for _, k := range []string{"session_id", "sessionId", "conversation_id"} {
				if v, ok := md[k].(string); ok && strings.TrimSpace(v) != "" {
					return requestedModel + "|e:" + strings.TrimSpace(v)
				}
			}
		}
	}

	// 2) 回退：模型 + 首条 system/developer/user 消息内容哈希
	for _, msg := range probe.Messages {
		switch strings.ToLower(strings.TrimSpace(msg.Role)) {
		case "system", "developer", "user":
			if len(msg.Content) == 0 {
				continue
			}
			sum := sha256.Sum256(msg.Content)
			return requestedModel + "|h:" + hex.EncodeToString(sum[:8])
		}
	}
	return ""
}
