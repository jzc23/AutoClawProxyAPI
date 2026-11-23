package protocol

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCalcAuthSign(t *testing.T) {
	ts := "1700000000"
	got := CalcAuthSign(ts)
	sum := md5.Sum([]byte("100003&1700000000&38d2391985e2369a5fb8227d8e6cd5e5"))
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("签名计算不一致: got=%s want=%s", got, want)
	}
}

func TestThreeHeaderSetsIsolated(t *testing.T) {
	reqChat, _ := http.NewRequest(http.MethodPost, "https://example.com/chat", nil)
	AttachChatHeaders(reqChat, "tok-123", "glm-5.3-flash")
	if reqChat.Header.Get("X-Tm") != "mac" {
		t.Fatalf("对话头 X-Tm 应为 mac，实际: %s", reqChat.Header.Get("X-Tm"))
	}
	if reqChat.Header.Get("Authorization") != "Bearer tok-123" || reqChat.Header.Get("X-Authorization") != "Bearer tok-123" {
		t.Fatalf("对话头缺少双通道 Bearer")
	}
	if reqChat.Header.Get("X-Request-Model") != "glm-5.3-flash" {
		t.Fatalf("对话头缺少 X-Request-Model")
	}
	traceID := reqChat.Header.Get("X-Trace-Id")
	xTraceID := reqChat.Header.Get("x_trace_id")
	if len(traceID) != 16 || xTraceID != traceID || xTraceID == "autoclaw2api" {
		t.Fatalf("对话头 X-Trace-Id 与 x_trace_id 应共用 16 位随机 hex 且不得硬编码项目名: X-Trace-Id=%s x_trace_id=%s", traceID, xTraceID)
	}
	if reqChat.Header.Get("X-Auth-Sign") != "" {
		t.Fatalf("对话头不应混入 UserAPI 签名头")
	}

	reqUser, _ := http.NewRequest(http.MethodPost, "https://example.com/user", nil)
	AttachUserHeaders(reqUser, "tok-123")
	if reqUser.Header.Get("X-Auth-Appid") != "100003" || reqUser.Header.Get("X-Auth-Sign") == "" {
		t.Fatalf("UserAPI 头缺少签名字段")
	}
	if reqUser.Header.Get("X-Harness-Type") != "zcode" || reqUser.Header.Get("User-Agent") != "AutoClaw/1.18.5" {
		t.Fatalf("UserAPI 头缺少客户端指纹或 UA")
	}

	reqOAuth, _ := http.NewRequest(http.MethodPost, "https://example.com/oauth", nil)
	AttachOAuthHeaders(reqOAuth, "")
	if reqOAuth.Header.Get("X-Tm") != "linux" {
		t.Fatalf("国际版 OAuth 头 X-Tm 必须固定为 linux，实际: %s", reqOAuth.Header.Get("X-Tm"))
	}
	if reqOAuth.Header.Get("X-Auth-Sign") == "" {
		t.Fatalf("国际版 OAuth 头缺少签名")
	}
}

func TestPrepareOutboundChatBodyAnti406(t *testing.T) {
	input := `{
		"model": "autoclawcn:glm-5.3-flash",
		"user": "someone",
		"max_tokens": 4096,
		"max_completion_tokens": 2048,
		"messages": [
			{"role": "system", "content": "You are Claude Code. Help the user."},
			{"role": "developer", "content": [{"type":"text","text":"You are a coding agent running in the Codex CLI tool."}]},
			{"role": "user", "content": "你好"}
		]
	}`
	outBytes := PrepareOutboundChatBody([]byte(input), "glm-5.3-flash")
	var out map[string]any
	if err := json.Unmarshal(outBytes, &out); err != nil {
		t.Fatalf("反序列化规整结果失败: %v", err)
	}
	if _, exists := out["user"]; exists {
		t.Fatalf("出站请求未删除 user 字段")
	}
	if out["model"] != "glm-5.3-flash" {
		t.Fatalf("model 未重写: %v", out["model"])
	}
	if out["max_tokens"].(float64) != 10000 {
		t.Fatalf("小正整数 max_tokens 应抬升至 10000，实际: %v", out["max_tokens"])
	}
	if out["max_completion_tokens"].(float64) != 2048 {
		t.Fatalf("max_completion_tokens 不应被修改，实际: %v", out["max_completion_tokens"])
	}

	msgs := out["messages"].([]any)
	sysContent := msgs[0].(map[string]any)["content"].(string)
	if !strings.HasPrefix(sysContent, SystemIdentityPrefix) {
		t.Fatalf("首条 system 未前置白名单前缀: %s", sysContent)
	}
	if strings.Contains(strings.ToLower(sysContent), "claude code") {
		t.Fatalf("外来身份句未替换: %s", sysContent)
	}
	devParts := msgs[1].(map[string]any)["content"].([]any)
	devText := devParts[0].(map[string]any)["text"].(string)
	if strings.Contains(strings.ToLower(devText), "codex") {
		t.Fatalf("developer 文本块中的外来身份句未替换: %s", devText)
	}

	// 幂等性验证：二次处理不重复拼接前缀
	outBytes2 := PrepareOutboundChatBody(outBytes, "glm-5.3-flash")
	var out2 map[string]any
	_ = json.Unmarshal(outBytes2, &out2)
	sysContent2 := out2["messages"].([]any)[0].(map[string]any)["content"].(string)
	if strings.Count(sysContent2, SystemIdentityLine) != 1 {
		t.Fatalf("重复规整不应重复拼接身份前缀: %s", sysContent2)
	}
}

func TestPrepareOutboundChatBodyNoSystemAndNoMaxTokens(t *testing.T) {
	input := `{"model":"openclaw","messages":[{"role":"user","content":"hi"}]}`
	outBytes := PrepareOutboundChatBody([]byte(input), "openclaw")
	var out map[string]any
	_ = json.Unmarshal(outBytes, &out)
	if _, exists := out["max_tokens"]; exists {
		t.Fatalf("未传 max_tokens 时不应主动注入")
	}
	msgs := out["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("缺少 system 时应插入首条 system 消息，实际数量: %d", len(msgs))
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "system" || first["content"] != SystemIdentityPrefix {
		t.Fatalf("插入的首条 system 消息内容不符: %+v", first)
	}
}

func TestClassifyErrorLevels(t *testing.T) {
	e1 := ClassifyError(200, 410000, "not logged in", 0)
	if e1.Kind != ErrorKindSessionDead {
		t.Fatalf("410000 应归类为会话失效，实际: %s", e1.Kind)
	}
	e2 := ClassifyError(402, 0, "余额不足", 0)
	if e2.Kind != ErrorKindHardCredit {
		t.Fatalf("402 应归类为积分不足长冷却，实际: %s", e2.Kind)
	}
	e3 := ClassifyError(429, 0, "rate limit", 15*time.Second)
	if e3.Kind != ErrorKindSoftRate || e3.RetryAfter != 15*time.Second {
		t.Fatalf("429 分类或 Retry-After 解析异常: %+v", e3)
	}
	if ParseRetryAfter("12") != 12*time.Second {
		t.Fatalf("Retry-After 秒数解析失败")
	}
}

func TestClassifyErrorRateLimitVsCreditAnd403(t *testing.T) {
	// 429 + "rate limit reached" 文案：必须先按 429 判软限流短冷却，
	// 而不是命中 hard_credit 的 "limit reached" 短语导致长冷却
	e := ClassifyError(429, 0, `{"error":"rate limit reached, please retry"}`, 0)
	if e.Kind != ErrorKindSoftRate {
		t.Fatalf("429 应归类 soft_rate，实际: %s", e.Kind)
	}
	// 配额文案（无 429 状态）仍判积分不足
	e2 := ClassifyError(200, 0, "usage limit exceeded for this account", 0)
	if e2.Kind != ErrorKindHardCredit {
		t.Fatalf("配额文案应归类 hard_credit，实际: %s", e2.Kind)
	}
	// 403 风控/WAF：中等冷却并切号，不再判会话死亡
	e3 := ClassifyError(403, 0, "forbidden", 0)
	if e3.Kind != ErrorKindRiskControl {
		t.Fatalf("403 应归类 risk_control，实际: %s", e3.Kind)
	}
	if !e3.ShouldFailover() {
		t.Fatal("risk_control 应触发切号")
	}
	// 401 仍判会话失效
	e4 := ClassifyError(401, 0, "unauthorized", 0)
	if e4.Kind != ErrorKindSessionDead {
		t.Fatalf("401 应归类 session_dead，实际: %s", e4.Kind)
	}
}

func TestParseModelsConfigBothShapes(t *testing.T) {
	topLevel := `{"models":[{"id":"glm-5","name":"GLM 5","creditConsumptionLevel":"中","input":["text","image"],"reasoning":true}]}`
	m1, err := ParseModelsConfigJSON([]byte(topLevel), 200, RegionGlobal)
	if err != nil || len(m1) != 1 || m1[0].Region != RegionGlobal || m1[0].CreditLevel != "中" || !m1[0].SupportsImages || !m1[0].IsReasoning {
		t.Fatalf("顶层 models 解析失败: %+v, err=%v", m1, err)
	}

	wrapped := `{"code":0,"msg":"ok","data":{"models":[{"id":"cn-model-x","creditConsumptionLevel":2}]}}`
	m2, err := ParseModelsConfigJSON([]byte(wrapped), 200, RegionCN)
	if err != nil || len(m2) != 1 || m2[0].Region != RegionCN || m2[0].CreditLevel != "档位 2" {
		t.Fatalf("data.models 信封解析失败: %+v, err=%v", m2, err)
	}
}

func TestParseOAuthCallbackURL(t *testing.T) {
	p, code, state, ok := ParseOAuthCallbackURL("http://localhost:53699/auth/callback-zai?code=abc123&state=xyz")
	if !ok || p != OAuthProviderZai || code != "abc123" || state != "xyz" {
		t.Fatalf("Zai 回调解析失败: provider=%s code=%s state=%s ok=%v", p, code, state, ok)
	}
	_, _, _, okBad := ParseOAuthCallbackURL("https://evil.example.com/auth/callback-google?code=123")
	if okBad {
		t.Fatalf("非回环域名回调应被拒绝")
	}
}

func TestJWTTTLAndOAuthCallbackErrorAndStreamUsage(t *testing.T) {
	// 1. JWT iat/exp/ttl 解析（24h 国际版与 28d 国内版）
	makeJWT := func(iat, exp int64) string {
		b, _ := json.Marshal(map[string]any{"iat": iat, "exp": exp, "sub": "u1"})
		enc := func(raw []byte) string {
			const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
			_ = tbl
			return strings.TrimRight(strings.NewReplacer("+", "-", "/", "_").Replace(
				string(mustBase64Std(raw)),
			), "=")
		}
		return enc([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + enc(b) + ".sig"
	}
	jwt24h := makeJWT(1700000000, 1700086400)
	if ttl := JWTTTLSeconds(jwt24h); ttl != 86400 {
		t.Fatalf("24h JWT TTL 解析异常: got=%d want=86400", ttl)
	}
	if iat := JWTIssuedAtSeconds(jwt24h); iat != 1700000000 {
		t.Fatalf("JWT iat 解析异常: got=%d", iat)
	}

	// 2. OAuth 第二步换票 630014 单独映射且保留原始 code 与 msg
	cbMsg := explainOAuthCallbackBizError(630014, "抱歉,审核失败,请重新尝试")
	if strings.Contains(cbMsg, "滑动验证") || !strings.Contains(cbMsg, "code=630014") || !strings.Contains(cbMsg, "抱歉,审核失败,请重新尝试") {
		t.Fatalf("换票阶段 630014 报错文案不符合预期: %s", cbMsg)
	}

	// 3. 流式默认补齐 stream_options.include_usage=true，显式 false 不覆盖，非流式不注入
	streamIn := `{"model":"glm-5","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	var streamOut map[string]any
	_ = json.Unmarshal(PrepareOutboundChatBody([]byte(streamIn), "glm-5"), &streamOut)
	so, ok := streamOut["stream_options"].(map[string]any)
	if !ok || so["include_usage"] != true {
		t.Fatalf("流式请求应默认补齐 stream_options.include_usage=true: %+v", streamOut["stream_options"])
	}

	explicitFalse := `{"model":"glm-5","stream":true,"stream_options":{"include_usage":false},"messages":[{"role":"user","content":"hi"}]}`
	var falseOut map[string]any
	_ = json.Unmarshal(PrepareOutboundChatBody([]byte(explicitFalse), "glm-5"), &falseOut)
	if falseOut["stream_options"].(map[string]any)["include_usage"] != false {
		t.Fatalf("显式传入 include_usage=false 不应被覆盖")
	}

	nonStream := `{"model":"glm-5","stream":false,"messages":[{"role":"user","content":"hi"}]}`
	var nonStreamOut map[string]any
	_ = json.Unmarshal(PrepareOutboundChatBody([]byte(nonStream), "glm-5"), &nonStreamOut)
	if _, exists := nonStreamOut["stream_options"]; exists {
		t.Fatalf("非流式请求不应注入 stream_options")
	}
}

func mustBase64Std(b []byte) []byte {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	out := make([]byte, 0, ((len(b)+2)/3)*4)
	for i := 0; i < len(b); i += 3 {
		var n uint32
		rem := len(b) - i
		n |= uint32(b[i]) << 16
		if rem > 1 {
			n |= uint32(b[i+1]) << 8
		}
		if rem > 2 {
			n |= uint32(b[i+2])
		}
		out = append(out, chars[(n>>18)&63], chars[(n>>12)&63])
		if rem > 1 {
			out = append(out, chars[(n>>6)&63])
		} else {
			out = append(out, '=')
		}
		if rem > 2 {
			out = append(out, chars[n&63])
		} else {
			out = append(out, '=')
		}
	}
	return out
}
