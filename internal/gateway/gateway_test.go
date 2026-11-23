package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
	"autoclaw2api/internal/scheduler"
)

func setupTestServer(t *testing.T, upstreamURL string) (*Server, http.Handler, *proxypool.Manager, *config.Manager) {
	t.Helper()
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	_ = os.MkdirAll(authDir, 0o755)
	_ = os.WriteFile(filepath.Join(authDir, "autoclawcn-a1.json"), []byte(`{"region":"cn","accessToken":"tok-1","userId":"a1"}`), 0o600)
	_ = os.WriteFile(filepath.Join(authDir, "autoclawcn-a2.json"), []byte(`{"region":"cn","accessToken":"tok-2","userId":"a2"}`), 0o600)

	cfgMgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.Upstream.CNBaseURL = upstreamURL
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	logger := logging.New("error", 50)
	proxyMgr, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := accounts.NewPool(cfgMgr, logger)
	if err != nil {
		t.Fatal(err)
	}
	router := accounts.NewRouter(cfgMgr, pool, proxyMgr, logger)
	sched := scheduler.New(cfgMgr, pool, router, proxyMgr, logger)
	srv := NewServer(cfgMgr, pool, router, proxyMgr, sched, logger)
	return srv, srv.Handler(), proxyMgr, cfgMgr
}

func TestPanelPasswordSetupLoginAndVKeyIsolation(t *testing.T) {
	_, handler, _, _ := setupTestServer(t, "http://127.0.0.1:1")

	// 1. 首次访问未初始化时，auth/status 返回 initialized=false，访问管理接口返回 401 panel_setup_required
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/admin/auth/status", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"initialized":false`) {
		t.Fatalf("未初始化状态异常: %d %s", rec.Code, rec.Body.String())
	}

	recBlocked := httptest.NewRecorder()
	handler.ServeHTTP(recBlocked, httptest.NewRequest(http.MethodGet, "/api/admin/overview", nil))
	if recBlocked.Code != http.StatusUnauthorized || !strings.Contains(recBlocked.Body.String(), "panel_setup_required") {
		t.Fatalf("未设置面板密码时应拦截管理请求: %d %s", recBlocked.Code, recBlocked.Body.String())
	}

	// 2. 初始化面板密码并自动签发会话
	setupReq := httptest.NewRequest(http.MethodPost, "/api/admin/auth/setup", bytes.NewBufferString(`{"password":"my-panel-pass","confirm_password":"my-panel-pass"}`))
	setupRec := httptest.NewRecorder()
	handler.ServeHTTP(setupRec, setupReq)
	if setupRec.Code != http.StatusOK {
		t.Fatalf("面板密码初始化失败: %d %s", setupRec.Code, setupRec.Body.String())
	}
	var setupResp struct {
		Session string `json:"session"`
	}
	_ = json.Unmarshal(setupRec.Body.Bytes(), &setupResp)
	if setupResp.Session == "" {
		t.Fatalf("初始化成功后应返回会话 token")
	}

	// 3. 使用面板会话一键生成 vKey- 访问密钥
	createKeyReq := httptest.NewRequest(http.MethodPost, "/api/admin/apikeys", bytes.NewBufferString(`{"name":"Claude Code 专用"}`))
	createKeyReq.Header.Set("X-Panel-Session", setupResp.Session)
	createKeyRec := httptest.NewRecorder()
	handler.ServeHTTP(createKeyRec, createKeyReq)
	if createKeyRec.Code != http.StatusOK {
		t.Fatalf("生成 vKey 失败: %d %s", createKeyRec.Code, createKeyRec.Body.String())
	}
	var keyResp struct {
		APIKey config.APIKeyItem `json:"api_key"`
	}
	_ = json.Unmarshal(createKeyRec.Body.Bytes(), &keyResp)
	if !strings.HasPrefix(keyResp.APIKey.Key, "vKey-") {
		t.Fatalf("生成的 API Key 必须以 vKey- 开头，实际: %s", keyResp.APIKey.Key)
	}

	// 4. 验证面板密码不能充当 /v1/models 的 API Key，只有 vKey 可以
	badModelReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	badModelReq.Header.Set("Authorization", "Bearer my-panel-pass")
	badModelRec := httptest.NewRecorder()
	handler.ServeHTTP(badModelRec, badModelReq)
	if badModelRec.Code != http.StatusUnauthorized {
		t.Fatalf("面板密码不应通过网关 API Key 鉴权，实际: %d", badModelRec.Code)
	}

	okModelReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	okModelReq.Header.Set("Authorization", "Bearer "+keyResp.APIKey.Key)
	okModelRec := httptest.NewRecorder()
	handler.ServeHTTP(okModelRec, okModelReq)
	if okModelRec.Code != http.StatusOK {
		t.Fatalf("有效 vKey 应通过网关鉴权，实际: %d %s", okModelRec.Code, okModelRec.Body.String())
	}

	// 5. 修改面板密码后旧会话应立即失效，需重新登录
	chgReq := httptest.NewRequest(http.MethodPost, "/api/admin/auth/password", bytes.NewBufferString(`{"old_password":"my-panel-pass","new_password":"new-panel-pass"}`))
	chgReq.Header.Set("X-Panel-Session", setupResp.Session)
	chgRec := httptest.NewRecorder()
	handler.ServeHTTP(chgRec, chgReq)
	if chgRec.Code != http.StatusOK {
		t.Fatalf("修改面板密码失败: %d %s", chgRec.Code, chgRec.Body.String())
	}

	oldSessReq := httptest.NewRequest(http.MethodGet, "/api/admin/overview", nil)
	oldSessReq.Header.Set("X-Panel-Session", setupResp.Session)
	oldSessRec := httptest.NewRecorder()
	handler.ServeHTTP(oldSessRec, oldSessReq)
	if oldSessRec.Code != http.StatusUnauthorized {
		t.Fatalf("改密后旧面板会话应立即失效，实际: %d", oldSessRec.Code)
	}
}

func TestGatewayAuthFailoverAndPerAccountProxy(t *testing.T) {
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.PathChat {
			http.NotFound(w, r)
			return
		}
		hit := upstreamHits.Add(1)
		if hit == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit"}`))
			return
		}
		var reqMap map[string]any
		_ = json.NewDecoder(r.Body).Decode(&reqMap)
		if stream, _ := reqMap["stream"].(bool); stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"id\":\"cmpl-s1\",\"model\":\"glm-5.3-flash\",\"choices\":[{\"delta\":{\"content\":\"你好世界\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"id\":\"cmpl-s1\",\"model\":\"glm-5.3-flash\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":4,\"total_tokens\":12}}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-1","object":"chat.completion","model":"glm-5.3-flash","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"网关回复正常"}}],"usage":{"prompt_tokens":10,"completion_tokens":6,"total_tokens":16}}`))
	}))
	defer upstream.Close()

	// 伪 HTTP 代理服务器：用于验证绑定了固定节点的账号确实走代理出站
	var proxyHits atomic.Int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-px","object":"chat.completion","model":"glm-5.3-flash","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"经由专属代理回复"}}],"usage":{"prompt_tokens":5,"completion_tokens":5,"total_tokens":10}}`))
	}))
	defer proxySrv.Close()

	_, handler, proxyMgr, cfgMgr := setupTestServer(t, upstream.URL)
	vkey := config.GenerateVKey()
	_, _ = cfgMgr.Update(func(c *config.Config) error {
		c.APIKeys = []config.APIKeyItem{{ID: "k1", Name: "主密钥", Key: vkey, Enabled: true}}
		return nil
	})

	// 1. Bearer 鉴权 + /v1/chat/completions 自动切号验证
	chatReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"测试"}]}`))
	chatReq.Header.Set("Authorization", "Bearer "+vkey)
	chatRec := httptest.NewRecorder()
	handler.ServeHTTP(chatRec, chatReq)
	if chatRec.Code != http.StatusOK || !strings.Contains(chatRec.Body.String(), "网关回复正常") {
		t.Fatalf("故障切号后应成功响应，实际 status=%d body=%s", chatRec.Code, chatRec.Body.String())
	}
	if upstreamHits.Load() != 2 {
		t.Fatalf("预期切号重试共命中上游 2 次，实际: %d", upstreamHits.Load())
	}

	// 2. x-api-key 鉴权 + /v1/messages 流式转换验证
	anthReq := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(`{"model":"glm-5.3-flash","max_tokens":1024,"stream":true,"messages":[{"role":"user","content":"你好"}]}`))
	anthReq.Header.Set("x-api-key", vkey)
	anthRec := httptest.NewRecorder()
	handler.ServeHTTP(anthRec, anthReq)
	if anthRec.Code != http.StatusOK || !strings.Contains(anthRec.Body.String(), "event: message_start") || !strings.Contains(anthRec.Body.String(), "你好世界") {
		t.Fatalf("Anthropic 流式接口异常: status=%d body=%s", anthRec.Code, anthRec.Body.String())
	}

	// 3. 将 a2 绑定到固定 HTTP 代理节点，发起一次请求确认流量确实通过专属代理服务器发出
	imp, err := proxyMgr.ImportBulk(t.Context(), proxySrv.URL+"#专属代理节点")
	if err != nil || imp.Added != 1 {
		t.Fatalf("导入专属代理失败: %+v err=%v", imp, err)
	}
	nodeID := imp.Items[0].ID
	if err := proxyMgr.SetAccountBinding("autoclawcn-a2", proxypool.Binding{Mode: proxypool.ModeFixed, ProxyID: nodeID}); err != nil {
		t.Fatalf("绑定账号专属代理失败: %v", err)
	}
	pxReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{"model":"glm-5.3-flash","messages":[{"role":"user","content":"走代理"}]}`))
	pxReq.Header.Set("Authorization", "Bearer "+vkey)
	pxRec := httptest.NewRecorder()
	handler.ServeHTTP(pxRec, pxReq)
	if pxRec.Code != http.StatusOK || !strings.Contains(pxRec.Body.String(), "经由专属代理回复") || proxyHits.Load() < 1 {
		t.Fatalf("账号绑定固定代理后未命中专属代理服务器: status=%d proxyHits=%d body=%s", pxRec.Code, proxyHits.Load(), pxRec.Body.String())
	}
}

func TestStickySessionAcrossAllThreeEndpointsAndStreamIncludeUsage(t *testing.T) {
	var seenAuths []string
	var seenStreamUsage atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.PathChat {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		seenAuths = append(seenAuths, r.Header.Get("Authorization"))
		if isStream, _ := body["stream"].(bool); isStream {
			if so, ok := body["stream_options"].(map[string]any); ok && so["include_usage"] == true {
				seenStreamUsage.Store(true)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"id\":\"s1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"id\":\"s1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","model":"glm-5","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`))
	}))
	defer upstream.Close()

	_, handler, _, cfgMgr := setupTestServer(t, upstream.URL)
	vkey := config.GenerateVKey()
	_, _ = cfgMgr.Update(func(c *config.Config) error {
		c.APIKeys = []config.APIKeyItem{{ID: "k1", Name: "主密钥", Key: vkey, Enabled: true}}
		return nil
	})

	// 1. /v1/chat/completions 多轮相同前缀应稳定粘在同一账号
	for _, msg2 := range []string{"第二轮追问", "第三轮追问"} {
		body := `{"model":"glm-5","messages":[{"role":"system","content":"你是架构师"},{"role":"user","content":"第一轮问题"},{"role":"assistant","content":"好"},{"role":"user","content":"` + msg2 + `"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+vkey)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("chat 请求失败: %d %s", rec.Code, rec.Body.String())
		}
	}
	if len(seenAuths) != 2 || seenAuths[0] != seenAuths[1] {
		t.Fatalf("多轮同前缀 /v1/chat/completions 未保持粘性: %+v", seenAuths)
	}

	// 2. /v1/responses 携带 prompt_cache_key（流式）连续两次应粘在同一账号，并自动携带 stream_options.include_usage=true
	seenAuths = nil
	for i := 0; i < 2; i++ {
		body := `{"model":"glm-5","stream":true,"prompt_cache_key":"resp-cache-001","input":"hello"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+vkey)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("responses 流式请求失败: %d %s", rec.Code, rec.Body.String())
		}
	}
	if len(seenAuths) != 2 || seenAuths[0] != seenAuths[1] {
		t.Fatalf("/v1/responses 显式 prompt_cache_key 未保持粘性: %+v", seenAuths)
	}
	if !seenStreamUsage.Load() {
		t.Fatalf("流式请求未自动补齐 stream_options.include_usage=true")
	}

	// 3. /v1/messages 携带 metadata.user_id 连续两次应粘在同一账号
	seenAuths = nil
	for i := 0; i < 2; i++ {
		body := `{"model":"glm-5","max_tokens":512,"metadata":{"user_id":"claude-sess-99"},"messages":[{"role":"user","content":"hi"}]}`
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewBufferString(body))
		req.Header.Set("x-api-key", vkey)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("anthropic 请求失败: %d %s", rec.Code, rec.Body.String())
		}
	}
	if len(seenAuths) != 2 || seenAuths[0] != seenAuths[1] {
		t.Fatalf("/v1/messages metadata.user_id 未保持粘性: %+v", seenAuths)
	}
}

func TestNonStreamKeepaliveWhitespace(t *testing.T) {
	// 缩短心跳参数便于测试（包级变量，defer 恢复）
	oldGrace, oldInterval := keepaliveGrace, keepaliveInterval
	keepaliveGrace = 150 * time.Millisecond
	keepaliveInterval = 150 * time.Millisecond
	defer func() {
		keepaliveGrace, keepaliveInterval = oldGrace, oldInterval
	}()

	// 1. 长生成（超过宽限期）：响应体应以空白心跳开头，且整体仍是可解析的合法 JSON
	slowUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(700 * time.Millisecond) // 期间应触发多次心跳
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-1","object":"chat.completion","model":"glm-5.3-flash","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"慢速回复"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer slowUpstream.Close()

	_, handlerSlow, _, cfgSlow := setupTestServer(t, slowUpstream.URL)
	vkey := config.GenerateVKey()
	if _, err := cfgSlow.Update(func(c *config.Config) error {
		c.APIKeys = []config.APIKeyItem{{ID: "k1", Name: "主密钥", Key: vkey, Enabled: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{"model":"glm-5.3-flash","stream":false,"messages":[{"role":"user","content":"慢请求"}]}`))
	req.Header.Set("Authorization", "Bearer "+vkey)
	rec := httptest.NewRecorder()
	handlerSlow.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.HasPrefix(body, "  ") {
		t.Fatalf("超过宽限期的长生成应包含空白心跳前缀，实际: %q", body)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("心跳空白 + JSON 应可整体解析: %v", err)
	}
	if !strings.Contains(body, "慢速回复") {
		t.Fatalf("心跳不应破坏最终 JSON 内容: %s", body)
	}

	// 2. 快速路径（宽限期内完成）：不应有任何心跳前缀，响应为纯 JSON
	fastUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"cmpl-2","object":"chat.completion","model":"glm-5.3-flash","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"快速回复"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer fastUpstream.Close()

	_, handlerFast, _, cfgFast := setupTestServer(t, fastUpstream.URL)
	if _, err := cfgFast.Update(func(c *config.Config) error {
		c.APIKeys = []config.APIKeyItem{{ID: "k1", Name: "主密钥", Key: vkey, Enabled: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fastReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{"model":"glm-5.3-flash","stream":false,"messages":[{"role":"user","content":"快请求"}]}`))
	fastReq.Header.Set("Authorization", "Bearer "+vkey)
	fastRec := httptest.NewRecorder()
	handlerFast.ServeHTTP(fastRec, fastReq)
	fastBody := fastRec.Body.String()
	if strings.HasPrefix(fastBody, " ") {
		t.Fatalf("宽限期内完成的请求不应有心跳前缀: %q", fastBody)
	}
	if !strings.HasPrefix(fastBody, "{") || !strings.Contains(fastBody, "快速回复") {
		t.Fatalf("快速路径应返回纯 JSON: %s", fastBody)
	}
}
