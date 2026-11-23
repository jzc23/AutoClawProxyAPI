package gateway

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/mihomo"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
	"autoclaw2api/internal/scheduler"
)

func TestLoginAutoRotationReusesSessionNodeAndKeepsDailyDirect(t *testing.T) {
	// 两个独立模拟 mihomo 本地回环混合代理节点，分别统计命中路径
	var jpStart, jpCallback, jpSMSSend, jpSMSVerify atomic.Int32
	var sgStart, sgCallback, sgSMSSend, sgSMSVerify atomic.Int32

	makeProxy := func(tag string, cStart, cCb, cSend, cVerify *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case protocol.PathOAuthCaptchaConf:
				_, _ = w.Write([]byte(`{"code":0,"msg":"SUCCESS","data":{"enabled":true,"region":"ga","prefix":"p1","scene_id":"s1"}}`))
			case protocol.PathOAuthURLZai:
				cStart.Add(1)
				_, _ = w.Write([]byte(`{"code":0,"msg":"SUCCESS","data":{"oauth_url":"https://auth.example/zai?via=` + tag + `"}}`))
			case protocol.PathOAuthLoginZai:
				cCb.Add(1)
				_, _ = w.Write([]byte(`{"code":0,"msg":"SUCCESS","data":{"access_token":"tok-gl-` + tag + `","refresh_token":"ref-` + tag + `","user_id":"uid-gl-` + tag + `","user_name":"user-` + tag + `"}}`))
			case protocol.PathSMSSendCode:
				cSend.Add(1)
				_, _ = w.Write([]byte(`{"code":0,"msg":"SUCCESS","data":{"result":true}}`))
			case protocol.PathSMSLogin:
				cVerify.Add(1)
				_, _ = w.Write([]byte(`{"code":0,"msg":"SUCCESS","data":{"access_token":"tok-cn-` + tag + `","refresh_token":"ref-cn-` + tag + `","user_id":"uid-cn-` + tag + `","user_name":"cn-` + tag + `"}}`))
			default:
				_, _ = w.Write([]byte(`{"code":0,"data":{"total_credits":1000}}`))
			}
		}))
	}

	jpSrv := makeProxy("jp", &jpStart, &jpCallback, &jpSMSSend, &jpSMSVerify)
	defer jpSrv.Close()
	sgSrv := makeProxy("sg", &sgStart, &sgCallback, &sgSMSSend, &sgSMSVerify)
	defer sgSrv.Close()

	jpPort := jpSrv.Listener.Addr().(*net.TCPAddr).Port
	sgPort := sgSrv.Listener.Addr().(*net.TCPAddr).Port

	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	_ = os.MkdirAll(authDir, 0o755)
	_ = os.MkdirAll(dataDir, 0o755)

	cfgMgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	passHash, err := config.HashPanelPassword("testpass123")
	if err != nil {
		t.Fatalf("HashPanelPassword: %v", err)
	}
	_, err = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.PanelAuth.PasswordHash = passHash
		c.Upstream.CNBaseURL = "http://cn-upstream.invalid"
		c.Upstream.GlobalBaseURL = "http://gl-upstream.invalid"
		return nil
	})
	if err != nil {
		t.Fatalf("cfg update: %v", err)
	}

	proxyMgr, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatalf("proxypool.NewManager: %v", err)
	}
	mhMgr, err := mihomo.NewManager(dir, dataDir, nil)
	if err != nil {
		t.Fatalf("mihomo.NewManager: %v", err)
	}
	mhMgr.SetNodesForTest(true, []mihomo.SubNode{
		{Name: "Node-JP", Port: jpPort, DelayMS: 35},
		{Name: "Node-SG", Port: sgPort, DelayMS: 60},
		{Name: "Node-Dead", Port: 29999, DelayMS: -1},
	})
	proxyMgr.SetMihomoResolver(mhMgr)

	pool, err := accounts.NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	router := accounts.NewRouter(cfgMgr, pool, proxyMgr, nil)
	sched := scheduler.New(cfgMgr, pool, router, proxyMgr, nil)
	srv := NewServer(cfgMgr, pool, router, proxyMgr, sched, nil)
	srv.SetMihomoManager(mhMgr)
	h := srv.Handler()

	// 登录管理面板获取会话 Cookie
	loginRec := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/api/admin/auth/login", bytes.NewBufferString(`{"password":"testpass123"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("面板登录失败: %d %s", loginRec.Code, loginRec.Body.String())
	}
	cookies := loginRec.Result().Cookies()

	doJSON := func(method, path string, body any) *httptest.ResponseRecorder {
		var rdr *bytes.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rdr = bytes.NewReader(b)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req := httptest.NewRequest(method, path, rdr)
		req.Header.Set("Content-Type", "application/json")
		for _, c := range cookies {
			req.AddCookie(c)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// === 国际版 OAuth 会话 #1：captcha 不推进游标，start 锁定 Node-JP，callback 严格复用 Node-JP ===
	recCap := doJSON(http.MethodGet, "/api/admin/accounts/oauth/captcha?exit_mode=auto", nil)
	if recCap.Code != http.StatusOK {
		t.Fatalf("captcha 失败: %d %s", recCap.Code, recCap.Body.String())
	}

	recStart1 := doJSON(http.MethodPost, "/api/admin/accounts/oauth/start", map[string]any{
		"provider":                 "zai",
		"ali_captcha_verify_param": "ok-param",
		"exit_mode":                "auto",
	})
	if recStart1.Code != http.StatusOK {
		t.Fatalf("oauth/start #1 失败: %d %s", recStart1.Code, recStart1.Body.String())
	}
	var start1 struct {
		SessionID     string `json:"session_id"`
		UsedNode      string `json:"used_node"`
		NextLoginNode string `json:"next_login_node"`
	}
	_ = json.Unmarshal(recStart1.Body.Bytes(), &start1)
	if start1.UsedNode != "Node-JP" || start1.NextLoginNode != "Node-SG" {
		t.Fatalf("OAuth #1 期望锁定 Node-JP 且下一节点为 Node-SG，实际: %+v", start1)
	}

	recCb1 := doJSON(http.MethodPost, "/api/admin/accounts/oauth/callback", map[string]any{
		"session_id":   start1.SessionID,
		"callback_url": "http://localhost:53699/auth/callback-zai?code=c1&state=s1",
		"alias":        "国际1号",
		"exit_mode":    "auto",
	})
	if recCb1.Code != http.StatusOK {
		t.Fatalf("oauth/callback #1 失败: %d %s", recCb1.Code, recCb1.Body.String())
	}
	var cb1 struct {
		UsedNode string               `json:"used_node"`
		Account  accounts.AccountView `json:"account"`
	}
	_ = json.Unmarshal(recCb1.Body.Bytes(), &cb1)
	if cb1.UsedNode != "Node-JP" {
		t.Fatalf("OAuth #1 callback 必须复用 Node-JP，实际: %q", cb1.UsedNode)
	}
	if cb1.Account.ProxyRoute == nil || cb1.Account.ProxyRoute.EffectiveMode != proxypool.ModeDirect {
		t.Fatalf("OAuth #1 入池新账号必须保持默认直连，绝不强绑 mihomo: %+v", cb1.Account.ProxyRoute)
	}

	// === 国际版 OAuth 会话 #2：自动轮换到 Node-SG，callback 复用 Node-SG，入池保持直连 ===
	recStart2 := doJSON(http.MethodPost, "/api/admin/accounts/oauth/start", map[string]any{
		"provider":                 "zai",
		"ali_captcha_verify_param": "ok-param",
		"exit_mode":                "auto",
	})
	if recStart2.Code != http.StatusOK {
		t.Fatalf("oauth/start #2 失败: %d %s", recStart2.Code, recStart2.Body.String())
	}
	var start2 struct {
		SessionID string `json:"session_id"`
		UsedNode  string `json:"used_node"`
	}
	_ = json.Unmarshal(recStart2.Body.Bytes(), &start2)
	if start2.UsedNode != "Node-SG" {
		t.Fatalf("OAuth #2 应自动轮换至 Node-SG，实际: %+v", start2)
	}
	recCb2 := doJSON(http.MethodPost, "/api/admin/accounts/oauth/callback", map[string]any{
		"session_id":   start2.SessionID,
		"callback_url": "http://localhost:53699/auth/callback-zai?code=c2&state=s2",
		"alias":        "国际2号",
		"exit_mode":    "auto",
	})
	if recCb2.Code != http.StatusOK {
		t.Fatalf("oauth/callback #2 失败: %d %s", recCb2.Code, recCb2.Body.String())
	}
	var cb2 struct {
		UsedNode string               `json:"used_node"`
		Account  accounts.AccountView `json:"account"`
	}
	_ = json.Unmarshal(recCb2.Body.Bytes(), &cb2)
	if cb2.UsedNode != "Node-SG" {
		t.Fatalf("OAuth #2 callback 必须复用 Node-SG，实际: %q", cb2.UsedNode)
	}
	if cb2.Account.ProxyRoute == nil || cb2.Account.ProxyRoute.EffectiveMode != proxypool.ModeDirect {
		t.Fatalf("OAuth #2 入池新账号必须保持默认直连: %+v", cb2.Account.ProxyRoute)
	}

	// === 国内短信登录 #1 & #2：分别轮换 Node-JP 与 Node-SG，send 与 verify 严格复用同节点，入池保持直连 ===
	recSend1 := doJSON(http.MethodPost, "/api/admin/accounts/sms/send", map[string]any{
		"phone":     "13800138001",
		"exit_mode": "auto",
	})
	if recSend1.Code != http.StatusOK {
		t.Fatalf("sms/send #1 失败: %d %s", recSend1.Code, recSend1.Body.String())
	}
	var send1 struct {
		DeviceID string `json:"device_id"`
		UsedNode string `json:"used_node"`
	}
	_ = json.Unmarshal(recSend1.Body.Bytes(), &send1)
	if send1.UsedNode != "Node-JP" {
		t.Fatalf("SMS #1 应轮换至 Node-JP，实际: %+v", send1)
	}
	recVer1 := doJSON(http.MethodPost, "/api/admin/accounts/sms/verify", map[string]any{
		"phone":     "13800138001",
		"code":      "123456",
		"device_id": send1.DeviceID,
		"alias":     "国内1号",
		"exit_mode": "auto",
	})
	if recVer1.Code != http.StatusOK {
		t.Fatalf("sms/verify #1 失败: %d %s", recVer1.Code, recVer1.Body.String())
	}
	var ver1 struct {
		UsedNode string               `json:"used_node"`
		Account  accounts.AccountView `json:"account"`
	}
	_ = json.Unmarshal(recVer1.Body.Bytes(), &ver1)
	if ver1.UsedNode != "Node-JP" {
		t.Fatalf("SMS #1 verify 必须复用 Node-JP，实际: %q", ver1.UsedNode)
	}
	if ver1.Account.ProxyRoute == nil || ver1.Account.ProxyRoute.EffectiveMode != proxypool.ModeDirect {
		t.Fatalf("SMS #1 入池新账号必须保持默认直连: %+v", ver1.Account.ProxyRoute)
	}

	recSend2 := doJSON(http.MethodPost, "/api/admin/accounts/sms/send", map[string]any{
		"phone":     "13800138002",
		"exit_mode": "auto",
	})
	if recSend2.Code != http.StatusOK {
		t.Fatalf("sms/send #2 失败: %d %s", recSend2.Code, recSend2.Body.String())
	}
	var send2 struct {
		DeviceID string `json:"device_id"`
		UsedNode string `json:"used_node"`
	}
	_ = json.Unmarshal(recSend2.Body.Bytes(), &send2)
	if send2.UsedNode != "Node-SG" {
		t.Fatalf("SMS #2 应轮换至 Node-SG，实际: %+v", send2)
	}
	recVer2 := doJSON(http.MethodPost, "/api/admin/accounts/sms/verify", map[string]any{
		"phone":     "13800138002",
		"code":      "654321",
		"device_id": send2.DeviceID,
		"alias":     "国内2号",
		"exit_mode": "auto",
	})
	if recVer2.Code != http.StatusOK {
		t.Fatalf("sms/verify #2 失败: %d %s", recVer2.Code, recVer2.Body.String())
	}

	// 核对真实代理流量命中计数：每个节点各承接 1 次完整 OAuth + 1 次完整 SMS
	if jpStart.Load() != 1 || jpCallback.Load() != 1 || sgStart.Load() != 1 || sgCallback.Load() != 1 {
		t.Fatalf("OAuth 节点命中不符: JP(start=%d,cb=%d) SG(start=%d,cb=%d)",
			jpStart.Load(), jpCallback.Load(), sgStart.Load(), sgCallback.Load())
	}
	if jpSMSSend.Load() != 1 || jpSMSVerify.Load() != 1 || sgSMSSend.Load() != 1 || sgSMSVerify.Load() != 1 {
		t.Fatalf("SMS 节点命中不符: JP(send=%d,verify=%d) SG(send=%d,verify=%d)",
			jpSMSSend.Load(), jpSMSVerify.Load(), sgSMSSend.Load(), sgSMSVerify.Load())
	}
}
