package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

func makeTestJWT(iat, exp int64) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body, _ := json.Marshal(map[string]any{"iat": iat, "exp": exp, "sub": "test"})
	return hdr + "." + base64.RawURLEncoding.EncodeToString(body) + ".sig"
}

func TestDynamicRefreshThresholdAndBackoff(t *testing.T) {
	now := time.Now().Unix()

	// 1. 国际版 24h JWT（剩 20h 不应刷新；剩 3h 应刷新）
	jwt24h := makeTestJWT(now-4*3600, now+20*3600)
	acc24h := accounts.Account{
		ID: "autoclawglobal-u1",
		Cred: accounts.Credential{
			Region:       protocol.RegionGlobal,
			AccessToken:  jwt24h,
			RefreshToken: "rt-1",
			ExpiresAt:    now + 20*3600,
		},
	}
	th24 := calcRefreshThresholdSeconds(acc24h)
	if th24 != 4*3600 {
		t.Fatalf("24h 令牌续期窗口应为 4 小时(14400s)，实际: %d", th24)
	}
	if acc24h.Cred.ExpiresAt-now <= th24 {
		t.Fatalf("剩余 20h 的国际版 24h 令牌不应触发续期")
	}

	// 2. 国内版 28 天 JWT 保持提前 3 天（72h）续期
	jwt28d := makeTestJWT(now-10*86400, now+18*86400)
	acc28d := accounts.Account{
		ID: "autoclawcn-u1",
		Cred: accounts.Credential{
			Region:      protocol.RegionCN,
			AccessToken: jwt28d,
			ExpiresAt:   now + 18*86400,
		},
	}
	if th28 := calcRefreshThresholdSeconds(acc28d); th28 != 3*86400 {
		t.Fatalf("28 天长效令牌续期窗口应为 72 小时，实际: %d", th28)
	}

	// 3. 指数退避曲线校验（15m -> 30m -> 1h -> ... -> 12h 上限）
	if calcRefreshBackoffDelay(1) != 15*time.Minute ||
		calcRefreshBackoffDelay(2) != 30*time.Minute ||
		calcRefreshBackoffDelay(3) != time.Hour ||
		calcRefreshBackoffDelay(10) != 12*time.Hour {
		t.Fatalf("续期失败指数退避曲线异常")
	}

	// 4. 实测 refreshExpiringTokens：20h 剩余不刷，临近过期刷新失败后立即进入退避不再死循环
	var refreshCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == protocol.PathRefreshToken {
			refreshCalls.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream fail"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	_ = os.MkdirAll(authDir, 0o755)
	// 号 A：国际版 24h JWT，还剩 20h（不应被刷）
	_ = os.WriteFile(filepath.Join(authDir, "autoclawglobal-fresh.json"), mustJSON(t, map[string]any{
		"region": "global", "accessToken": jwt24h, "refreshToken": "rt-fresh", "expiresAt": now + 20*3600,
	}), 0o600)
	// 号 B：国际版 24h JWT，仅剩 2h（应尝试刷新，失败后触发退避）
	jwtSoon := makeTestJWT(now-22*3600, now+2*3600)
	_ = os.WriteFile(filepath.Join(authDir, "autoclawglobal-soon.json"), mustJSON(t, map[string]any{
		"region": "global", "accessToken": jwtSoon, "refreshToken": "rt-soon", "expiresAt": now + 2*3600,
	}), 0o600)

	cfgMgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.Upstream.GlobalBaseURL = srv.URL
		return nil
	})
	pool, err := accounts.NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyMgr, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := accounts.NewRouter(cfgMgr, pool, proxyMgr, nil)
	eng := New(cfgMgr, pool, router, proxyMgr, nil)

	eng.refreshExpiringTokens(context.Background())
	if refreshCalls.Load() != 1 {
		t.Fatalf("首轮应仅刷新即将过期的 1 个账号（剩 20h 的号不刷），实际请求次数: %d", refreshCalls.Load())
	}
	// 立即再次触发：处于退避期，不应再发请求
	eng.refreshExpiringTokens(context.Background())
	if refreshCalls.Load() != 1 {
		t.Fatalf("失败退避期内不应重复刷新，实际请求次数: %d", refreshCalls.Load())
	}
}

func TestRestoreLastAutoDayAndSkipDoneAndBalanceConcurrency(t *testing.T) {
	var checkinHits atomic.Int32
	var activeBal, maxBal atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case protocol.PathDailyCheckin:
			checkinHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"points":10}}`))
		case protocol.PathUserInfo:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok"}`))
		case "/agent-assetmgr/api/v2/wallets":
			cur := activeBal.Add(1)
			for {
				prev := maxBal.Load()
				if cur <= prev || maxBal.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
			activeBal.Add(-1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"wallets":[{"balance":123}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	_ = os.MkdirAll(authDir, 0o755)
	_ = os.MkdirAll(dataDir, 0o755)
	for _, uid := range []string{"u1", "u2", "u3", "u4"} {
		_ = os.WriteFile(filepath.Join(authDir, "autoclawcn-"+uid+".json"), mustJSON(t, map[string]any{
			"region": "cn", "accessToken": "tok-" + uid, "userId": uid,
		}), 0o600)
	}

	// 预先写入今日已自动签到的历史记录（模拟 08:30 后重启服务）
	today := time.Now().Format("2006-01-02")
	hist := []CheckinRecord{
		{Date: today, AccountID: "autoclawcn-u1", Status: "done", Source: "auto", Message: "签到成功"},
		{Date: today, AccountID: "autoclawcn-u2", Status: "already", Source: "manual", Message: "今日已签"},
	}
	_ = os.WriteFile(filepath.Join(dataDir, "checkin_history.json"), mustJSON(t, hist), 0o600)

	cfgMgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.Upstream.CNBaseURL = srv.URL
		c.Checkin.Enabled = true
		c.Checkin.Time = "00:00"
		return nil
	})
	pool, err := accounts.NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyMgr, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := accounts.NewRouter(cfgMgr, pool, proxyMgr, nil)
	eng := New(cfgMgr, pool, router, proxyMgr, nil)
	eng.checkinStagger = func() time.Duration { return 5 * time.Millisecond }

	// 1. 重启后应从历史恢复 lastAutoDay == today
	eng.mu.RLock()
	restoredDay := eng.lastAutoDay
	eng.mu.RUnlock()
	if restoredDay != today {
		t.Fatalf("启动加载今日 auto 签到历史后应恢复 lastAutoDay=%s，实际=%s", today, restoredDay)
	}

	// 2. CheckinAll("auto") 应跳过已 done 的 u1 与已 already 的 u2，仅签到 u3、u4
	recs := eng.CheckinAll(context.Background(), "auto")
	if len(recs) != 2 || checkinHits.Load() != 2 {
		t.Fatalf("自动签到应跳过当日已签账号(u1/u2)，实际执行 %d 次 (hits=%d)", len(recs), checkinHits.Load())
	}

	// 3. RefreshAllBalances 并发上限应收敛至 2
	okCnt := eng.RefreshAllBalances(context.Background())
	if okCnt != 4 {
		t.Fatalf("期望 4 个账号查余额均成功，实际: %d", okCnt)
	}
	if maxBal.Load() > balanceRefreshConc {
		t.Fatalf("查余额并发度超过上限 %d，实际峰值: %d", balanceRefreshConc, maxBal.Load())
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
