package scheduler

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/proxypool"
)

// makeJWT 构造带 iat/exp 的伪 JWT（本程序只解析载荷字段，不校验签名）。
func makeJWT(t *testing.T, iat, exp int64) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, err := json.Marshal(map[string]any{"iat": iat, "exp": exp, "user_id": "u1"})
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestTokenRefreshThresholdByTTL(t *testing.T) {
	now := time.Now().Unix()
	engine := &Engine{}

	// 短效令牌（国际版 24h）：ttl/6 = 4h，取 max(2h, 4h) = 4h
	short := &accounts.Account{
		Cred: accounts.Credential{
			AccessToken: makeJWT(t, now-3600, now+82800),
			ExpiresAt:   now + 82800,
		},
	}
	if got := engine.tokenRefreshThreshold(short); got != 4*3600 {
		t.Fatalf("24h 令牌应提前 4 小时续期，实际 %d 秒", got)
	}

	// 长效令牌（国内版 28 天）：沿用 72h 保守窗口
	long := &accounts.Account{
		Cred: accounts.Credential{
			AccessToken: makeJWT(t, now-3600, now+86400*28-3600),
			ExpiresAt:   now + 86400*28 - 3600,
		},
	}
	if got := engine.tokenRefreshThreshold(long); got != int64((3*24*time.Hour).Seconds()) {
		t.Fatalf("28 天令牌应沿用 72h 窗口，实际 %d 秒", got)
	}

	// 无法解析 iat 时回落 72h
	fallback := &accounts.Account{Cred: accounts.Credential{AccessToken: "not-a-jwt", ExpiresAt: now + 3600}}
	if got := engine.tokenRefreshThreshold(fallback); got != int64((3*24*time.Hour).Seconds()) {
		t.Fatalf("无法解析 TTL 应回落 72h，实际 %d 秒", got)
	}
}

func TestLoadHistoryRestoresLastAutoDay(t *testing.T) {
	dir := t.TempDir()
	mgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Update(func(c *config.Config) error {
		c.DataDir = dir
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pool, err := accounts.NewPool(mgr, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyMgr, err := proxypool.NewManager(filepath.Join(dir, "proxies"), config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := accounts.NewRouter(mgr, pool, proxyMgr, nil)

	today := time.Now().Format("2006-01-02")
	history := `[{"time":"` + today + ` 08:30:01","date":"` + today + `","account_id":"a1","status":"done","points":5,"message":"ok","source":"auto"}]`
	if err := os.WriteFile(filepath.Join(dir, "checkin_history.json"), []byte(history), 0o600); err != nil {
		t.Fatal(err)
	}

	engine := New(mgr, pool, router, proxyMgr, nil)
	engine.mu.RLock()
	got := engine.lastAutoDay
	engine.mu.RUnlock()
	if got != today {
		t.Fatalf("启动应从历史恢复 lastAutoDay=%s，实际 %q", today, got)
	}
	if strings.TrimSpace(got) == "" {
		t.Fatal("lastAutoDay 不应为空")
	}
}
