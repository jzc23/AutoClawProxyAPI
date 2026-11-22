package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/config"
)

const (
	panelSessionCookie = "ac2a_panel_session"
	panelSessionTTL    = 7 * 24 * time.Hour

	// 面板登录防爆破：连续失败 5 次锁定 5 分钟（内存态，重启即清零）。
	panelLoginMaxFails   = 5
	panelLoginLockWindow = 5 * time.Minute
)

type panelSession struct {
	ExpiresAt time.Time
}

// panelLoginGuard 内存态登录失败计数器与临时锁定。
type panelLoginGuard struct {
	mu          sync.Mutex
	fails       int
	lockedUntil time.Time
}

func (g *panelLoginGuard) allow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().After(g.lockedUntil)
}

// recordFail 记录一次失败；达到阈值时进入锁定并返回锁定时长（0 表示未触发锁定）。
func (g *panelLoginGuard) recordFail() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fails++
	if g.fails >= panelLoginMaxFails {
		g.lockedUntil = time.Now().Add(panelLoginLockWindow)
		g.fails = 0
		return panelLoginLockWindow
	}
	return 0
}

func (g *panelLoginGuard) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fails = 0
	g.lockedUntil = time.Time{}
}

func (s *Server) newPanelSession() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	token := hex.EncodeToString(b)

	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	now := time.Now()
	for k, v := range s.panelSessions {
		if now.After(v.ExpiresAt) {
			delete(s.panelSessions, k)
		}
	}
	s.panelSessions[token] = panelSession{ExpiresAt: now.Add(panelSessionTTL)}
	return token
}

func (s *Server) isPanelAuthenticated(r *http.Request) bool {
	token := s.extractPanelSessionToken(r)
	if token == "" {
		return false
	}
	s.sessionMu.RLock()
	sess, ok := s.panelSessions[token]
	s.sessionMu.RUnlock()
	if !ok {
		return false
	}
	if time.Now().After(sess.ExpiresAt) {
		s.sessionMu.Lock()
		delete(s.panelSessions, token)
		s.sessionMu.Unlock()
		return false
	}
	return true
}

func (s *Server) extractPanelSessionToken(r *http.Request) string {
	if c, err := r.Cookie(panelSessionCookie); err == nil && strings.TrimSpace(c.Value) != "" {
		return strings.TrimSpace(c.Value)
	}
	if h := strings.TrimSpace(r.Header.Get("X-Panel-Session")); h != "" {
		return h
	}
	return ""
}

func setPanelSessionCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     panelSessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(panelSessionTTL.Seconds()),
	})
}

func clearPanelSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     panelSessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

// withPanelAuth 保护所有管理控制台接口：未初始化密码时提示首次引导，未登录时要求输入面板密码。
func (s *Server) withPanelAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg := s.cfgMgr.Get()
		if !cfg.PanelAuth.Initialized() {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"code":  "panel_setup_required",
				"error": "尚未设置面板登录密码，请先完成首次初始化向导",
			})
			return
		}
		if !s.isPanelAuthenticated(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"code":  "panel_login_required",
				"error": "请先使用面板密码登录控制台",
			})
			return
		}
		next(w, r)
	}
}

func (s *Server) handlePanelAuthStatus(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	initialized := cfg.PanelAuth.Initialized()
	authed := initialized && s.isPanelAuthenticated(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"initialized":   initialized,
		"authenticated": authed,
		"version":       config.Version,
	})
}

func (s *Server) handlePanelAuthSetup(w http.ResponseWriter, r *http.Request) {
	if s.cfgMgr.Get().PanelAuth.Initialized() {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "面板密码已初始化，请直接登录"})
		return
	}
	var body struct {
		Password        string `json:"password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	if body.ConfirmPassword != "" && body.Password != body.ConfirmPassword {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "两次输入的面板密码不一致"})
		return
	}
	hash, err := config.HashPanelPassword(body.Password)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	_, err = s.cfgMgr.Update(func(c *config.Config) error {
		if c.PanelAuth.Initialized() {
			return fmt.Errorf("面板密码已存在")
		}
		c.PanelAuth.PasswordHash = hash
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	token := s.newPanelSession()
	setPanelSessionCookie(w, token)
	if s.logger != nil {
		s.logger.Info("Auth", "面板登录密码已完成首次初始化设置")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "面板密码初始化成功，已自动登录",
		"session": token,
	})
}

func (s *Server) handlePanelAuthLogin(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !cfg.PanelAuth.Initialized() {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"code":  "panel_setup_required",
			"error": "尚未初始化面板密码，请先设置密码",
		})
		return
	}
	var body struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	if !s.loginGuard.allow() {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error": fmt.Sprintf("面板密码连续错误次数过多，已临时锁定，请 %.0f 分钟后再试", panelLoginLockWindow.Minutes()),
		})
		return
	}
	if !config.VerifyPanelPassword(body.Password, cfg.PanelAuth.PasswordHash) {
		if lockFor := s.loginGuard.recordFail(); lockFor > 0 {
			if s.logger != nil {
				s.logger.Warn("Auth", "面板密码连续 %d 次校验失败，已临时锁定 %.0f 分钟", panelLoginMaxFails, lockFor.Minutes())
			}
		} else if s.logger != nil {
			s.logger.Warn("Auth", "面板密码校验失败（来自 %s）", r.RemoteAddr)
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "面板密码不正确，请重试"})
		return
	}
	s.loginGuard.reset()

	// 旧版单轮 SHA-256 哈希在登录成功后透明升级为 PBKDF2，无需用户操作
	if !config.IsPBKDF2Hash(cfg.PanelAuth.PasswordHash) {
		if newHash, err := config.HashPanelPassword(body.Password); err == nil {
			_, _ = s.cfgMgr.Update(func(c *config.Config) error {
				c.PanelAuth.PasswordHash = newHash
				return nil
			})
			if s.logger != nil {
				s.logger.Info("Auth", "面板密码存储已从单轮 SHA-256 自动升级为 PBKDF2-SHA256")
			}
		}
	}

	token := s.newPanelSession()
	setPanelSessionCookie(w, token)
	if s.logger != nil {
		s.logger.Info("Auth", "管理员已登录控制台")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "登录成功",
		"session": token,
	})
}

func (s *Server) handlePanelAuthLogout(w http.ResponseWriter, r *http.Request) {
	if token := s.extractPanelSessionToken(r); token != "" {
		s.sessionMu.Lock()
		delete(s.panelSessions, token)
		s.sessionMu.Unlock()
	}
	clearPanelSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"message": "已退出登录"})
}

func (s *Server) handlePanelAuthChangePassword(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	cfg := s.cfgMgr.Get()
	if !config.VerifyPanelPassword(body.OldPassword, cfg.PanelAuth.PasswordHash) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "当前面板密码输入不正确"})
		return
	}
	newHash, err := config.HashPanelPassword(body.NewPassword)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if _, err := s.cfgMgr.Update(func(c *config.Config) error {
		c.PanelAuth.PasswordHash = newHash
		return nil
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存新密码失败: " + err.Error()})
		return
	}

	// 失效所有旧会话并为当前连接签发新会话
	s.sessionMu.Lock()
	s.panelSessions = make(map[string]panelSession)
	s.sessionMu.Unlock()
	token := s.newPanelSession()
	setPanelSessionCookie(w, token)
	if s.logger != nil {
		s.logger.Info("Auth", "面板登录密码已修改，旧会话已全部失效")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "面板密码修改成功",
		"session": token,
	})
}

// ---- 独立 API Key 管理（一键随机生成 vKey-） ----

func (s *Server) handleAdminListAPIKeys(w http.ResponseWriter, _ *http.Request) {
	keys := s.cfgMgr.Get().APIKeys
	writeJSON(w, http.StatusOK, map[string]any{
		"api_keys": keys,
		"total":    len(keys),
	})
}

func (s *Server) handleAdminCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	name := strings.TrimSpace(body.Name)
	var created config.APIKeyItem
	_, err := s.cfgMgr.Update(func(c *config.Config) error {
		if name == "" {
			name = fmt.Sprintf("访问密钥-%d", len(c.APIKeys)+1)
		}
		created = config.APIKeyItem{
			ID:        config.GenerateItemID("key"),
			Name:      name,
			Key:       config.GenerateVKey(),
			Enabled:   true,
			CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
		}
		c.APIKeys = append([]config.APIKeyItem{created}, c.APIKeys...)
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "创建 API Key 失败: " + err.Error()})
		return
	}
	if s.logger != nil {
		s.logger.Info("Admin", "已生成新的网关 API Key「%s」(%s)", created.Name, created.Key[:10]+"...")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "已生成新的 vKey 访问密钥",
		"api_key": created,
	})
}

func (s *Server) handleAdminUpdateAPIKey(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	var body struct {
		Name    *string `json:"name"`
		Enabled *bool   `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}
	var found bool
	_, err := s.cfgMgr.Update(func(c *config.Config) error {
		for i := range c.APIKeys {
			if c.APIKeys[i].ID == id {
				if body.Name != nil && strings.TrimSpace(*body.Name) != "" {
					c.APIKeys[i].Name = strings.TrimSpace(*body.Name)
				}
				if body.Enabled != nil {
					c.APIKeys[i].Enabled = *body.Enabled
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("未找到指定的 API Key: %s", id)
		}
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": "API Key 状态已更新"})
}

func (s *Server) handleAdminDeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	var removed bool
	_, err := s.cfgMgr.Update(func(c *config.Config) error {
		next := make([]config.APIKeyItem, 0, len(c.APIKeys))
		for _, k := range c.APIKeys {
			if k.ID == id {
				removed = true
				continue
			}
			next = append(next, k)
		}
		if !removed {
			return fmt.Errorf("未找到指定的 API Key: %s", id)
		}
		c.APIKeys = next
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": "API Key 已删除"})
}
