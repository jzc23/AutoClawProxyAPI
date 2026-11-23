package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

func (s *Server) handleAdminOverview(w http.ResponseWriter, _ *http.Request) {
	cfg := s.cfgMgr.Get()
	stats := s.pool.Stats()
	models := s.router.ListOpenAIModels()
	proxySnap := s.proxyMgr.Snapshot()

	healthyNodes := 0
	for _, n := range proxySnap.Nodes {
		if n.Enabled && n.Status == "healthy" {
			healthyNodes++
		}
	}

	var mihomoStats map[string]any
	if s.mihomoMgr != nil {
		ms := s.mihomoMgr.Snapshot()
		mihomoStats = map[string]any{
			"binary_found":    ms.BinaryFound,
			"running":         ms.Running,
			"enabled":         ms.Enabled,
			"total_nodes":     ms.TotalCount,
			"healthy_nodes":   ms.HealthyCount,
			"next_login_node": ms.NextLoginNode,
		}
	}
	recentBal := s.pool.BalanceHistory("", 12)
	recentSplit := s.pool.BalanceHistorySplit("", 50)

	writeJSON(w, http.StatusOK, map[string]any{
		"version":     config.Version,
		"author":      "VILK",
		"uptime_sec":  int64(time.Since(s.startedAt).Seconds()),
		"started_at":  s.startedAt.Format("2006-01-02 15:04:05"),
		"listen":      cfg.Listen,
		"port":        cfg.ListenPort(),
		"auth_dir":    cfg.AuthDir,
		"api_auth_on": len(cfg.APIKeys) > 0,
		"apikey_cnt":  len(cfg.APIKeys),
		"checkin":     cfg.Checkin,
		"proxy_stats": map[string]any{
			"total":   len(proxySnap.Nodes),
			"healthy": healthyNodes,
			"regions": proxySnap.Regions,
		},
		"mihomo_stats":           mihomoStats,
		"recent_balance_changes": recentBal,
		"balance_abnormal_count": recentSplit.AbnormalCount,
		"upstream": map[string]string{
			"cn_base_url":     protocol.ResolveBaseURL(protocol.RegionCN, cfg.Upstream.CNBaseURL),
			"global_base_url": protocol.ResolveBaseURL(protocol.RegionGlobal, cfg.Upstream.GlobalBaseURL),
		},
		"stats":       stats,
		"model_count": len(models),
	})
}

func (s *Server) handleAdminListAccounts(w http.ResponseWriter, _ *http.Request) {
	_ = s.pool.SyncFromDisk()
	views := s.pool.ListViews()
	for i := range views {
		route := s.proxyMgr.DescribeAccountRoute(views[i].ID, string(views[i].Region))
		views[i].ProxyRoute = &route
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts": views,
		"stats":    s.pool.Stats(),
	})
}

func (s *Server) handleAdminImportAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Region string `json:"region"`
		Alias  string `json:"alias"`
		Note   string `json:"note"`
		Raw    string `json:"raw"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败: " + err.Error()})
		return
	}
	rawBytes := []byte(strings.TrimSpace(body.Raw))
	if len(rawBytes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请粘贴凭证 JSON 内容"})
		return
	}

	hintName := ""
	if strings.TrimSpace(body.Region) != "" {
		hintName = protocol.NormalizeRegion(body.Region).Channel() + "-import.json"
	}
	cred, err := accounts.ParseCredentialJSON(rawBytes, hintName)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if strings.TrimSpace(body.Region) != "" {
		cred.Region = protocol.NormalizeRegion(body.Region)
	}
	if strings.TrimSpace(body.Alias) != "" {
		cred.Alias = strings.TrimSpace(body.Alias)
	}
	if strings.TrimSpace(body.Note) != "" {
		cred.Note = strings.TrimSpace(body.Note)
	}

	var rawMap map[string]any
	if json.Unmarshal(rawBytes, &rawMap) == nil {
		rawMap["region"] = string(cred.Region)
		rawMap["vendor"] = cred.Region.Channel()
		if cred.Alias != "" {
			rawMap["alias"] = cred.Alias
		}
		if cred.Note != "" {
			rawMap["note"] = cred.Note
		}
		if updated, errM := json.MarshalIndent(rawMap, "", "  "); errM == nil {
			rawBytes = updated
		}
	}

	view, err := s.pool.SaveCredential(cred, rawBytes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存凭证失败: " + err.Error()})
		return
	}
	if s.logger != nil {
		s.logger.Info("Admin", "已导入账号 [%s]（%s）", view.Label, view.Region)
	}
	go func(id string, region protocol.Region) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		_, _ = s.scheduler.RefreshAccountBalanceWithTrigger(ctx, id, "login")
		_, _ = s.router.SyncRegionModels(ctx, region)
	}(view.ID, view.Region)

	writeJSON(w, http.StatusOK, map[string]any{
		"message": "凭证导入成功",
		"account": view,
	})
}

func (s *Server) nextLoginNodePreview() string {
	if s.mihomoMgr == nil {
		return ""
	}
	if n, ok := s.mihomoMgr.PeekNextLoginNode(); ok {
		return n.Name
	}
	return ""
}

// resolveLoginExit 解析本次登录/换票所使用的出口客户端：
// - exitMode == "auto" 时，从 mihomo 登录节点桥自动轮换挑选下一个健康节点（advanceAuto=true 推进游标，false 仅预览复用）；
// - 若 mihomo 未启动或无可用节点，自动平滑回退直连；
// - exitMode == "mihomo" / "fixed" / "direct" / "region" 时按用户显式选择执行。
func (s *Server) resolveLoginExit(exitMode, proxyID, mihomoNode, fallbackRegion string, advanceAuto bool, timeout time.Duration) (
	client *http.Client,
	resolvedMode string,
	resolvedProxyID string,
	resolvedMihomo string,
	usedExit string,
	err error,
) {
	mode := strings.ToLower(strings.TrimSpace(exitMode))
	pid := strings.TrimSpace(proxyID)
	mh := strings.TrimSpace(mihomoNode)

	if mode == "auto" {
		if s.mihomoMgr != nil {
			var (
				pickedName string
				ok         bool
			)
			if advanceAuto {
				if n, found := s.mihomoMgr.PickNextLoginNode(); found {
					pickedName = n.Name
					ok = true
				}
			} else {
				if n, found := s.mihomoMgr.PeekNextLoginNode(); found {
					pickedName = n.Name
					ok = true
				}
			}
			if ok && pickedName != "" {
				c, label, e := s.proxyMgr.ClientForExit(proxypool.ModeMihomo, "", pickedName, fallbackRegion, timeout)
				if e == nil {
					return c, proxypool.ModeMihomo, "", pickedName, label, nil
				}
			}
		}
		// mihomo 不可用时平滑回落直连
		mode = proxypool.ModeDirect
		pid = ""
		mh = ""
	}

	c, label, e := s.proxyMgr.ClientForExit(mode, pid, mh, fallbackRegion, timeout)
	if e != nil {
		return nil, "", "", "", "", e
	}
	if mode == "" {
		mode = proxypool.ModeDirect
	}
	return c, mode, pid, mh, label, nil
}

func (s *Server) handleAdminSMSSend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone      string `json:"phone"`
		ExitMode   string `json:"exit_mode"`
		ProxyID    string `json:"proxy_id"`
		MihomoNode string `json:"mihomo_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}
	baseURL := s.baseURLFor(protocol.RegionCN)
	client, resMode, resProxyID, resMihomo, usedExit, errClient := s.resolveLoginExit(
		body.ExitMode, body.ProxyID, body.MihomoNode, string(protocol.RegionCN), true, 20*time.Second,
	)
	if errClient != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "国内区登录出口不可用: " + errClient.Error()})
		return
	}

	deviceID, err := protocol.SendSMSCode(r.Context(), client, baseURL, body.Phone)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":           err.Error(),
			"used_exit":       usedExit,
			"used_node":       resMihomo,
			"next_login_node": s.nextLoginNodePreview(),
		})
		return
	}

	// 将本次选中的登录出口绑定到 deviceID 会话，确保后续 verify-code 严格复用同一个节点
	s.oauthMu.Lock()
	now := time.Now()
	for k, v := range s.smsSessions {
		if now.After(v.ExpiresAt) {
			delete(s.smsSessions, k)
		}
	}
	s.smsSessions[deviceID] = smsLoginSession{
		Phone:      strings.TrimSpace(body.Phone),
		ExitMode:   resMode,
		ProxyID:    resProxyID,
		MihomoNode: resMihomo,
		UsedExit:   usedExit,
		ExpiresAt:  now.Add(10 * time.Minute),
	}
	s.oauthMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"message":         fmt.Sprintf("短信验证码已发送（本次登录节点：%s），请查收并输入 6 位数字验证码", usedExit),
		"device_id":       deviceID,
		"used_exit":       usedExit,
		"used_node":       resMihomo,
		"next_login_node": s.nextLoginNodePreview(),
	})
}

func (s *Server) handleAdminSMSVerify(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone      string `json:"phone"`
		Code       string `json:"code"`
		DeviceID   string `json:"device_id"`
		Alias      string `json:"alias"`
		ExitMode   string `json:"exit_mode"`
		ProxyID    string `json:"proxy_id"`
		MihomoNode string `json:"mihomo_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}

	// 优先沿用 send-code 阶段锁定的登录节点，保证同一次短信登录全流程走同一出口 IP
	devKey := strings.TrimSpace(body.DeviceID)
	s.oauthMu.Lock()
	smsSess, hasSess := s.smsSessions[devKey]
	s.oauthMu.Unlock()

	exitMode := strings.TrimSpace(body.ExitMode)
	proxyID := strings.TrimSpace(body.ProxyID)
	mihomoNode := strings.TrimSpace(body.MihomoNode)
	if hasSess && !time.Now().After(smsSess.ExpiresAt) && (exitMode == "" || strings.EqualFold(exitMode, "auto")) {
		exitMode = smsSess.ExitMode
		proxyID = smsSess.ProxyID
		mihomoNode = smsSess.MihomoNode
	}

	baseURL := s.baseURLFor(protocol.RegionCN)
	client, _, _, resMihomo, usedExit, errClient := s.resolveLoginExit(
		exitMode, proxyID, mihomoNode, string(protocol.RegionCN), false, 20*time.Second,
	)
	if errClient != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "国内区登录出口不可用: " + errClient.Error()})
		return
	}

	data, err := protocol.VerifySMSLogin(r.Context(), client, baseURL, body.Phone, body.Code, body.DeviceID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":           err.Error(),
			"used_exit":       usedExit,
			"used_node":       resMihomo,
			"next_login_node": s.nextLoginNodePreview(),
		})
		return
	}

	s.oauthMu.Lock()
	delete(s.smsSessions, devKey)
	s.oauthMu.Unlock()

	cred := &accounts.Credential{
		Region:       protocol.RegionCN,
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresAt:    data.ExpiresAt,
		UserID:       data.UserID,
		UserName:     data.UserName,
		Phone:        data.Phone,
		DeviceID:     data.DeviceID,
		Alias:        strings.TrimSpace(body.Alias),
	}
	view, err := s.pool.SaveCredential(cred, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存凭证失败: " + err.Error()})
		return
	}
	// 职责明确：mihomo 登录节点仅用于登录换票，绝不强绑到新账号日常出站（保持原有默认直连或区域代理池策略）
	route := s.proxyMgr.DescribeAccountRoute(view.ID, string(view.Region))
	view.ProxyRoute = &route
	if s.logger != nil {
		s.logger.Info("Admin", "国内版短信登录成功，已入池账号 [%s]（登录出口：%s，日常出口：%s）", view.Label, usedExit, route.Summary)
	}
	go func(accID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.scheduler.RefreshAccountBalanceWithTrigger(ctx, accID, "login")
		_, _ = s.router.SyncRegionModels(ctx, protocol.RegionCN)
	}(view.ID)
	writeJSON(w, http.StatusOK, map[string]any{
		"message":         fmt.Sprintf("国内版账号登录成功（本次登录节点：%s）", usedExit),
		"used_exit":       usedExit,
		"used_node":       resMihomo,
		"next_login_node": s.nextLoginNodePreview(),
		"account":         view,
	})
}

func (s *Server) handleAdminOAuthCaptcha(w http.ResponseWriter, r *http.Request) {
	baseURL := s.baseURLFor(protocol.RegionGlobal)
	q := r.URL.Query()
	// captcha 阶段仅预览而不推进轮询游标，确保随后 oauth/start 与 oauth/callback 命中同一节点
	client, _, _, resMihomo, usedExit, errClient := s.resolveLoginExit(
		q.Get("exit_mode"), q.Get("proxy_id"), q.Get("mihomo_node"), string(protocol.RegionGlobal), false, 15*time.Second,
	)
	if errClient != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "国际区登录出口不可用: " + errClient.Error()})
		return
	}

	conf, err := protocol.FetchOAuthCaptchaConfig(r.Context(), client, baseURL)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "used_exit": usedExit, "used_node": resMihomo})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"captcha":         conf,
		"used_exit":       usedExit,
		"used_node":       resMihomo,
		"next_login_node": s.nextLoginNodePreview(),
		"variants": []map[string]string{
			{"id": "zai", "label": "Zai 登录", "callback_hint": protocol.DefaultNavigateURI(protocol.OAuthProviderZai)},
			{"id": "google", "label": "Google 登录", "callback_hint": protocol.DefaultNavigateURI(protocol.OAuthProviderGoogle)},
		},
	})
}

func (s *Server) handleAdminOAuthStart(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Provider    string `json:"provider"`
		CaptchaText string `json:"ali_captcha_verify_param"`
		ExitMode    string `json:"exit_mode"`
		ProxyID     string `json:"proxy_id"`
		MihomoNode  string `json:"mihomo_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}
	provider, err := protocol.NormalizeOAuthProvider(body.Provider)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	deviceID := protocol.RandomHex(32)
	navigateURI := protocol.DefaultNavigateURI(provider)
	baseURL := s.baseURLFor(protocol.RegionGlobal)

	// 发起本次 OAuth 授权会话时正式分配并锁定节点（auto 模式下推进游标，下一次新登录自动切换下一个健康节点）
	client, resMode, resProxyID, resMihomo, usedExit, errClient := s.resolveLoginExit(
		body.ExitMode, body.ProxyID, body.MihomoNode, string(protocol.RegionGlobal), true, 15*time.Second,
	)
	if errClient != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "国际区登录出口不可用: " + errClient.Error()})
		return
	}

	authURL, err := protocol.RequestOAuthURL(r.Context(), client, baseURL, provider, navigateURI, body.CaptchaText, deviceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("OAuth", "获取国际版 %s 授权链接失败（出口：%s）: %v", strings.ToUpper(string(provider)), usedExit, err)
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":           err.Error(),
			"used_exit":       usedExit,
			"used_node":       resMihomo,
			"next_login_node": s.nextLoginNodePreview(),
		})
		return
	}

	sessionID := "oauth_" + protocol.RandomHex(16)
	s.oauthMu.Lock()
	now := time.Now()
	for k, v := range s.oauthSessions {
		if now.After(v.ExpiresAt) {
			delete(s.oauthSessions, k)
		}
	}
	s.oauthSessions[sessionID] = oauthSession{
		Provider:    provider,
		DeviceID:    deviceID,
		NavigateURI: navigateURI,
		ExitMode:    resMode,
		ProxyID:     resProxyID,
		MihomoNode:  resMihomo,
		UsedExit:    usedExit,
		ExpiresAt:   now.Add(10 * time.Minute),
	}
	s.oauthMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":      sessionID,
		"provider":        provider,
		"auth_url":        authURL,
		"callback_hint":   navigateURI,
		"used_exit":       usedExit,
		"used_node":       resMihomo,
		"next_login_node": s.nextLoginNodePreview(),
		"message":         fmt.Sprintf("请在新标签页完成 %s 授权（本次登录节点：%s），然后将浏览器最终跳转的 %s?code=... 整条链接粘贴回来", strings.ToUpper(string(provider)), usedExit, navigateURI),
	})
}

func (s *Server) handleAdminOAuthCallback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SessionID   string `json:"session_id"`
		CallbackURL string `json:"callback_url"`
		Alias       string `json:"alias"`
		ExitMode    string `json:"exit_mode"`
		ProxyID     string `json:"proxy_id"`
		MihomoNode  string `json:"mihomo_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}

	s.oauthMu.Lock()
	sess, ok := s.oauthSessions[strings.TrimSpace(body.SessionID)]
	s.oauthMu.Unlock()
	if !ok || time.Now().After(sess.ExpiresAt) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "OAuth 登录会话不存在或已过期，请重新获取授权链接"})
		return
	}

	provider, code, upstreamState, okParse := protocol.ParseOAuthCallbackURL(body.CallbackURL)
	if !okParse {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "回调链接格式有误，请粘贴完整的 localhost 回调地址（需包含 code 参数）"})
		return
	}
	if provider != sess.Provider {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "回调链接渠道与当前发起的登录方式不一致"})
		return
	}

	// 同一次 OAuth 会话严格复用 oauth-url 阶段锁定的节点；仅当用户显式手动切到非 auto 特定节点时才覆盖
	exitMode := strings.TrimSpace(body.ExitMode)
	proxyID := strings.TrimSpace(body.ProxyID)
	mihomoNode := strings.TrimSpace(body.MihomoNode)
	if exitMode == "" || strings.EqualFold(exitMode, "auto") {
		exitMode = sess.ExitMode
		proxyID = sess.ProxyID
		mihomoNode = sess.MihomoNode
	}

	baseURL := s.baseURLFor(protocol.RegionGlobal)
	client, _, _, resMihomo, usedExit, errClient := s.resolveLoginExit(
		exitMode, proxyID, mihomoNode, string(protocol.RegionGlobal), false, 20*time.Second,
	)
	if errClient != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "国际区登录出口不可用: " + errClient.Error()})
		return
	}

	data, err := protocol.ExchangeOAuthCode(r.Context(), client, baseURL, provider, code, upstreamState, sess.NavigateURI, sess.DeviceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("OAuth", "国际版 %s 回调换票失败（出口：%s）: %v", strings.ToUpper(string(provider)), usedExit, err)
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":           err.Error(),
			"used_exit":       usedExit,
			"used_node":       resMihomo,
			"next_login_node": s.nextLoginNodePreview(),
		})
		return
	}

	s.oauthMu.Lock()
	delete(s.oauthSessions, strings.TrimSpace(body.SessionID))
	s.oauthMu.Unlock()

	cred := &accounts.Credential{
		Region:       protocol.RegionGlobal,
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresAt:    data.ExpiresAt,
		UserID:       data.UserID,
		UserName:     data.UserName,
		DeviceID:     data.DeviceID,
		Alias:        strings.TrimSpace(body.Alias),
	}
	view, err := s.pool.SaveCredential(cred, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存凭证失败: " + err.Error()})
		return
	}
	// 职责明确：mihomo 登录节点仅用于换票拿凭证，绝不自动绑定到新账号日常出站（保持原有默认直连或区域代理池策略）
	route := s.proxyMgr.DescribeAccountRoute(view.ID, string(view.Region))
	view.ProxyRoute = &route
	if s.logger != nil {
		s.logger.Info("Admin", "国际版 %s OAuth 登录成功，已入池账号 [%s]（本次登录节点：%s，日常出口：%s）", provider, view.Label, usedExit, route.Summary)
	}
	go func(accID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.scheduler.RefreshAccountBalanceWithTrigger(ctx, accID, "login")
		_, _ = s.router.SyncRegionModels(ctx, protocol.RegionGlobal)
	}(view.ID)

	writeJSON(w, http.StatusOK, map[string]any{
		"message":         fmt.Sprintf("国际版账号登录成功（本次登录节点：%s）", usedExit),
		"used_exit":       usedExit,
		"used_node":       resMihomo,
		"next_login_node": s.nextLoginNodePreview(),
		"account":         view,
	})
}

func (s *Server) handleAdminUpdateAccount(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	var body struct {
		Alias    *string `json:"alias"`
		Note     *string `json:"note"`
		Disabled *bool   `json:"disabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}
	if err := s.pool.UpdateMeta(id, body.Alias, body.Note, body.Disabled); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	s.router.PruneInactiveRegions()
	if body.Disabled != nil && !*body.Disabled {
		if acc, ok := s.pool.GetByID(id); ok {
			go func(reg protocol.Region) {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				_, _ = s.router.SyncRegionModels(ctx, reg)
			}(acc.Cred.Region)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "账号信息已更新",
	})
}

func (s *Server) handleAdminDeleteAccount(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if err := s.pool.Delete(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	s.proxyMgr.RemoveAccountBinding(id)
	s.router.PruneInactiveRegions()
	if s.logger != nil {
		s.logger.Info("Admin", "已删除账号: %s", id)
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": "账号已删除"})
}

func (s *Server) handleAdminBatchAccounts(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Action string   `json:"action"` // enable | disable | delete
		IDs    []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败"})
		return
	}
	if len(body.IDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请先选择至少一个账号"})
		return
	}
	var affected int
	switch strings.ToLower(strings.TrimSpace(body.Action)) {
	case "enable":
		affected = s.pool.BatchSetDisabled(body.IDs, false)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = s.router.RefreshAllModels(ctx)
		}()
	case "disable":
		affected = s.pool.BatchSetDisabled(body.IDs, true)
		s.router.PruneInactiveRegions()
	case "delete":
		affected = s.pool.BatchDelete(body.IDs)
		for _, id := range body.IDs {
			s.proxyMgr.RemoveAccountBinding(id)
		}
		s.router.PruneInactiveRegions()
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "不支持的批量动作: " + body.Action})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  fmt.Sprintf("批量操作完成，共处理 %d 个账号", affected),
		"affected": affected,
	})
}

func (s *Server) handleAdminRefreshToken(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if err := s.scheduler.RefreshAccountToken(r.Context(), id); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": "Token 刷新成功并已无损写回"})
}

func (s *Server) handleAdminRefreshBalance(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	bal, err := s.scheduler.RefreshAccountBalance(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": fmt.Sprintf("余额刷新成功：%.0f 积分", bal),
		"balance": bal,
	})
}

func (s *Server) handleAdminRefreshAllBalances(w http.ResponseWriter, r *http.Request) {
	okCount := s.scheduler.RefreshAllBalancesWithTrigger(r.Context(), "manual")
	writeJSON(w, http.StatusOK, map[string]any{
		"message": fmt.Sprintf("已刷新 %d 个账号的积分余额", okCount),
		"updated": okCount,
		"stats":   s.pool.Stats(),
	})
}

func (s *Server) handleAdminAccountBalanceHistory(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	limit := 300
	if q := strings.TrimSpace(r.URL.Query().Get("limit")); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 && n <= 300 {
			limit = n
		}
	}
	acc, ok := s.pool.GetByID(id)
	label := id
	bal := 0.0
	balKnown := false
	if ok {
		label = acc.Cred.DisplayLabel()
		bal = acc.Balance
		balKnown = acc.BalanceKnown
	}
	split := s.pool.BalanceHistorySplit(id, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"account_id":     id,
		"account_label":  label,
		"balance":        bal,
		"balance_known":  balKnown,
		"usage_history":  split.Usage,
		"event_history":  split.Events,
		"usage_count":    split.UsageCount,
		"event_count":    split.EventCount,
		"abnormal_count": split.AbnormalCount,
		"history":        s.pool.BalanceHistory(id, limit),
	})
}

func (s *Server) handleAdminClearAccountBalanceHistory(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	cleared := s.pool.ClearBalanceHistory(id)
	writeJSON(w, http.StatusOK, map[string]any{
		"message": fmt.Sprintf("已清空该账号积分变动流水（共清除 %d 条）", cleared),
		"cleared": cleared,
	})
}

func (s *Server) handleAdminBalanceHistory(w http.ResponseWriter, r *http.Request) {
	accountID := strings.TrimSpace(r.URL.Query().Get("account_id"))
	limit := 300
	if q := strings.TrimSpace(r.URL.Query().Get("limit")); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 && n <= 300 {
			limit = n
		}
	}
	split := s.pool.BalanceHistorySplit(accountID, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"usage_history":  split.Usage,
		"event_history":  split.Events,
		"usage_count":    split.UsageCount,
		"event_count":    split.EventCount,
		"abnormal_count": split.AbnormalCount,
		"history":        s.pool.BalanceHistory(accountID, limit),
	})
}

func (s *Server) handleAdminCheckinRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AccountID string `json:"account_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	if id := strings.TrimSpace(body.AccountID); id != "" {
		rec := s.scheduler.CheckinAccount(r.Context(), id, "manual")
		writeJSON(w, http.StatusOK, map[string]any{
			"message": rec.Message,
			"results": []any{rec},
		})
		return
	}
	results := s.scheduler.CheckinAll(r.Context(), "manual")
	writeJSON(w, http.StatusOK, map[string]any{
		"message": fmt.Sprintf("全部签到任务执行完毕，共处理 %d 个账号", len(results)),
		"results": results,
	})
}

func (s *Server) handleAdminCheckinHistory(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"config":  s.cfgMgr.Get().Checkin,
		"history": s.scheduler.History(limit),
	})
}

func (s *Server) handleAdminModels(w http.ResponseWriter, _ *http.Request) {
	cfg := s.cfgMgr.Get()
	writeJSON(w, http.StatusOK, map[string]any{
		"models":      s.router.ListAllModelsForAdmin(),
		"routing":     cfg.Routing,
		"sync_status": s.router.SyncStatuses(),
	})
}

func (s *Server) handleAdminRefreshModels(w http.ResponseWriter, r *http.Request) {
	statuses := s.router.RefreshAllModels(r.Context())
	cn := statuses["cn"]
	gl := statuses["global"]

	if !cn.HasAccount && !gl.HasAccount {
		writeJSON(w, http.StatusOK, map[string]any{
			"message":     "当前国内版与国际版均无已启用的有效账号，模型目录已保持为空",
			"sync_status": statuses,
			"count":       0,
		})
		return
	}

	var errs []string
	if cn.HasAccount && cn.ErrorMessage != "" {
		errs = append(errs, "国内版："+cn.ErrorMessage)
	}
	if gl.HasAccount && gl.ErrorMessage != "" {
		errs = append(errs, "国际版："+gl.ErrorMessage)
	}
	total := cn.ModelCount + gl.ModelCount
	if len(errs) > 0 && total == 0 {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":       "实时同步模型目录失败：" + strings.Join(errs, "；"),
			"sync_status": statuses,
		})
		return
	}
	msg := fmt.Sprintf("实时同步完成：国内版 %d 个 / 国际版 %d 个", cn.ModelCount, gl.ModelCount)
	if len(errs) > 0 {
		msg += "（部分区域失败：" + strings.Join(errs, "；") + "）"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":     msg,
		"count":       total,
		"sync_status": statuses,
	})
}

func (s *Server) handleAdminUpdateRouting(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PrefixMode     *string             `json:"prefix_mode"`
		DisabledModels *[]string           `json:"disabled_models"`
		Aliases        map[string][]string `json:"aliases"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "参数解析失败: " + err.Error()})
		return
	}
	updated, err := s.cfgMgr.Update(func(c *config.Config) error {
		if body.PrefixMode != nil {
			c.Routing.PrefixMode = *body.PrefixMode
		}
		if body.DisabledModels != nil {
			c.Routing.DisabledModels = *body.DisabledModels
		}
		if body.Aliases != nil {
			c.Routing.Aliases = body.Aliases
		}
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存模型路由配置失败: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "模型路由策略已保存并生效",
		"routing": updated.Routing,
	})
}

func (s *Server) handleAdminGetSettings(w http.ResponseWriter, _ *http.Request) {
	cfg := s.cfgMgr.Get()
	writeJSON(w, http.StatusOK, map[string]any{
		"listen":    cfg.Listen,
		"log_level": cfg.LogLevel,
		"auth_dir":  cfg.AuthDir,
		"data_dir":  cfg.DataDir,
		"upstream":  cfg.Upstream,
		"checkin":   cfg.Checkin,
		"retry":     cfg.Retry,
	})
}

func (s *Server) handleAdminUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Listen   *string                `json:"listen"`
		LogLevel *string                `json:"log_level"`
		Upstream *config.UpstreamConfig `json:"upstream"`
		Checkin  *config.CheckinConfig  `json:"checkin"`
		Retry    *config.RetryConfig    `json:"retry"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "解析设置参数失败: " + err.Error()})
		return
	}

	oldListen := s.cfgMgr.Get().Listen
	updated, err := s.cfgMgr.Update(func(c *config.Config) error {
		if body.Listen != nil {
			c.Listen = *body.Listen
		}
		if body.LogLevel != nil {
			c.LogLevel = *body.LogLevel
		}
		if body.Upstream != nil {
			c.Upstream = *body.Upstream
		}
		if body.Checkin != nil {
			c.Checkin = *body.Checkin
		}
		if body.Retry != nil {
			c.Retry = *body.Retry
		}
		return nil
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "保存配置失败: " + err.Error()})
		return
	}

	if s.logger != nil {
		s.logger.SetLevel(updated.LogLevel)
		s.logger.Info("Admin", "系统配置已更新（日志级别=%s，自动签到=%v %s）", updated.LogLevel, updated.Checkin.Enabled, updated.Checkin.Time)
	}

	msg := "配置已保存并立即生效"
	if updated.Listen != oldListen {
		msg = "配置已保存（监听地址变更将在下次重启程序后生效）"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": msg,
	})
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	limit := 120
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	var entries any = []any{}
	if s.logger != nil {
		entries = s.logger.Recent(limit)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"level": s.cfgMgr.Get().LogLevel,
		"logs":  entries,
	})
}

func (s *Server) handleAdminClearLogs(w http.ResponseWriter, _ *http.Request) {
	if s.logger != nil {
		s.logger.Clear()
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": "内存日志已清空"})
}
