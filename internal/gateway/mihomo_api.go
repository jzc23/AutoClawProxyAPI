package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (s *Server) handleAdminMihomoStatus(w http.ResponseWriter, _ *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	writeJSON(w, http.StatusOK, s.mihomoMgr.Snapshot())
}

func (s *Server) handleAdminMihomoDownloadStart(w http.ResponseWriter, _ *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	st, err := s.mihomoMgr.StartDownload()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  "已启动 mihomo 内核后台下载（国内加速源优先，失败自动回落 GitHub）",
		"download": st,
	})
}

func (s *Server) handleAdminMihomoDownloadProgress(w http.ResponseWriter, _ *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	snap := s.mihomoMgr.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"binary_found": snap.BinaryFound,
		"binary_path":  snap.BinaryPath,
		"download":     snap.Download,
	})
}

func (s *Server) handleAdminMihomoConfigUpdate(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	var body struct {
		Enabled              *bool    `json:"enabled"`
		Subscriptions        []string `json:"subscriptions"`
		AutoProbeEnabled     *bool    `json:"auto_probe_enabled"`
		AutoProbeIntervalMin *int     `json:"auto_probe_interval_min"`
		MaxNodes             *int     `json:"max_nodes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	// 大订阅首次同步需要下载 provider 并重启内核，脱离浏览器请求上下文，避免客户端超时导致同步中断。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer cancel()
	snap, err := s.mihomoMgr.UpdateConfig(ctx, body.Enabled, body.Subscriptions, body.AutoProbeEnabled, body.AutoProbeIntervalMin, body.MaxNodes)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    err.Error(),
			"snapshot": snap,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  fmt.Sprintf("mihomo 订阅代理桥配置已保存（真实节点 %d 个，可用 %d 个）", snap.TotalCount, snap.HealthyCount),
		"snapshot": snap,
	})
}

func (s *Server) handleAdminMihomoRefresh(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer cancel()
	snap, err := s.mihomoMgr.RefreshSubscriptions(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    err.Error(),
			"snapshot": snap,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  fmt.Sprintf("订阅节点同步完成：共 %d 个真实节点（已过滤 %d 个假节点），正在后台测速…", snap.TotalCount, snap.FilteredFakeCount),
		"snapshot": snap,
	})
}

func (s *Server) handleAdminMihomoDeleteNodes(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	var body struct {
		Names []string `json:"names"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	// 删除后需要重建配置并重启内核释放端口，脱离浏览器请求上下文避免客户端超时中断。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer cancel()
	before := s.mihomoMgr.Snapshot()
	removing := s.mihomoMgr.NodesByName(body.Names)
	snap, err := s.mihomoMgr.DeleteNodes(ctx, body.Names)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    err.Error(),
			"snapshot": snap,
		})
		return
	}
	msg := fmt.Sprintf("已删除 %d 个机场节点，剩余 %d 个（累计排除 %d 个，可随时恢复）",
		before.TotalCount-snap.TotalCount, snap.TotalCount, snap.ExcludedCount)
	if snap.LastError != "" {
		msg += "；" + snap.LastError
	}
	// 联动清理此前由「加入代理节点池」转出、指向这些节点回环端口的池节点，避免留下指向空端口的死节点。
	if s.proxyMgr != nil && len(removing) > 0 {
		ports := make([]int, 0, len(removing))
		for _, n := range removing {
			ports = append(ports, n.Port)
		}
		removed, unbound, errClean := s.proxyMgr.RemoveNodesByLocalPorts(ports)
		if errClean != nil {
			msg += "；清理关联的代理池节点失败: " + errClean.Error()
		} else if removed > 0 {
			msg += fmt.Sprintf("；已同步清理 %d 个关联的代理池节点", removed)
			if unbound > 0 {
				msg += fmt.Sprintf("，并解绑 %d 处固定出口", unbound)
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  msg,
		"snapshot": snap,
	})
}

func (s *Server) handleAdminMihomoRestoreNodes(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer cancel()
	snap, resynced, err := s.mihomoMgr.RestoreExcludedNodes(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    err.Error(),
			"snapshot": snap,
		})
		return
	}
	msg := fmt.Sprintf("已恢复被删除的机场节点，当前共有 %d 个真实节点", snap.TotalCount)
	if !resynced {
		msg = fmt.Sprintf("已清空删除记录，当前共有 %d 个真实节点；启用订阅桥并同步订阅后节点会自动回来", snap.TotalCount)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  msg,
		"snapshot": snap,
	})
}

// handleAdminMihomoNodesToPool 把选中的机场节点以 http://127.0.0.1:<端口> 形式导入代理节点池，
// 便于日常请求直接复用这些登录节点；节点名称沿用机场原名，重复地址自动跳过。
func (s *Server) handleAdminMihomoNodesToPool(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil || s.proxyMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥或代理池未初始化"})
		return
	}
	var body struct {
		Names []string `json:"names"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	nodes := s.mihomoMgr.NodesByName(body.Names)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "未找到要转移的机场节点，节点列表可能已变化，请刷新后重试"})
		return
	}

	var sb strings.Builder
	for _, n := range nodes {
		sb.WriteString(fmt.Sprintf("http://127.0.0.1:%d#%s\n", n.Port, url.QueryEscape(n.Name)))
	}
	summary, err := s.proxyMgr.ImportBulk(r.Context(), sb.String())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "写入代理节点池失败: " + err.Error()})
		return
	}

	msg := fmt.Sprintf("已把 %d 个机场节点加入代理节点池（指向 127.0.0.1 本地端口，可直接用于日常请求）", summary.Added)
	if summary.Duplicate > 0 {
		msg += fmt.Sprintf("；%d 个已在池中，已跳过", summary.Duplicate)
	}
	if missing := len(body.Names) - len(nodes); missing > 0 {
		msg += fmt.Sprintf("；%d 个节点已不在当前列表中", missing)
	}
	if summary.Added > 0 {
		msg += "，正在后台测活…"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": msg,
		"summary": summary,
	})
}

func (s *Server) handleAdminMihomoProbe(w http.ResponseWriter, r *http.Request) {
	if s.mihomoMgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "mihomo 订阅代理桥未初始化"})
		return
	}
	snap, err := s.mihomoMgr.ProbeAllNodes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    err.Error(),
			"snapshot": snap,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message":  fmt.Sprintf("测速完成：%d / %d 个机场节点可用，已按延迟升序排列", snap.HealthyCount, snap.TotalCount),
		"snapshot": snap,
	})
}
