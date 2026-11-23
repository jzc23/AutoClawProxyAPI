package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求参数解析失败"})
		return
	}
	snap, err := s.mihomoMgr.UpdateConfig(r.Context(), body.Enabled, body.Subscriptions, body.AutoProbeEnabled, body.AutoProbeIntervalMin)
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
	snap, err := s.mihomoMgr.RefreshSubscriptions(r.Context())
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
