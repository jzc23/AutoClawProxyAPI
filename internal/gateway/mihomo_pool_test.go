package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/mihomo"
	"autoclaw2api/internal/proxypool"
)

func TestMihomoNodesToPoolImportsLocalPorts(t *testing.T) {
	dataDir := t.TempDir()
	mh, err := mihomo.NewManager(t.TempDir(), filepath.Join(dataDir, "mihomo"), nil)
	if err != nil {
		t.Fatalf("mihomo.NewManager: %v", err)
	}
	mh.SetNodesForTest(true, []mihomo.SubNode{
		{Name: "🇯🇵 日本 01 · 专线", Port: 20001, DelayMS: 83},
		{Name: "US East 02", Port: 20002, DelayMS: 150},
	})
	pp, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatalf("proxypool.NewManager: %v", err)
	}
	srv := &Server{proxyMgr: pp}
	srv.SetMihomoManager(mh)

	call := func(names []string) (int, map[string]any) {
		body, _ := json.Marshal(map[string]any{"names": names})
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/admin/mihomo/nodes/to-pool", bytes.NewReader(body))
		srv.handleAdminMihomoNodesToPool(rec, req)
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}

	code, out := call([]string{"🇯🇵 日本 01 · 专线", "US East 02"})
	if code != http.StatusOK {
		t.Fatalf("首次转移应成功，状态码 %d: %v", code, out)
	}
	nodes := pp.Snapshot().Nodes
	if len(nodes) != 2 {
		t.Fatalf("代理池应新增 2 个节点，实际 %d", len(nodes))
	}
	byName := map[string]proxypool.NodeView{}
	for _, n := range nodes {
		byName[n.Name] = n
	}
	jp, ok := byName["🇯🇵 日本 01 · 专线"]
	if !ok {
		t.Fatalf("机场节点名称应保留为池节点名称: %+v", nodes)
	}
	if jp.Protocol != "http" || !strings.Contains(jp.MaskedURL, "127.0.0.1:20001") {
		t.Fatalf("池节点应指向 mihomo 回环端口: %+v", jp)
	}
	if us, ok := byName["US East 02"]; !ok || !strings.Contains(us.MaskedURL, "127.0.0.1:20002") {
		t.Fatalf("第二个节点端口映射不符: %+v", us)
	}

	// 重复转移：同一端口地址会被判重跳过，不产生重复池节点
	code, out = call([]string{"🇯🇵 日本 01 · 专线", "US East 02"})
	if code != http.StatusOK {
		t.Fatalf("重复转移应返回 200: %d %v", code, out)
	}
	if msg, _ := out["message"].(string); !strings.Contains(msg, "已在池中") {
		t.Fatalf("重复转移应提示已跳过: %q", msg)
	}
	if got := len(pp.Snapshot().Nodes); got != 2 {
		t.Fatalf("重复转移不应新增池节点，实际 %d", got)
	}

	// 未命中的名称：全部不存在时报错，部分不存在时只导入存在的节点
	if code, _ := call([]string{"不存在的节点"}); code != http.StatusBadRequest {
		t.Fatalf("全部未命中应返回 400，实际 %d", code)
	}
	if code, _ := call([]string{"不存在的节点", "US East 02"}); code != http.StatusOK {
		t.Fatalf("部分命中应成功，实际 %d", code)
	}

	// 删除机场节点时联动清理指向其回环端口的池节点
	delBody, _ := json.Marshal(map[string]any{"names": []string{"🇯🇵 日本 01 · 专线", "US East 02"}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/admin/mihomo/nodes/delete", bytes.NewReader(delBody))
	srv.handleAdminMihomoDeleteNodes(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("删除机场节点应成功，状态码 %d: %s", rec.Code, rec.Body.String())
	}
	var delOut map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &delOut)
	if msg, _ := delOut["message"].(string); !strings.Contains(msg, "已同步清理 2 个关联的代理池节点") {
		t.Fatalf("删除后应提示联动清理池节点: %q", msg)
	}
	if got := len(pp.Snapshot().Nodes); got != 0 {
		t.Fatalf("关联池节点应被全部清理，实际剩余 %d", got)
	}
}
