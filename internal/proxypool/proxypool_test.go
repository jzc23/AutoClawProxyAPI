package proxypool

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"autoclaw2api/internal/config"
)

func TestParseBulkURIAndBase64AndClash(t *testing.T) {
	// 1. 多行 URI（含支持的、不支持的、无效的）
	uriText := `
http://user:pass123@127.0.0.1:7890#香港HTTP
socks5://10.0.0.1:1080#东京SOCKS5
ss://YWVzLTI1Ni1nY206cGFzcw@1.2.3.4:8388#SS节点
not-a-valid-proxy
`
	entries := ParseBulkInput(uriText)
	if len(entries) != 4 {
		t.Fatalf("预期解析 4 条记录，实际: %d", len(entries))
	}
	if entries[0].Name != "香港HTTP" || entries[0].Protocol != "http" || entries[0].Normalized == "" {
		t.Fatalf("HTTP 节点解析异常: %+v", entries[0])
	}
	if MaskProxyURL(entries[0].Normalized) != "http://user:***@127.0.0.1:7890" {
		t.Fatalf("代理密码掩码异常: %s", MaskProxyURL(entries[0].Normalized))
	}
	if !entries[2].Unsupported || entries[2].Protocol != "ss" {
		t.Fatalf("ss 节点应被标记为 unsupported: %+v", entries[2])
	}
	if entries[3].Normalized != "" || entries[3].Unsupported {
		t.Fatalf("无效行应被标记为 invalid: %+v", entries[3])
	}

	// 2. Base64 订阅自动解码
	b64 := base64.StdEncoding.EncodeToString([]byte("socks5h://u1:p1@proxy.example.com:1080#US-1\nvmess://abc"))
	b64Entries := ParseBulkInput(b64)
	if len(b64Entries) != 2 || b64Entries[0].Protocol != "socks5h" || !b64Entries[1].Unsupported {
		t.Fatalf("Base64 订阅解析异常: %+v", b64Entries)
	}

	// 3. Clash YAML（块式 + 流式对象）
	clashYAML := `
proxies:
  - name: 新加坡S5
    type: socks5
    server: sg.example.com
    port: 1080
    username: alice
    password: secret
  - {name: 日本HTTP, type: http, server: jp.example.com, port: 8080}
  - name: 机场Trojan
    type: trojan
    server: tr.example.com
    port: 443
`
	clashEntries := ParseBulkInput(clashYAML)
	if len(clashEntries) != 3 {
		t.Fatalf("Clash YAML 预期 3 条，实际: %d", len(clashEntries))
	}
	if clashEntries[0].Name != "新加坡S5" || !strings.Contains(clashEntries[0].Normalized, "socks5://alice:secret@sg.example.com:1080") {
		t.Fatalf("Clash 块式节点解析失败: %+v", clashEntries[0])
	}
	if clashEntries[1].Name != "日本HTTP" || clashEntries[1].Normalized != "http://jp.example.com:8080" {
		t.Fatalf("Clash 流式节点解析失败: %+v", clashEntries[1])
	}
	if !clashEntries[2].Unsupported || clashEntries[2].Protocol != "trojan" {
		t.Fatalf("Clash trojan 节点应标记 unsupported: %+v", clashEntries[2])
	}
}

func TestManagerBindingsPriorityGuardAndTransportEviction(t *testing.T) {
	// 启动一个本地伪代理服务器（收到请求直接返回 204）用于测活验证
	fakeProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer fakeProxy.Close()

	mgr, err := NewManager(t.TempDir(), config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatalf("初始化代理池失败: %v", err)
	}

	// 批量导入（含重复项）
	summary, err := mgr.ImportBulk(context.Background(), fakeProxy.URL+"#测试节点A\n"+fakeProxy.URL+"#重复项\nhttp://127.0.0.1:19999#测试节点B")
	if err != nil {
		t.Fatalf("导入失败: %v", err)
	}
	if summary.Added != 2 || summary.Duplicate != 1 {
		t.Fatalf("导入统计异常: %+v", summary)
	}
	nodeA := summary.Items[0].ID
	nodeB := summary.Items[2].ID

	// 配置测活目标并执行单节点测活
	_, err = mgr.UpdateHealthConfig(HealthConfig{
		Enabled:     true,
		URL:         "http://example.com/healthz",
		IntervalSec: 30,
		TimeoutSec:  5,
	})
	if err != nil {
		t.Fatalf("更新测活配置失败: %v", err)
	}
	viewA, err := mgr.ProbeNode(context.Background(), nodeA)
	if err != nil || viewA.Status != StatusHealthy {
		t.Fatalf("节点 A 测活应健康，实际: %+v err=%v", viewA, err)
	}

	// 三级路由优先级验证：
	// 1. 区域 global 设为 fixed(nodeA)，账号 g1 默认 inherit -> 生效 nodeA
	if err := mgr.SetRegionBinding("global", Binding{Mode: ModeFixed, ProxyID: nodeA}); err != nil {
		t.Fatal(err)
	}
	r1 := mgr.DescribeAccountRoute("acc-g1", "global")
	if r1.EffectiveMode != ModeFixed || r1.NodeID != nodeA {
		t.Fatalf("账号继承区域出口解析错误: %+v", r1)
	}

	// 2. 账号 g1 显式绑定 fixed(nodeB) -> 覆盖区域默认
	if err := mgr.SetAccountBinding("acc-g1", Binding{Mode: ModeFixed, ProxyID: nodeB}); err != nil {
		t.Fatal(err)
	}
	r2 := mgr.DescribeAccountRoute("acc-g1", "global")
	if r2.EffectiveMode != ModeFixed || r2.NodeID != nodeB {
		t.Fatalf("账号固定绑定未优先于区域设置: %+v", r2)
	}

	// 3. 账号 g2 设为 direct -> 覆盖区域默认
	if err := mgr.SetAccountBinding("acc-g2", Binding{Mode: ModeDirect}); err != nil {
		t.Fatal(err)
	}
	r3 := mgr.DescribeAccountRoute("acc-g2", "global")
	if r3.EffectiveMode != ModeDirect {
		t.Fatalf("账号直连设置未覆盖区域默认: %+v", r3)
	}

	// 4. 防误删保护：nodeB 被 acc-g1 绑定，nodeA 被 global 区域绑定，两者均不能直接删除
	if err := mgr.DeleteNode(nodeB); !errors.Is(err, ErrNodeBound) {
		t.Fatalf("已绑定账号的节点应触发防误删拦截，实际 err=%v", err)
	}
	if err := mgr.DeleteNode(nodeA); !errors.Is(err, ErrNodeBound) {
		t.Fatalf("已绑定区域的节点应触发防误删拦截，实际 err=%v", err)
	}

	// 5. 获取客户端触发连接池缓存，随后禁用节点应自动驱逐旧连接池并阻断绑定流量
	if _, err := mgr.ClientForAccount("acc-g1", "global", 5*time.Second); err != nil {
		t.Fatalf("获取账号专属客户端失败: %v", err)
	}
	if !mgr.HasCachedTransport(nodeB) {
		t.Fatalf("预期 nodeB 连接池已被缓存")
	}
	disabled := false
	if _, err := mgr.UpdateNode(nodeB, nil, nil, &disabled); err != nil {
		t.Fatalf("停用节点失败: %v", err)
	}
	if mgr.HasCachedTransport(nodeB) {
		t.Fatalf("停用节点后应立即关闭并清除旧连接池")
	}
	if _, err := mgr.ClientForAccount("acc-g1", "global", 5*time.Second); err == nil {
		t.Fatalf("绑定已停用固定节点时应返回明确错误")
	}

	// 解绑后可正常删除
	if err := mgr.SetAccountBinding("acc-g1", Binding{Mode: ModeInherit}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DeleteNode(nodeB); err != nil {
		t.Fatalf("解绑后删除节点应成功: %v", err)
	}
}

type stubMihomoResolver struct {
	nodes map[string]string
}

func (s stubMihomoResolver) ResolveNodeProxyURL(name string) (string, string, error) {
	if u, ok := s.nodes[name]; ok {
		return u, name, nil
	}
	return "", "", ErrNodeNotFound
}

func TestMihomoBindingAndLoginClientForExit(t *testing.T) {
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("via-mihomo-local-mixed"))
	}))
	defer proxySrv.Close()

	mgr, err := NewManager(t.TempDir(), config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetMihomoResolver(stubMihomoResolver{
		nodes: map[string]string{
			"🇯🇵 日本 | 01": proxySrv.URL,
		},
	})

	// 1. mihomo 登录节点专用于登录换票，禁止绑定到账号或区域日常出站
	if err := mgr.SetAccountBinding("acc-jp-1", Binding{Mode: ModeMihomo, MihomoNode: "🇯🇵 日本 | 01"}); err == nil {
		t.Fatalf("账号日常出口不应允许绑定 mihomo 登录节点")
	}
	if err := mgr.SetRegionBinding("global", Binding{Mode: ModeMihomo, MihomoNode: "🇯🇵 日本 | 01"}); err == nil {
		t.Fatalf("区域日常出口不应允许绑定 mihomo 登录节点")
	}
	route := mgr.DescribeAccountRoute("acc-jp-1", "global")
	if route.EffectiveMode != ModeDirect {
		t.Fatalf("默认日常出口应保持直连，实际: %+v", route)
	}

	// 2. 登录换票专用 ClientForExit(mihomo) 正常经由本地 mixed 端口转发
	cLogin, usedLabel, err := mgr.ClientForExit(ModeMihomo, "", "🇯🇵 日本 | 01", "global", 5*time.Second)
	if err != nil || !strings.Contains(usedLabel, "🇯🇵 日本 | 01") {
		t.Fatalf("ClientForExit(mihomo) 失败: label=%q err=%v", usedLabel, err)
	}
	resp2, err := cLogin.Get("http://example.invalid/login")
	if err != nil {
		t.Fatalf("经由登录指定 mihomo 节点请求失败: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("期望 HTTP 200，实际 %d", resp2.StatusCode)
	}
}
