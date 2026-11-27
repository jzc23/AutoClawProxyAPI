package proxypool

import (
	"context"
	"strings"
	"testing"

	"autoclaw2api/internal/config"
)

func TestRemoveNodesByLocalPorts(t *testing.T) {
	m, err := NewManager(t.TempDir(), config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if _, err := m.ImportBulk(context.Background(), strings.Join([]string{
		"http://127.0.0.1:20001#机场节点A",
		"http://127.0.0.1:20002#机场节点B",
		"http://10.0.0.9:20003#远程节点（同端口号但非本机，不应被清理）",
		"socks5://1.2.3.4:1080#远程节点",
	}, "\n")); err != nil {
		t.Fatalf("ImportBulk: %v", err)
	}

	var targetID string
	for _, n := range m.Snapshot().Nodes {
		if strings.Contains(n.MaskedURL, "127.0.0.1:20001") {
			targetID = n.ID
		}
	}
	if targetID == "" {
		t.Fatalf("未找到待测试的本机端口节点")
	}
	if err := m.SetRegionBinding("global", Binding{Mode: ModeFixed, ProxyID: targetID}); err != nil {
		t.Fatalf("SetRegionBinding: %v", err)
	}

	removed, unbound, err := m.RemoveNodesByLocalPorts([]int{20001})
	if err != nil {
		t.Fatalf("RemoveNodesByLocalPorts: %v", err)
	}
	if removed != 1 || unbound != 1 {
		t.Fatalf("应清理 1 个节点并解绑 1 处固定出口，实际 removed=%d unbound=%d", removed, unbound)
	}

	snap := m.Snapshot()
	if len(snap.Nodes) != 3 {
		t.Fatalf("应剩余 3 个节点，实际 %d", len(snap.Nodes))
	}
	for _, n := range snap.Nodes {
		if strings.Contains(n.MaskedURL, "127.0.0.1:20001") {
			t.Fatalf("指向被删回环端口的节点应已清理: %+v", n)
		}
	}
	// 同端口号的非回环地址必须保留
	foundRemoteSamePort := false
	for _, n := range snap.Nodes {
		if strings.Contains(n.MaskedURL, "10.0.0.9:20003") {
			foundRemoteSamePort = true
		}
	}
	if !foundRemoteSamePort {
		t.Fatalf("非本机回环地址不应被按端口清理")
	}
	// 解绑后区域 fixed 回落为直连
	if b := snap.Regions["global"]; b.Mode != ModeDirect {
		t.Fatalf("解绑后区域出口应回落为直连，实际 %+v", b)
	}

	// 端口已不存在时为空操作
	if removed, unbound, err := m.RemoveNodesByLocalPorts([]int{20001}); err != nil || removed != 0 || unbound != 0 {
		t.Fatalf("重复清理应为空操作: removed=%d unbound=%d err=%v", removed, unbound, err)
	}
	if removed, unbound, err := m.RemoveNodesByLocalPorts(nil); err != nil || removed != 0 || unbound != 0 {
		t.Fatalf("空端口列表应为空操作: removed=%d unbound=%d err=%v", removed, unbound, err)
	}
}

func TestLocalLoopbackPort(t *testing.T) {
	cases := map[string]int{
		"http://127.0.0.1:20001":   20001,
		"http://localhost:20002":   20002,
		"http://[::1]:20003":       20003,
		"socks5://127.0.0.1:1080":  1080,
		"http://10.0.0.1:20001":    0,
		"http://example.com:20001": 0,
		"http://127.0.0.1":         0,
		"":                         0,
	}
	for raw, want := range cases {
		if got := localLoopbackPort(raw); got != want {
			t.Fatalf("localLoopbackPort(%q) = %d, 期望 %d", raw, got, want)
		}
	}
}
