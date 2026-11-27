package mihomo

import (
	"context"
	"strings"
	"testing"
)

func TestFilterExcludedNames(t *testing.T) {
	names := []string{"Node-A", "Node-B", "Node-C"}
	got := FilterExcludedNames(names, []string{"Node-B", " Node-C "})
	if len(got) != 1 || got[0] != "Node-A" {
		t.Fatalf("排除过滤异常: %v", got)
	}
	if all := FilterExcludedNames(names, nil); len(all) != 3 {
		t.Fatalf("无排除名单时应原样返回: %v", all)
	}
}

func TestPruneExcludedNamesDropsStaleEntries(t *testing.T) {
	excluded := []string{"Old-Node-1", " Node-Current ", "Old-Node-2"}
	got := PruneExcludedNames(excluded, []string{"Node-Current", "Other"})
	if len(got) != 1 || got[0] != " Node-Current " {
		t.Fatalf("只应保留当前订阅中仍存在的排除记录: %v", got)
	}
	if kept := PruneExcludedNames(nil, []string{"x"}); kept != nil {
		t.Fatalf("空排除名单应原样返回: %v", kept)
	}
}

func TestNodesByNameKeepsOrderAndSkipsMissing(t *testing.T) {
	mgr, err := NewManager(t.TempDir(), t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetNodesForTest(true, []SubNode{
		{Name: "Node-A", Port: 20001},
		{Name: "Node-B", Port: 20002},
	})
	got := mgr.NodesByName([]string{"Node-B", "Node-Nope", " Node-A ", "Node-B"})
	if len(got) != 2 || got[0].Name != "Node-B" || got[0].Port != 20002 || got[1].Name != "Node-A" {
		t.Fatalf("按名称取节点应保持入参顺序并跳过缺失项: %+v", got)
	}
}

func TestDeleteNodesPersistsExclusionAndSkipsResolve(t *testing.T) {
	dataDir := t.TempDir()
	mgr, err := NewManager(t.TempDir(), dataDir, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetNodesForTest(true, []SubNode{
		{Name: "Node-A", Port: 20001, DelayMS: 40},
		{Name: "Node-B", Port: 20002, DelayMS: 80},
		{Name: "Node-C", Port: 20003, DelayMS: 120},
	})

	if _, err := mgr.DeleteNodes(context.Background(), []string{"Node-B"}); err != nil {
		t.Fatalf("DeleteNodes: %v", err)
	}
	snap := mgr.Snapshot()
	if snap.TotalCount != 2 || snap.ExcludedCount != 1 {
		t.Fatalf("删除后快照不符: total=%d excluded=%d", snap.TotalCount, snap.ExcludedCount)
	}
	for _, n := range snap.Nodes {
		if n.Name == "Node-B" {
			t.Fatalf("Node-B 应已从节点列表移除")
		}
	}
	// 剩余节点端口保持稳定，不因删除而重排
	if snap.Nodes[0].Name != "Node-A" || snap.Nodes[0].Port != 20001 {
		t.Fatalf("剩余节点端口应保持不变: %+v", snap.Nodes[0])
	}
	// 被删节点不再参与登录节点解析
	if _, _, err := mgr.ResolveNodeProxyURL("Node-B"); err == nil {
		t.Fatalf("被删除节点不应还能解析出代理地址")
	}
	if _, _, err := mgr.ResolveNodeProxyURL("Node-A"); err != nil {
		t.Fatalf("未删除节点应可正常解析: %v", err)
	}

	// 删除不存在的节点应报错且不改动状态；混入的不存在名称不应写入排除名单
	if _, err := mgr.DeleteNodes(context.Background(), []string{"Node-Nope"}); err == nil {
		t.Fatalf("删除不存在节点应返回错误")
	}
	if _, err := mgr.DeleteNodes(context.Background(), nil); err == nil {
		t.Fatalf("空选择应返回错误")
	}
	if snap := mgr.Snapshot(); snap.ExcludedCount != 1 || len(snap.ExcludedNodes) != 1 || snap.ExcludedNodes[0] != "Node-B" {
		t.Fatalf("排除名单应只包含真实删除的节点: %+v", snap.ExcludedNodes)
	}

	// 落盘后重新加载：节点列表与排除名单都应保留
	reloaded, err := NewManager(t.TempDir(), dataDir, nil)
	if err != nil {
		t.Fatalf("重新加载 Manager: %v", err)
	}
	rs := reloaded.Snapshot()
	if rs.TotalCount != 2 || rs.ExcludedCount != 1 {
		t.Fatalf("重启后状态未持久化: total=%d excluded=%d", rs.TotalCount, rs.ExcludedCount)
	}

	// 恢复：排除名单清空（无订阅时不会重新同步，节点等下次同步回来）
	if _, _, err := reloaded.RestoreExcludedNodes(context.Background()); err != nil {
		t.Fatalf("RestoreExcludedNodes: %v", err)
	}
	if rs := reloaded.Snapshot(); rs.ExcludedCount != 0 {
		t.Fatalf("恢复后排除名单应为空: %d", rs.ExcludedCount)
	}
	if _, _, err := reloaded.RestoreExcludedNodes(context.Background()); err == nil {
		t.Fatalf("无删除记录时恢复应返回错误")
	}
}

func TestDeleteAllNodesStopsAndClearsListeners(t *testing.T) {
	mgr, err := NewManager(t.TempDir(), t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetNodesForTest(true, []SubNode{
		{Name: "Only-1", Port: 20001},
		{Name: "Only-2", Port: 20002},
	})
	snap, err := mgr.DeleteNodes(context.Background(), []string{"Only-1", "Only-2"})
	if err != nil {
		t.Fatalf("DeleteNodes: %v", err)
	}
	if snap.TotalCount != 0 || snap.ExcludedCount != 2 {
		t.Fatalf("全部删除后快照不符: total=%d excluded=%d", snap.TotalCount, snap.ExcludedCount)
	}
	if p, ok := mgr.PeekNextLoginNode(); ok {
		t.Fatalf("无节点时不应返回登录节点: %+v", p)
	}
}

func TestBuildConfigYAMLOnlyContainsRemainingNodes(t *testing.T) {
	nodes := AssignStableNodePorts([]string{"Node-A", "Node-C"}, nil, 20001, 100)
	yml := BuildConfigYAML(ConfigOptions{
		MixedPort:  17890,
		Controller: "127.0.0.1:19090",
		Secret:     "sec",
		Subs:       []string{"https://airport.example/sub?token=abc"},
		Nodes:      nodes,
	})
	if strings.Contains(yml, "Node-B") {
		t.Fatalf("已删除节点不应出现在配置中:\n%s", yml)
	}
	if got := strings.Count(yml, "\n  - name: pub-"); got != 2 {
		t.Fatalf("listeners 应只包含剩余 2 个节点，实际 %d", got)
	}
}
