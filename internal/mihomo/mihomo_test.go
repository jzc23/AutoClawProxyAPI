package mihomo

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func buildTestZip(t *testing.T, innerName string, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(innerName)
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func buildTestGzip(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(payload); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestDownloadMirrorFirstZipAndDirectFallbackGzip(t *testing.T) {
	wantWin := []byte("FAKE-MIHOMO-WINDOWS-BINARY-V11929")
	zipBytes := buildTestZip(t, "mihomo-windows-amd64.exe", wantWin)

	var mirrorHits, directHits atomic.Int32
	mirrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mirrorHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(zipBytes)
	}))
	defer mirrorSrv.Close()

	directSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		directHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(zipBytes)
	}))
	defer directSrv.Close()

	// 1. 镜像成功时优先走镜像（绝不打直连），并正确解压 .zip + 原子落盘 + 清理临时文件
	dir1 := t.TempDir()
	target1 := filepath.Join(dir1, "mihomo.exe")
	src1, err := downloadAndExtract(context.Background(), mirrorSrv.Client(), mirrorSrv.URL+"/ok.zip", directSrv.URL+"/ok.zip", "zip", target1, nil)
	if err != nil {
		t.Fatalf("镜像下载 ZIP 失败: %v", err)
	}
	if src1 != "mirror" || mirrorHits.Load() != 1 || directHits.Load() != 0 {
		t.Fatalf("期望仅走镜像源: src=%s mirror=%d direct=%d", src1, mirrorHits.Load(), directHits.Load())
	}
	got1, err := os.ReadFile(target1)
	if err != nil || !bytes.Equal(got1, wantWin) {
		t.Fatalf("ZIP 解压落盘内容不符: err=%v got=%q", err, string(got1))
	}
	if _, err := os.Stat(target1 + ".part"); !os.IsNotExist(err) {
		t.Fatalf("下载完成后不应残留 .part 文件")
	}

	// 2. 镜像返回 502 时自动回落直连，并正确解压 Linux .gz 归档
	wantLinux := []byte("FAKE-MIHOMO-LINUX-ELF-BINARY-V11929")
	gzBytes := buildTestGzip(t, wantLinux)

	failMirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("mirror down"))
	}))
	defer failMirror.Close()

	okDirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(gzBytes)
	}))
	defer okDirect.Close()

	dir2 := t.TempDir()
	target2 := filepath.Join(dir2, "mihomo")
	src2, err := downloadAndExtract(context.Background(), okDirect.Client(), failMirror.URL+"/fail.gz", okDirect.URL+"/ok.gz", "gz", target2, nil)
	if err != nil {
		t.Fatalf("直连兜底下载 GZ 失败: %v", err)
	}
	if src2 != "direct" {
		t.Fatalf("期望回落至 direct，实际 %s", src2)
	}
	got2, err := os.ReadFile(target2)
	if err != nil || !bytes.Equal(got2, wantLinux) {
		t.Fatalf("GZ 解压落盘内容不符: err=%v got=%q", err, string(got2))
	}

	// 3. 均失败时必须自动清理 .part 与 .tmp 临时文件
	target3 := filepath.Join(t.TempDir(), "mihomo")
	if _, err := downloadAndExtract(context.Background(), failMirror.Client(), failMirror.URL+"/a", failMirror.URL+"/b", "gz", target3, nil); err == nil {
		t.Fatalf("双源失败时应返回错误")
	}
	if _, err := os.Stat(target3 + ".part"); !os.IsNotExist(err) {
		t.Fatalf("失败后应清理 .part")
	}
	if _, err := os.Stat(target3 + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("失败后应清理 .tmp")
	}
}

func TestAssetNameAndEnvVersionOverride(t *testing.T) {
	t.Setenv("MIHOMO_VERSION", "v1.19.30")
	if v := ResolveVersion(); v != "v1.19.30" {
		t.Fatalf("MIHOMO_VERSION 覆盖失效: %q", v)
	}
	assetW, typW, err := AssetName("windows", "amd64", "v1.19.29")
	if err != nil || assetW != "mihomo-windows-amd64-v1.19.29.zip" || typW != "zip" {
		t.Fatalf("Windows 资产名不符: asset=%q type=%q err=%v", assetW, typW, err)
	}
	assetL, typL, err := AssetName("linux", "arm64", "v1.19.29")
	if err != nil || assetL != "mihomo-linux-arm64-v1.19.29.gz" || typL != "gz" {
		t.Fatalf("Linux 资产名不符: asset=%q type=%q err=%v", assetL, typL, err)
	}
	mURL, dURL, _, _, err := BuildDownloadURLs("linux", "amd64", "v1.19.29", "", "")
	if err != nil {
		t.Fatalf("BuildDownloadURLs: %v", err)
	}
	wantDirect := "https://github.com/MetaCubeX/mihomo/releases/download/v1.19.29/mihomo-linux-amd64-v1.19.29.gz"
	if dURL != wantDirect || mURL != "https://ghfast.top/"+wantDirect {
		t.Fatalf("下载地址拼接不符:\nmirror=%s\ndirect=%s", mURL, dURL)
	}
}

func TestFilterFakeNodesAndAnti502PubGroupYAML(t *testing.T) {
	raw := []string{
		"剩余流量：88.5 GB",
		"套餐到期：2026-12-31",
		"官网地址：https://sub.example.com",
		"距离下次重置还有 12 天",
		"Traffic: 99GB left",
		"Expire: 2026-12-31",
		"DIRECT",
		"REJECT",
		"GLOBAL",
		"LOGIN",
		"PROXY",
		"PUB-20001",
		`🇯🇵 日本 | 01 (AWS) [x1.5]`,
		`🇸🇬 新加坡 "专线" +02`,
		`🇯🇵 日本 | 01 (AWS) [x1.5]`, // 重复去重
	}
	real := FilterRealNodes(raw)
	if len(real) != 2 {
		t.Fatalf("期望过滤后保留 2 个真实节点，实际 %d: %v", len(real), real)
	}
	assigned := AssignStableNodePorts(real, nil, 20001, 50)
	if len(assigned) != 2 || assigned[0].Port != 20001 || assigned[1].Port != 20002 {
		t.Fatalf("端口分配异常: %+v", assigned)
	}

	yml := BuildConfigYAML(ConfigOptions{
		MixedPort:  17890,
		Controller: "127.0.0.1:19090",
		Secret:     "sec123",
		Subs:       []string{"https://airport.example/sub?token=abc"},
		Nodes:      assigned,
	})

	idxGroups := strings.Index(yml, "proxy-groups:")
	idxListeners := strings.Index(yml, "listeners:")
	if idxGroups < 0 || idxListeners < 0 || idxGroups > idxListeners {
		t.Fatalf("proxy-groups 必须位于 listeners 之前:\n%s", yml)
	}
	// 验证 PUB-20001 过滤组存在，且对 | ( ) [ ] . 等正则元字符先转义再 YAML 双引号转义
	wantFilter1 := `filter: "^🇯🇵 日本 \\| 01 \\(AWS\\) \\[x1\\.5\\]$"`
	if !strings.Contains(yml, "- name: PUB-20001") || !strings.Contains(yml, wantFilter1) {
		t.Fatalf("PUB-20001 过滤组或正则转义缺失:\n期望包含: %s\n实际 YAML:\n%s", wantFilter1, yml)
	}
	// 验证含双引号和 + 的节点名同样安全转义
	wantFilter2 := `filter: "^🇸🇬 新加坡 \"专线\" \\+02$"`
	if !strings.Contains(yml, "- name: PUB-20002") || !strings.Contains(yml, wantFilter2) {
		t.Fatalf("PUB-20002 双引号与正则转义不符:\n期望包含: %s\n实际 YAML:\n%s", wantFilter2, yml)
	}
	// 验证 listeners 的 proxy 指向 PUB-<端口> 而非原始节点名，且仅监听 127.0.0.1
	if !strings.Contains(yml, `proxy: "PUB-20001"`) || !strings.Contains(yml, "listen: 127.0.0.1") || !strings.Contains(yml, "type: mixed") {
		t.Fatalf("listeners 防 502 配置不符合规范:\n%s", yml)
	}
}

func TestPickAndPeekLoginNodeRoundRobinSkipsTimeout(t *testing.T) {
	mgr, err := NewManager(t.TempDir(), t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mgr.SetNodesForTest(true, []SubNode{
		{Name: "Node-US", Port: 20003, DelayMS: 120},
		{Name: "Node-Dead", Port: 20004, DelayMS: -1},
		{Name: "Node-JP", Port: 20001, DelayMS: 40},
		{Name: "Node-SG", Port: 20002, DelayMS: 65},
	})

	// Peek 不推进游标，连续预览始终返回当前待用节点
	p1, ok := mgr.PeekNextLoginNode()
	p2, _ := mgr.PeekNextLoginNode()
	if !ok || p1.Name != "Node-JP" || p2.Name != "Node-JP" {
		t.Fatalf("PeekNextLoginNode 异常: p1=%+v p2=%+v", p1, p2)
	}

	// 连续 Pick 按延迟升序依次轮换健康节点，并严格跳过超时节点 Node-Dead
	wantSeq := []string{"Node-JP", "Node-SG", "Node-US", "Node-JP"}
	for i, want := range wantSeq {
		got, ok := mgr.PickNextLoginNode()
		if !ok || got.Name != want {
			t.Fatalf("第 %d 次 Pick 期望 %s，实际 %+v (ok=%v)", i+1, want, got, ok)
		}
	}
}
