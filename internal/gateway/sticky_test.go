package gateway

import (
	"fmt"
	"strings"
	"testing"

	"autoclaw2api/internal/protocol"
)

func TestSessionKeyForChatBodyFallbackHash(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"system","content":"You are a coding assistant."},{"role":"user","content":"hi"}]}`)
	k1 := sessionKeyForChatBody("m", body)
	if k1 == "" || !strings.Contains(k1, "|h:") {
		t.Fatalf("无显式标识时应回退哈希键，实际: %q", k1)
	}
	if again := sessionKeyForChatBody("m", body); again != k1 {
		t.Fatalf("相同请求体哈希键应稳定: %q vs %q", k1, again)
	}
	// 多轮对话只在尾部追加消息，首条 system 前缀不变 → 键保持稳定（Prompt Cache 生效的前提）
	nextTurn := []byte(`{"model":"m","messages":[{"role":"system","content":"You are a coding assistant."},{"role":"user","content":"hi"},{"role":"assistant","content":"回复"},{"role":"user","content":"第二轮"}]}`)
	if got := sessionKeyForChatBody("m", nextTurn); got != k1 {
		t.Fatalf("多轮追加消息不应改变粘性键: %q vs %q", k1, got)
	}
}

func TestSessionKeyForChatBodyExplicitKey(t *testing.T) {
	body := []byte(`{"model":"m","prompt_cache_key":"conv-123","messages":[{"role":"user","content":"a"}]}`)
	if got := sessionKeyForChatBody("m", body); !strings.Contains(got, "|e:conv-123") {
		t.Fatalf("显式 prompt_cache_key 应优先: %q", got)
	}
	mdBody := []byte(`{"model":"m","metadata":{"session_id":"sess-9"},"messages":[{"role":"user","content":"a"}]}`)
	if got := sessionKeyForChatBody("m", mdBody); !strings.Contains(got, "|e:sess-9") {
		t.Fatalf("metadata.session_id 应作为显式键: %q", got)
	}
	if got := sessionKeyForChatBody("m", []byte("not-json")); got != "" {
		t.Fatalf("非法 JSON 应返回空键，实际: %q", got)
	}
}

func TestStickyTableBindLookupUnbindAndEviction(t *testing.T) {
	tbl := newStickyTable()
	tbl.bind("k1", "acc-1", protocol.RegionCN)
	if id, _, ok := tbl.lookup("k1"); !ok || id != "acc-1" {
		t.Fatalf("绑定后应可查询: ok=%v id=%q", ok, id)
	}
	tbl.unbind("k1")
	if _, _, ok := tbl.lookup("k1"); ok {
		t.Fatal("解绑后不应可查询")
	}
	if _, _, ok := tbl.lookup(""); ok {
		t.Fatal("空键不应可查询")
	}

	// 容量上限：超量写入触发惰性淘汰，最终不超过上限
	for i := 0; i < stickyMaxEntries+100; i++ {
		tbl.bind(fmt.Sprintf("key-%d", i), "acc", protocol.RegionCN)
	}
	tbl.mu.Lock()
	size := len(tbl.entries)
	tbl.mu.Unlock()
	if size > stickyMaxEntries {
		t.Fatalf("绑定表应惰性淘汰至容量上限内，实际 %d > %d", size, stickyMaxEntries)
	}
	tbl.bind("fresh", "acc-new", protocol.RegionGlobal)
	if id, region, ok := tbl.lookup("fresh"); !ok || id != "acc-new" || region != protocol.RegionGlobal {
		t.Fatalf("淘汰后新绑定应可查询并保留区域: ok=%v id=%q region=%q", ok, id, region)
	}
}
