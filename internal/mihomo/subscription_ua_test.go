package mihomo

import (
	"strings"
	"testing"
)

func TestBuildConfigYAMLUsesNonClashUserAgent(t *testing.T) {
	yml := BuildConfigYAML(ConfigOptions{Subs: []string{"https://example.com/sub?token=a"}})
	if !strings.Contains(yml, "User-Agent: [\""+SubscriptionUserAgent+"\"]") {
		t.Fatalf("订阅 provider 缺少 User-Agent 头:\n%s", yml)
	}
	// Clash 系 UA 会触发机场/中转站的订阅转换后端，后端故障时整条订阅 400/502/504。
	if low := strings.ToLower(SubscriptionUserAgent); strings.Contains(low, "clash") || strings.Contains(low, "mihomo") {
		t.Fatalf("订阅 UA 不应使用 Clash 系标识（会触发订阅转换后端）: %q", SubscriptionUserAgent)
	}
}

func TestBuildConfigYAMLCachePathFollowsSubscriptionURL(t *testing.T) {
	a := BuildConfigYAML(ConfigOptions{Subs: []string{"https://a.example.com/sub"}})
	b := BuildConfigYAML(ConfigOptions{Subs: []string{"https://b.example.com/sub"}})
	pathOf := func(s string) string {
		for _, line := range strings.Split(s, "\n") {
			if strings.Contains(line, "path:") {
				return line
			}
		}
		return ""
	}
	if pathOf(a) == "" || pathOf(a) == pathOf(b) {
		t.Fatalf("更换订阅链接后缓存路径应变化: %q vs %q", pathOf(a), pathOf(b))
	}
}
