package mihomo

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// SubNode 表示由单 mihomo 进程映射出的一个真实机场订阅节点及其独立本地回环端口。
type SubNode struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	DelayMS   int    `json:"delay"` // >0 表示测速毫秒，0 表示未测速，<0 表示超时/不可用
	CheckedAt string `json:"checked_at,omitempty"`
}

// ConfigOptions 描述生成单 mihomo 配置 YAML 所需的全部参数。
type ConfigOptions struct {
	MixedPort  int
	Controller string
	Secret     string
	Subs       []string
	Nodes      []SubNode
}

var fakeNodePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)剩余流量`),
	regexp.MustCompile(`(?i)套餐到期`),
	regexp.MustCompile(`(?i)到期时间`),
	regexp.MustCompile(`(?i)流量剩余`),
	regexp.MustCompile(`(?i)已用流量`),
	regexp.MustCompile(`(?i)过期`),
	regexp.MustCompile(`(?i)到期`),
	regexp.MustCompile(`(?i)重置`),
	regexp.MustCompile(`(?i)官网`),
	regexp.MustCompile(`(?i)网址`),
	regexp.MustCompile(`(?i)失联`),
	regexp.MustCompile(`(?i)备用域名`),
	regexp.MustCompile(`(?i)最新地址`),
	regexp.MustCompile(`(?i)联系客服`),
	regexp.MustCompile(`(?i)官方群`),
	regexp.MustCompile(`(?i)电报群`),
	regexp.MustCompile(`(?i)通知`),
	regexp.MustCompile(`(?i)公告`),
	regexp.MustCompile(`(?i)订阅`),
	regexp.MustCompile(`(?i)更新`),
	regexp.MustCompile(`(?i)购买`),
	regexp.MustCompile(`(?i)续费`),
	regexp.MustCompile(`(?i)套餐`),
	regexp.MustCompile(`(?i)余额`),
	regexp.MustCompile(`(?i)\btraffic\b`),
	regexp.MustCompile(`(?i)\bexpire[ds]?\b`),
	regexp.MustCompile(`(?i)\breset\b`),
	regexp.MustCompile(`(?i)\bwebsite\b`),
	regexp.MustCompile(`(?i)\bremaining\b`),
	regexp.MustCompile(`(?i)\bdays?\s*left\b`),
	regexp.MustCompile(`(?i)\bquota\b`),
	regexp.MustCompile(`(?i)\bnotice\b`),
	regexp.MustCompile(`(?i)^direct$`),
	regexp.MustCompile(`(?i)^reject$`),
	regexp.MustCompile(`(?i)^reject-drop$`),
	regexp.MustCompile(`(?i)^compatible$`),
	regexp.MustCompile(`(?i)^pass$`),
	regexp.MustCompile(`(?i)^global$`),
	regexp.MustCompile(`(?i)^login$`),
	regexp.MustCompile(`(?i)^proxy$`),
	regexp.MustCompile(`(?i)^match$`),
	regexp.MustCompile(`(?i)^auto$`),
	regexp.MustCompile(`(?i)^select$`),
	regexp.MustCompile(`(?i)^fallback$`),
	regexp.MustCompile(`(?i)^pub-\d+$`),
}

// IsFakeNode 判断节点名称是否属于机场提示信息假节点或内置策略组名称。
func IsFakeNode(name string) bool {
	s := strings.TrimSpace(name)
	if s == "" {
		return true
	}
	for _, re := range fakeNodePatterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// FilterRealNodes 过滤假节点与重复项，保留真实可用代理节点名称列表。
func FilterRealNodes(names []string) []string {
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		s := strings.TrimSpace(raw)
		if s == "" || IsFakeNode(s) {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// AssignStableNodePorts 为过滤后的真实节点分配独立且稳定的回环端口（默认 20001 起），
// 尽量保留已有节点端口号，新增节点自动顺延且跳过已占端口，避免端口冲突。
func AssignStableNodePorts(validNames []string, prevNodes []SubNode, basePort, maxNodes int) []SubNode {
	if basePort < 1024 || basePort > 60000 {
		basePort = 20001
	}
	if maxNodes <= 0 {
		maxNodes = 120
	}
	real := FilterRealNodes(validNames)
	if len(real) > maxNodes {
		real = real[:maxNodes]
	}

	realSet := make(map[string]struct{}, len(real))
	for _, n := range real {
		realSet[n] = struct{}{}
	}

	keptByName := make(map[string]SubNode, len(prevNodes))
	usedPorts := make(map[int]struct{}, len(prevNodes))
	for _, prev := range prevNodes {
		name := strings.TrimSpace(prev.Name)
		if name == "" {
			continue
		}
		if _, exists := realSet[name]; !exists {
			continue
		}
		if prev.Port >= 1024 && prev.Port <= 65535 {
			if _, used := usedPorts[prev.Port]; !used {
				keptByName[name] = prev
				usedPorts[prev.Port] = struct{}{}
			}
		}
	}

	nextPort := basePort
	out := make([]SubNode, 0, len(real))
	for _, name := range real {
		if prev, ok := keptByName[name]; ok {
			out = append(out, SubNode{
				Name:      name,
				Port:      prev.Port,
				DelayMS:   prev.DelayMS,
				CheckedAt: prev.CheckedAt,
			})
			continue
		}
		for {
			if _, used := usedPorts[nextPort]; !used {
				break
			}
			nextPort++
		}
		if nextPort > 65535 {
			break
		}
		usedPorts[nextPort] = struct{}{}
		out = append(out, SubNode{
			Name: name,
			Port: nextPort,
		})
		nextPort++
	}
	return out
}

// SortNodesByDelay 按测速结果排序：已测通（DelayMS > 0）按延迟升序排在前，未测速（0）居中，超时（< 0）排在末尾。
func SortNodesByDelay(nodes []SubNode) []SubNode {
	cp := make([]SubNode, len(nodes))
	copy(cp, nodes)
	sort.Slice(cp, func(i, j int) bool {
		di, dj := cp[i].DelayMS, cp[j].DelayMS
		iOk, jOk := di > 0, dj > 0
		if iOk && jOk {
			if di != dj {
				return di < dj
			}
		} else if iOk != jOk {
			return iOk
		} else if di != dj {
			return di > dj
		}
		return cp[i].Name < cp[j].Name
	})
	return cp
}

// EscapeRegex 转义正则表达式元字符（如 | . * + ? ^ $ ( ) [ ] { } \），用于精确匹配订阅节点名。
func EscapeRegex(s string) string {
	return regexp.QuoteMeta(s)
}

// YAMLQuote 将字符串包装为安全的 YAML 双引号字符串（先转义反斜杠与双引号）。
func YAMLQuote(s string) string {
	r := strings.ReplaceAll(s, `\`, `\\`)
	r = strings.ReplaceAll(r, `"`, `\"`)
	r = strings.ReplaceAll(r, "\r", "")
	r = strings.ReplaceAll(r, "\n", " ")
	return `"` + r + `"`
}

// BuildConfigYAML 生成防 502 的单 mihomo 进程配置：
// 1. 在 proxy-providers 段配置全部机场订阅链接（sub1..subN）；
// 2. 在 proxy-groups 段先为每个真实节点创建 PUB-<端口> 精确过滤组（use + filter: "^<正则转义节点名>$"）；
// 3. 在 listeners 段为每个节点创建仅监听 127.0.0.1 的 mixed 独立端口，并将 proxy 指向对应 PUB-<端口> 组名
//    （严禁直接填订阅节点名，避免 mihomo 启动阶段报 proxy not found 引发 502）。
func BuildConfigYAML(opts ConfigOptions) string {
	mixedPort := opts.MixedPort
	if mixedPort < 1024 || mixedPort > 65535 {
		mixedPort = 17890
	}
	controller := strings.TrimSpace(opts.Controller)
	if controller == "" {
		controller = "127.0.0.1:19090"
	}

	validSubs := make([]string, 0, len(opts.Subs))
	for _, raw := range opts.Subs {
		u := strings.TrimSpace(raw)
		low := strings.ToLower(u)
		if strings.HasPrefix(low, "http://") || strings.HasPrefix(low, "https://") {
			validSubs = append(validSubs, u)
		}
	}

	var b strings.Builder
	b.WriteString("# AutoClaw2API v1.1.0 自动生成（单 mihomo 订阅代理桥配置）\n")
	b.WriteString(fmt.Sprintf("mixed-port: %d\n", mixedPort))
	b.WriteString("bind-address: 127.0.0.1\n")
	b.WriteString("allow-lan: false\n")
	b.WriteString("mode: rule\n")
	b.WriteString("log-level: warning\n")
	b.WriteString(fmt.Sprintf("external-controller: %s\n", controller))
	b.WriteString(fmt.Sprintf("secret: %s\n", YAMLQuote(opts.Secret)))
	b.WriteString("unified-delay: true\n")

	if len(validSubs) > 0 {
		b.WriteString("proxy-providers:\n")
		for i, sub := range validSubs {
			b.WriteString(fmt.Sprintf("  sub%d:\n", i+1))
			b.WriteString("    type: http\n")
			b.WriteString(fmt.Sprintf("    url: %s\n", YAMLQuote(sub)))
			b.WriteString(fmt.Sprintf("    path: \"./providers/sub%d.yaml\"\n", i+1))
			b.WriteString("    interval: 86400\n")
			b.WriteString("    health-check:\n")
			b.WriteString("      enable: true\n")
			b.WriteString("      url: \"https://www.gstatic.com/generate_204\"\n")
			b.WriteString("      interval: 1800\n")
		}

		// 过滤并校验待映射的真实节点端口列表
		validListeners := make([]SubNode, 0, len(opts.Nodes))
		seenPorts := make(map[int]struct{}, len(opts.Nodes))
		for _, n := range opts.Nodes {
			name := strings.TrimSpace(n.Name)
			if name == "" || IsFakeNode(name) || n.Port < 1024 || n.Port > 65535 {
				continue
			}
			if _, dup := seenPorts[n.Port]; dup {
				continue
			}
			seenPorts[n.Port] = struct{}{}
			validListeners = append(validListeners, SubNode{Name: name, Port: n.Port})
		}

		// 先在 proxy-groups 段创建总组 PROXY 与每个节点的 PUB-<端口> 过滤组
		b.WriteString("proxy-groups:\n")
		b.WriteString("  - name: PROXY\n")
		b.WriteString("    type: select\n")
		b.WriteString("    use:\n")
		for i := range validSubs {
			b.WriteString(fmt.Sprintf("      - sub%d\n", i+1))
		}
		for _, item := range validListeners {
			groupName := fmt.Sprintf("PUB-%d", item.Port)
			b.WriteString(fmt.Sprintf("  - name: %s\n", groupName))
			b.WriteString("    type: select\n")
			b.WriteString("    use:\n")
			for i := range validSubs {
				b.WriteString(fmt.Sprintf("      - sub%d\n", i+1))
			}
			b.WriteString(fmt.Sprintf("    filter: %s\n", YAMLQuote("^"+EscapeRegex(item.Name)+"$")))
		}

		// 再在 listeners 段将独立回环 mixed 端口指向对应 PUB-<端口> 组名
		if len(validListeners) > 0 {
			b.WriteString("listeners:\n")
			for _, item := range validListeners {
				groupName := fmt.Sprintf("PUB-%d", item.Port)
				b.WriteString(fmt.Sprintf("  - name: pub-%d\n", item.Port))
				b.WriteString("    type: mixed\n")
				b.WriteString(fmt.Sprintf("    port: %d\n", item.Port))
				b.WriteString("    listen: 127.0.0.1\n")
				b.WriteString(fmt.Sprintf("    proxy: %s\n", YAMLQuote(groupName)))
			}
		}
	}

	b.WriteString("rules:\n")
	b.WriteString("  - MATCH,DIRECT\n")
	return b.String()
}
