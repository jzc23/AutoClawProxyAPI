package mihomo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/logging"
)

const (
	defaultBasePort   = 20001
	defaultMixedPort  = 17890
	defaultController = "127.0.0.1:19090"
	probeConcurrency  = 8
)

type bridgeDiskState struct {
	Enabled              bool      `json:"enabled"`
	Subscriptions        []string  `json:"subscriptions"`
	BasePort             int       `json:"base_port"`
	MaxNodes             int       `json:"max_nodes"`
	MixedPort            int       `json:"mixed_port"`
	Controller           string    `json:"controller"`
	Secret               string    `json:"secret"`
	AutoProbeEnabled     bool      `json:"auto_probe_enabled"`
	AutoProbeIntervalMin int       `json:"auto_probe_interval_min"`
	LastProbeAt          string    `json:"last_probe_at,omitempty"`
	Nodes                []SubNode `json:"nodes"`
	// ExcludedNodes 保存被用户删除、需要长期排除的节点名称，重新同步订阅时不再映射。
	ExcludedNodes []string `json:"excluded_nodes"`
}

// Snapshot 描述面向 Web 控制台展示的 mihomo 登录节点桥实时快照。
type Snapshot struct {
	BinaryFound          bool          `json:"binary_found"`
	BinaryPath           string        `json:"binary_path"`
	TargetVersion        string        `json:"target_version"`
	Platform             string        `json:"platform"`
	Running              bool          `json:"running"`
	Enabled              bool          `json:"enabled"`
	Subscriptions        []string      `json:"subscriptions"`
	BasePort             int           `json:"base_port"`
	MaxNodes             int           `json:"max_nodes"`
	AutoProbeEnabled     bool          `json:"auto_probe_enabled"`
	AutoProbeIntervalMin int           `json:"auto_probe_interval_min"`
	LastProbeAt          string        `json:"last_probe_at,omitempty"`
	Probing              bool          `json:"probing"`
	FilteredFakeCount    int           `json:"filtered_fake_count"`
	HealthyCount         int           `json:"healthy_count"`
	TotalCount           int           `json:"total_count"`
	NextLoginNode        string        `json:"next_login_node,omitempty"`
	Nodes                []SubNode     `json:"nodes"`
	ExcludedCount        int           `json:"excluded_count"`
	ExcludedNodes        []string      `json:"excluded_nodes"`
	Download             DownloadState `json:"download"`
	LastError            string        `json:"last_error,omitempty"`
}

// Manager 管理单 mihomo 内核下载、子进程生命周期、登录节点池映射与定时测速。
type Manager struct {
	mu           sync.RWMutex
	baseDir      string
	dataDir      string
	workDir      string
	statePath    string
	logger       *logging.Logger
	state        bridgeDiskState
	download     DownloadState
	filteredFake int
	lastError    string
	loginRR      uint64

	procMu   sync.Mutex
	cmd      *exec.Cmd
	cmdDone  chan struct{}
	probing  bool
	probeMu  sync.Mutex
	wakeCh   chan struct{}
	httpCtl  *http.Client
	dlClient *http.Client

	mirrorPrefix string
	githubBase   string
}

// NewManager 初始化 mihomo 订阅代理桥管理器（启动时自动探测程序目录下是否已存在 mihomo 内核）。
func NewManager(baseDir, dataDir string, logger *logging.Logger) (*Manager, error) {
	if strings.TrimSpace(baseDir) == "" {
		if wd, err := os.Getwd(); err == nil {
			baseDir = wd
		} else {
			baseDir = "."
		}
	}
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "./data"
	}
	workDir := filepath.Join(dataDir, "mihomo")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建 mihomo 工作目录失败: %w", err)
	}

	m := &Manager{
		baseDir:      baseDir,
		dataDir:      dataDir,
		workDir:      workDir,
		statePath:    filepath.Join(dataDir, "mihomo_bridge.json"),
		logger:       logger,
		wakeCh:       make(chan struct{}, 1),
		httpCtl:      &http.Client{Timeout: 30 * time.Second},
		dlClient:     &http.Client{Timeout: 8 * time.Minute},
		mirrorPrefix: DefaultMirrorPrefix,
		githubBase:   DefaultGitHubReleaseBase,
		download: DownloadState{
			Status:  "idle",
			Version: ResolveVersion(),
		},
		state: bridgeDiskState{
			Enabled:              false,
			Subscriptions:        []string{"", "", ""},
			BasePort:             defaultBasePort,
			MaxNodes:             DefaultMaxNodes,
			MixedPort:            defaultMixedPort,
			Controller:           defaultController,
			Secret:               randomHex(8),
			AutoProbeEnabled:     true,
			AutoProbeIntervalMin: 10,
			Nodes:                []SubNode{},
			ExcludedNodes:        []string{},
		},
	}
	m.loadState()
	return m, nil
}

func randomHex(nBytes int) string {
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func normalizeSubs(subs []string) []string {
	out := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		if i < len(subs) {
			out = append(out, strings.TrimSpace(subs[i]))
		} else {
			out = append(out, "")
		}
	}
	return out
}

func hasValidSub(subs []string) bool {
	for _, s := range subs {
		low := strings.ToLower(strings.TrimSpace(s))
		if strings.HasPrefix(low, "http://") || strings.HasPrefix(low, "https://") {
			return true
		}
	}
	return false
}

func normalizeProbeMin(v int) int {
	if v < 1 {
		return 10
	}
	if v > 1440 {
		return 1440
	}
	return v
}

// scaledStartWait 按节点规模放宽 mihomo 启动等待时间（每个监听端口都会增加配置解析与监听初始化耗时）。
func scaledStartWait(nodeCount int) time.Duration {
	d := 10*time.Second + time.Duration(nodeCount)*20*time.Millisecond
	if d > 2*time.Minute {
		d = 2 * time.Minute
	}
	return d
}

// scaledProbeTimeout 按节点规模放宽整体测速超时，避免大订阅只完成一小部分节点测速。
func scaledProbeTimeout(nodeCount int) time.Duration {
	d := 60*time.Second + time.Duration(nodeCount)*300*time.Millisecond
	if d > 20*time.Minute {
		d = 20 * time.Minute
	}
	return d
}

func (m *Manager) loadState() {
	raw, err := os.ReadFile(m.statePath)
	if err != nil {
		return
	}
	var st bridgeDiskState
	if err := json.Unmarshal(raw, &st); err != nil {
		return
	}
	st.Subscriptions = normalizeSubs(st.Subscriptions)
	if st.BasePort < 1024 || st.BasePort > 60000 {
		st.BasePort = defaultBasePort
	}
	if st.MaxNodes <= 0 {
		st.MaxNodes = DefaultMaxNodes
	}
	if st.MaxNodes > MaxNodesLimit {
		st.MaxNodes = MaxNodesLimit
	}
	if st.MixedPort < 1024 || st.MixedPort > 65535 {
		st.MixedPort = defaultMixedPort
	}
	if strings.TrimSpace(st.Controller) == "" {
		st.Controller = defaultController
	}
	if strings.TrimSpace(st.Secret) == "" {
		st.Secret = randomHex(8)
	}
	st.AutoProbeIntervalMin = normalizeProbeMin(st.AutoProbeIntervalMin)
	if st.Nodes == nil {
		st.Nodes = []SubNode{}
	}
	if st.ExcludedNodes == nil {
		st.ExcludedNodes = []string{}
	}
	st.Nodes = SortNodesByDelay(st.Nodes)
	m.state = st
}

func (m *Manager) saveStateLocked() error {
	if err := os.MkdirAll(filepath.Dir(m.statePath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.statePath)
}

// Start 启动 mihomo 代理桥后台守护（如果已存在内核且配置了订阅则自动拉起进程并启动定时测速协程）。
func (m *Manager) Start(ctx context.Context) {
	binPath, found := DetectBinary(m.baseDir)
	if m.logger != nil {
		if found {
			m.logger.Info("Mihomo", "已探测到 mihomo 内核: %s", binPath)
		} else {
			m.logger.Info("Mihomo", "程序目录未发现 %s，可在控制台「代理管理」一键在线下载", BinaryName(runtime.GOOS))
		}
	}

	m.mu.RLock()
	shouldBoot := found && m.state.Enabled && hasValidSub(m.state.Subscriptions)
	m.mu.RUnlock()

	if shouldBoot {
		m.mu.RLock()
		nodeCnt := len(m.state.Nodes)
		m.mu.RUnlock()
		bootWait := 25 * time.Second
		if nodeCnt > 0 {
			bootWait = scaledStartWait(nodeCnt) + 5*time.Second
		}
		go func() {
			bootCtx, cancel := context.WithTimeout(ctx, bootWait)
			defer cancel()
			if err := m.ensureProcessRunning(bootCtx); err != nil {
				if m.logger != nil {
					m.logger.Warn("Mihomo", "启动 mihomo 进程失败: %v", err)
				}
				return
			}
			// 若已有节点列表则延迟测一次速，否则拉取一次订阅节点
			if nodeCnt == 0 {
				_, _ = m.RefreshSubscriptions(bootCtx)
			} else {
				probeCtx, pCancel := context.WithTimeout(ctx, scaledProbeTimeout(nodeCnt))
				defer pCancel()
				_, _ = m.ProbeAllNodes(probeCtx)
			}
		}()
	}

	// 退出时优雅关闭 mihomo 子进程，避免孤儿进程残留
	go func() {
		<-ctx.Done()
		m.Stop()
	}()

	// 后台定时测速协程
	go func() {
		for {
			m.mu.RLock()
			intervalMin := normalizeProbeMin(m.state.AutoProbeIntervalMin)
			m.mu.RUnlock()

			timer := time.NewTimer(time.Duration(intervalMin) * time.Minute)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-m.wakeCh:
				timer.Stop()
				continue
			case <-timer.C:
				m.mu.RLock()
				en := m.state.Enabled && m.state.AutoProbeEnabled && len(m.state.Nodes) > 0
				nodeCnt := len(m.state.Nodes)
				m.mu.RUnlock()
				if en && m.IsRunning() {
					probeCtx, cancel := context.WithTimeout(ctx, scaledProbeTimeout(nodeCnt))
					_, _ = m.ProbeAllNodes(probeCtx)
					cancel()
				}
			}
		}
	}()
}

// Stop 立即停止正在运行的 mihomo 子进程。
func (m *Manager) Stop() {
	m.procMu.Lock()
	cmd := m.cmd
	done := m.cmdDone
	m.cmd = nil
	m.cmdDone = nil
	m.procMu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		}
	}
}

// IsRunning 检测 mihomo 控制端口是否正常响应。
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	ctl := m.state.Controller
	secret := m.state.Secret
	m.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	status, _, err := m.controllerRequest(ctx, http.MethodGet, "/version", nil, ctl, secret)
	return err == nil && (status == http.StatusOK || status == http.StatusUnauthorized)
}

// Snapshot 返回内核探测、在线下载进度、运行状态与排序后的机场节点列表。
func (m *Manager) Snapshot() Snapshot {
	binPath, found := DetectBinary(m.baseDir)
	running := m.IsRunning()

	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := SortNodesByDelay(m.state.Nodes)
	healthy := 0
	for _, n := range nodes {
		if n.DelayMS > 0 {
			healthy++
		}
	}
	nextNodeName := ""
	if nextNode, ok := m.peekNextLoginNodeLocked(); ok {
		nextNodeName = nextNode.Name
	}
	return Snapshot{
		BinaryFound:          found,
		BinaryPath:           binPath,
		TargetVersion:        ResolveVersion(),
		Platform:             runtime.GOOS + "/" + runtime.GOARCH,
		Running:              running,
		Enabled:              m.state.Enabled,
		Subscriptions:        append([]string(nil), m.state.Subscriptions...),
		BasePort:             m.state.BasePort,
		MaxNodes:             m.state.MaxNodes,
		AutoProbeEnabled:     m.state.AutoProbeEnabled,
		AutoProbeIntervalMin: m.state.AutoProbeIntervalMin,
		LastProbeAt:          m.state.LastProbeAt,
		Probing:              m.probing,
		FilteredFakeCount:    m.filteredFake,
		HealthyCount:         healthy,
		TotalCount:           len(nodes),
		NextLoginNode:        nextNodeName,
		Nodes:                nodes,
		ExcludedCount:        len(m.state.ExcludedNodes),
		ExcludedNodes:        append([]string(nil), m.state.ExcludedNodes...),
		Download:             m.download,
		LastError:            m.lastError,
	}
}

func (m *Manager) loginCandidatesLocked() []SubNode {
	if !m.state.Enabled || len(m.state.Nodes) == 0 {
		return nil
	}
	sorted := SortNodesByDelay(m.state.Nodes)
	healthy := make([]SubNode, 0, len(sorted))
	untested := make([]SubNode, 0, len(sorted))
	for _, n := range sorted {
		if n.Port < 1024 || n.Port > 65535 || strings.TrimSpace(n.Name) == "" {
			continue
		}
		if n.DelayMS > 0 {
			healthy = append(healthy, n)
		} else if n.DelayMS == 0 {
			untested = append(untested, n)
		}
		// DelayMS < 0（明确超时/不可用节点）严格跳过
	}
	if len(healthy) > 0 {
		return healthy
	}
	return untested
}

func (m *Manager) peekNextLoginNodeLocked() (SubNode, bool) {
	cands := m.loginCandidatesLocked()
	if len(cands) == 0 {
		return SubNode{}, false
	}
	idx := m.loginRR % uint64(len(cands))
	return cands[idx], true
}

// PeekNextLoginNode 预览下一次自动轮换将会选中的健康登录节点（不推进轮询游标，供界面展示与 captcha 阶段复用）。
func (m *Manager) PeekNextLoginNode() (SubNode, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.peekNextLoginNodeLocked()
}

// PickNextLoginNode 按轮询顺序挑选下一个不同的健康登录节点并推进游标（用于发起新一次短信/OAuth 登录会话）。
func (m *Manager) PickNextLoginNode() (SubNode, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cands := m.loginCandidatesLocked()
	if len(cands) == 0 {
		return SubNode{}, false
	}
	idx := m.loginRR % uint64(len(cands))
	m.loginRR++
	return cands[idx], true
}

// SetNodesForTest 仅供单元测试注入节点与启用状态。
func (m *Manager) SetNodesForTest(enabled bool, nodes []SubNode) {
	m.mu.Lock()
	m.state.Enabled = enabled
	m.state.Nodes = SortNodesByDelay(nodes)
	m.mu.Unlock()
}

// DownloadState 返回当前实时下载进度状态。
func (m *Manager) DownloadState() DownloadState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.download
}

// StartDownload 在后台协程发起 mihomo 内核一键在线下载（国内镜像优先，GitHub 直连兜底）。
func (m *Manager) StartDownload() (DownloadState, error) {
	ver := ResolveVersion()
	mirrorURL, directURL, asset, archiveType, err := BuildDownloadURLs(runtime.GOOS, runtime.GOARCH, ver, m.mirrorPrefix, m.githubBase)
	if err != nil {
		return DownloadState{}, err
	}

	m.mu.Lock()
	if m.download.Status == "downloading" {
		cur := m.download
		m.mu.Unlock()
		return cur, nil
	}
	m.download = DownloadState{
		Status:          "downloading",
		Version:         ver,
		Asset:           asset,
		Source:          "mirror",
		DownloadedBytes: 0,
		TotalBytes:      0,
		Percent:         0,
		Message:         "正在通过国内加速源拉取 " + asset + "…",
		UpdatedAt:       time.Now().Format("2006-01-02 15:04:05"),
	}
	snap := m.download
	m.mu.Unlock()

	targetPath := filepath.Join(m.baseDir, BinaryName(runtime.GOOS))
	if abs, errAbs := filepath.Abs(targetPath); errAbs == nil {
		targetPath = abs
	}

	go func() {
		// 若当前正在运行旧内核，先停止再覆盖落盘
		m.Stop()
		dlCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		usedSrc, errDL := downloadAndExtract(dlCtx, m.dlClient, mirrorURL, directURL, archiveType, targetPath, func(source string, downloaded, total int64) {
			pct := 0
			if total > 0 {
				pct = int(downloaded * 100 / total)
				if pct > 99 {
					pct = 99
				}
			}
			srcLabel := "国内加速源"
			if source == "direct" {
				srcLabel = "GitHub 直连"
			}
			m.mu.Lock()
			m.download.Status = "downloading"
			m.download.Source = source
			m.download.DownloadedBytes = downloaded
			m.download.TotalBytes = total
			m.download.Percent = pct
			m.download.Message = fmt.Sprintf("正在从 %s 下载 %s…", srcLabel, asset)
			m.download.UpdatedAt = time.Now().Format("2006-01-02 15:04:05")
			m.mu.Unlock()
		})

		m.mu.Lock()
		nowStr := time.Now().Format("2006-01-02 15:04:05")
		if errDL != nil {
			m.download.Status = "error"
			m.download.Error = errDL.Error()
			m.download.Message = "下载失败：" + errDL.Error()
			m.download.UpdatedAt = nowStr
			m.lastError = errDL.Error()
			m.mu.Unlock()
			if m.logger != nil {
				m.logger.Error("Mihomo", "一键下载 mihomo 内核失败: %v", errDL)
			}
			return
		}
		m.download.Status = "done"
		m.download.Source = usedSrc
		m.download.Percent = 100
		m.download.Error = ""
		m.download.Message = fmt.Sprintf("mihomo %s 已下载并就绪（来源：%s）", ver, usedSrc)
		m.download.UpdatedAt = nowStr
		m.lastError = ""
		m.mu.Unlock()

		if m.logger != nil {
			m.logger.Info("Mihomo", "mihomo %s 下载并解压成功 -> %s（来源：%s）", ver, targetPath, usedSrc)
		}
	}()

	return snap, nil
}

// UpdateConfig 更新订阅链接、开关、最大映射节点数或后台定时测速参数，并按需拉起或停止 mihomo 进程。
func (m *Manager) UpdateConfig(ctx context.Context, enabled *bool, subs []string, autoProbe *bool, probeMin *int, maxNodes *int) (Snapshot, error) {
	needRefresh := false
	needStop := false

	m.mu.Lock()
	if enabled != nil {
		m.state.Enabled = *enabled
		if !*enabled {
			needStop = true
		} else {
			needRefresh = true
		}
	}
	if subs != nil {
		m.state.Subscriptions = normalizeSubs(subs)
		if hasValidSub(m.state.Subscriptions) {
			if enabled == nil {
				m.state.Enabled = true
			}
			needRefresh = true
		} else {
			m.state.Nodes = []SubNode{}
			needStop = true
		}
	}
	if autoProbe != nil {
		m.state.AutoProbeEnabled = *autoProbe
	}
	if probeMin != nil {
		if *probeMin < 1 || *probeMin > 1440 {
			m.mu.Unlock()
			return Snapshot{}, errors.New("后台测速间隔必须为 1 ～ 1440 分钟之间的整数")
		}
		m.state.AutoProbeIntervalMin = *probeMin
	}
	if maxNodes != nil {
		if *maxNodes < 1 || *maxNodes > MaxNodesLimit {
			m.mu.Unlock()
			return Snapshot{}, fmt.Errorf("最大映射节点数必须为 1 ～ %d 之间的整数", MaxNodesLimit)
		}
		if m.state.MaxNodes != *maxNodes {
			m.state.MaxNodes = *maxNodes
			needRefresh = true
		}
	}
	if err := m.saveStateLocked(); err != nil {
		m.mu.Unlock()
		return Snapshot{}, err
	}
	active := m.state.Enabled && hasValidSub(m.state.Subscriptions)
	m.mu.Unlock()

	select {
	case m.wakeCh <- struct{}{}:
	default:
	}

	if needStop || !active {
		m.Stop()
		return m.Snapshot(), nil
	}
	if needRefresh {
		return m.RefreshSubscriptions(ctx)
	}
	return m.Snapshot(), nil
}

// RefreshSubscriptions 写入配置并重启单 mihomo 进程，拉取全部机场订阅节点、过滤假节点、生成 PUB-<端口> 组与本地 mixed 监听器并触发测速。
func (m *Manager) RefreshSubscriptions(ctx context.Context) (Snapshot, error) {
	if _, found := DetectBinary(m.baseDir); !found {
		return m.Snapshot(), fmt.Errorf("未检测到 %s 内核，请先点击「一键下载 mihomo 内核」", BinaryName(runtime.GOOS))
	}

	m.mu.RLock()
	subs := append([]string(nil), m.state.Subscriptions...)
	m.mu.RUnlock()
	if !hasValidSub(subs) {
		return m.Snapshot(), errors.New("请先填写至少一条以 http:// 或 https:// 开头的机场订阅链接")
	}

	// 1. 先按现有配置启动 mihomo 以拉取订阅 provider 列表
	if err := m.ensureProcessRunning(ctx); err != nil {
		m.setLastError(err.Error())
		return m.Snapshot(), err
	}

	// 2. 等待 proxy-providers 拉取就绪（最多 90 秒：大订阅或中转站慢响应都可能超过 30 秒）
	rawNodes, err := m.waitForProviderNodes(ctx, 90*time.Second)
	if err != nil {
		m.setLastError(err.Error())
		return m.Snapshot(), err
	}

	// 3. 过滤假节点与非落地策略组，剔除用户手动删除的节点，并分配稳定独立端口（20001 起）
	realNames := FilterRealNodes(rawNodes)
	filteredFake := len(rawNodes) - len(realNames)
	if filteredFake < 0 {
		filteredFake = 0
	}

	m.mu.Lock()
	// 排除记录只保留当前订阅里仍存在的名字，避免换订阅后旧机场节点名长期残留、误伤同名新节点。
	beforePrune := len(m.state.ExcludedNodes)
	m.state.ExcludedNodes = PruneExcludedNames(m.state.ExcludedNodes, realNames)
	prunedExcluded := beforePrune - len(m.state.ExcludedNodes)
	realNames = FilterExcludedNames(realNames, m.state.ExcludedNodes)
	assigned := AssignStableNodePorts(realNames, m.state.Nodes, m.state.BasePort, m.state.MaxNodes)
	m.state.Nodes = SortNodesByDelay(assigned)
	m.state.Enabled = true
	m.filteredFake = filteredFake
	m.lastError = ""
	if errSave := m.saveStateLocked(); errSave != nil {
		m.mu.Unlock()
		return m.Snapshot(), errSave
	}
	m.mu.Unlock()

	// 4. 写回包含完整 PUB-<端口> 过滤组与 listeners 的防 502 配置，并重启进程使全部独立回环端口监听生效
	if err := m.restartProcess(ctx); err != nil {
		m.setLastError(err.Error())
		return m.Snapshot(), err
	}

	if m.logger != nil {
		m.logger.Info("Mihomo", "订阅同步成功：共映射 %d 个真实节点（已过滤 %d 个假节点、清理 %d 条历史排除记录），独立回环端口 %d 起",
			len(assigned), filteredFake, prunedExcluded, m.state.BasePort)
	}

	// 5. 异步触发一轮并发测速
	if len(assigned) > 0 {
		go func() {
			pCtx, cancel := context.WithTimeout(context.Background(), scaledProbeTimeout(len(assigned)))
			defer cancel()
			_, _ = m.ProbeAllNodes(pCtx)
		}()
	}

	return m.Snapshot(), nil
}

// ProbeAllNodes 并发限流（8 并发）对所有真实机场节点执行测速，按延迟升序排序并落盘。
func (m *Manager) ProbeAllNodes(ctx context.Context) (Snapshot, error) {
	if !m.IsRunning() {
		return m.Snapshot(), errors.New("mihomo 内核未运行，请先导入订阅并启动")
	}

	m.probeMu.Lock()
	m.mu.Lock()
	if m.probing {
		m.mu.Unlock()
		m.probeMu.Unlock()
		return m.Snapshot(), nil
	}
	m.probing = true
	nodes := append([]SubNode(nil), m.state.Nodes...)
	ctl := m.state.Controller
	secret := m.state.Secret
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.probing = false
		m.mu.Unlock()
		m.probeMu.Unlock()
	}()

	if len(nodes) == 0 {
		return m.Snapshot(), nil
	}

	// 优先尝试 /group/PROXY/delay 组测速；未命中节点走 8 并发单节点补测
	delayMap := m.probeGroupDelay(ctx, ctl, secret)
	if ctx.Err() != nil {
		// 调用方已取消/超时，放弃本轮结果，避免把未测节点误标为超时
		return m.Snapshot(), ctx.Err()
	}

	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	var mapMu sync.Mutex
	nowStr := time.Now().Format("2006-01-02 15:04:05")

	for i := range nodes {
		name := nodes[i].Name
		if d, ok := delayMap[name]; ok && d > 0 {
			nodes[i].DelayMS = d
			nodes[i].CheckedAt = nowStr
			continue
		}
		wg.Add(1)
		go func(idx int, groupName string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			d := m.probeSingleDelay(ctx, ctl, secret, groupName)
			if ctx.Err() != nil {
				return
			}
			mapMu.Lock()
			nodes[idx].DelayMS = d
			nodes[idx].CheckedAt = nowStr
			mapMu.Unlock()
		}(i, fmt.Sprintf("PUB-%d", nodes[i].Port))
	}
	wg.Wait()

	if ctx.Err() != nil {
		return m.Snapshot(), ctx.Err()
	}

	sorted := SortNodesByDelay(nodes)
	m.mu.Lock()
	// 测速期间可能有节点被删除：写回前剔除已排除节点，避免删除操作被测速结果覆盖。
	excluded := make(map[string]struct{}, len(m.state.ExcludedNodes))
	for _, n := range m.state.ExcludedNodes {
		excluded[n] = struct{}{}
	}
	kept := make([]SubNode, 0, len(sorted))
	for _, n := range sorted {
		if _, gone := excluded[n.Name]; !gone {
			kept = append(kept, n)
		}
	}
	m.state.Nodes = kept
	m.state.LastProbeAt = nowStr
	_ = m.saveStateLocked()
	m.mu.Unlock()

	if m.logger != nil {
		okCnt := 0
		for _, n := range sorted {
			if n.DelayMS > 0 {
				okCnt++
			}
		}
		m.logger.Info("Mihomo", "机场节点测速完成：%d / %d 可用，已按延迟升序排列", okCnt, len(sorted))
	}
	return m.Snapshot(), nil
}

// DeleteNodes 删除选中的机场节点：从当前映射列表移除并记入长期排除名单，
// 之后重新同步订阅也不会再加回来；若内核可用则重建配置使被删节点端口立即释放。
func (m *Manager) DeleteNodes(ctx context.Context, names []string) (Snapshot, error) {
	targets := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		targets = append(targets, name)
	}
	if len(targets) == 0 {
		return m.Snapshot(), errors.New("请先勾选需要删除的机场节点")
	}

	m.mu.Lock()
	if len(m.state.Nodes) == 0 {
		m.mu.Unlock()
		return m.Snapshot(), errors.New("当前没有可删除的机场节点")
	}
	remove := make(map[string]struct{}, len(targets))
	for _, n := range targets {
		remove[n] = struct{}{}
	}
	kept := make([]SubNode, 0, len(m.state.Nodes))
	removedNames := make([]string, 0, len(targets))
	for _, n := range m.state.Nodes {
		if _, drop := remove[n.Name]; drop {
			removedNames = append(removedNames, n.Name)
			continue
		}
		kept = append(kept, n)
	}
	if len(removedNames) == 0 {
		m.mu.Unlock()
		return m.Snapshot(), errors.New("未找到要删除的机场节点，节点列表可能已变化，请刷新后重试")
	}
	// 只把真实存在的节点记入排除名单，避免误提交的不存在名称长期污染状态。
	m.state.ExcludedNodes = dedupeNames(append(m.state.ExcludedNodes, removedNames...))
	m.state.Nodes = kept
	m.loginRR = 0
	remaining := len(kept)
	excludedTotal := len(m.state.ExcludedNodes)
	enabled := m.state.Enabled
	if err := m.saveStateLocked(); err != nil {
		m.mu.Unlock()
		return m.Snapshot(), err
	}
	m.mu.Unlock()

	// 重建配置：端口与 PUB 组都要随之收缩，被删节点的回环端口立刻释放。
	if _, found := DetectBinary(m.baseDir); found && enabled {
		if remaining == 0 {
			m.Stop()
		} else if err := m.restartProcess(ctx); err != nil {
			m.setLastError(fmt.Sprintf("节点已删除，但 mihomo 重启失败：%v", err))
		} else {
			m.setLastError("")
		}
	}

	if m.logger != nil {
		m.logger.Info("Mihomo", "已删除 %d 个机场节点，剩余 %d 个，累计排除 %d 个", len(removedNames), remaining, excludedTotal)
	}
	return m.Snapshot(), nil
}

// RestoreExcludedNodes 清空长期排除名单；订阅桥已启用且内核可用时立即重新同步订阅把节点加回。
// 返回的 resynced 表示本次是否真的重新拉取订阅（false 时节点要等下次同步才会回来）。
func (m *Manager) RestoreExcludedNodes(ctx context.Context) (snap Snapshot, resynced bool, err error) {
	m.mu.Lock()
	restored := len(m.state.ExcludedNodes)
	m.state.ExcludedNodes = []string{}
	enabled := m.state.Enabled
	subs := append([]string(nil), m.state.Subscriptions...)
	if err := m.saveStateLocked(); err != nil {
		m.mu.Unlock()
		return m.Snapshot(), false, err
	}
	m.mu.Unlock()

	if restored == 0 {
		return m.Snapshot(), false, errors.New("当前没有被删除的机场节点")
	}

	if enabled && hasValidSub(subs) {
		if _, found := DetectBinary(m.baseDir); found {
			snap, err := m.RefreshSubscriptions(ctx)
			if err != nil {
				return snap, false, fmt.Errorf("已清空删除记录，但重新同步订阅失败: %w", err)
			}
			resynced = true
		}
	}
	if m.logger != nil {
		m.logger.Info("Mihomo", "已恢复 %d 个删除的机场节点（重新同步=%v）", restored, resynced)
	}
	return m.Snapshot(), resynced, nil
}

func dedupeNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		s := strings.TrimSpace(raw)
		if s == "" {
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

// NodesByName 按名称批量取回当前映射的机场节点（用于把登录节点桥的节点转出到代理池）。
// 返回顺序与入参一致，未命中的名称被跳过。
func (m *Manager) NodesByName(names []string) []SubNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	byName := make(map[string]SubNode, len(m.state.Nodes))
	for _, n := range m.state.Nodes {
		byName[n.Name] = n
	}
	out := make([]SubNode, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		if n, ok := byName[name]; ok {
			out = append(out, n)
		}
	}
	return out
}

// ResolveNodeProxyURL 实现 proxypool.MihomoResolver 接口：按节点名称解析出本地回环 mixed 代理地址。
func (m *Manager) ResolveNodeProxyURL(nodeName string) (proxyURL string, resolvedName string, err error) {
	target := strings.TrimSpace(nodeName)
	if target == "" {
		return "", "", errors.New("未指定机场订阅节点名称")
	}
	m.mu.RLock()
	enabled := m.state.Enabled
	nodes := m.state.Nodes
	m.mu.RUnlock()

	if !enabled {
		return "", "", errors.New("mihomo 订阅代理桥未启用")
	}
	for _, n := range nodes {
		if n.Name == target {
			return fmt.Sprintf("http://127.0.0.1:%d", n.Port), n.Name, nil
		}
	}
	low := strings.ToLower(target)
	for _, n := range nodes {
		if strings.ToLower(strings.TrimSpace(n.Name)) == low {
			return fmt.Sprintf("http://127.0.0.1:%d", n.Port), n.Name, nil
		}
	}
	return "", "", fmt.Errorf("机场订阅节点「%s」不存在或已从订阅中下线，请重新选择节点", target)
}

// ListNodeNames 返回当前按延迟排序后的真实机场节点名列表。
func (m *Manager) ListNodeNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sorted := SortNodesByDelay(m.state.Nodes)
	out := make([]string, 0, len(sorted))
	for _, n := range sorted {
		out = append(out, n.Name)
	}
	return out
}

func (m *Manager) setLastError(msg string) {
	m.mu.Lock()
	m.lastError = msg
	m.mu.Unlock()
}

func (m *Manager) writeConfigFile() (string, error) {
	if err := os.MkdirAll(filepath.Join(m.workDir, "providers"), 0o755); err != nil {
		return "", err
	}
	m.mu.RLock()
	opts := ConfigOptions{
		MixedPort:  m.state.MixedPort,
		Controller: m.state.Controller,
		Secret:     m.state.Secret,
		Subs:       append([]string(nil), m.state.Subscriptions...),
		Nodes:      append([]SubNode(nil), m.state.Nodes...),
	}
	m.mu.RUnlock()

	yamlContent := BuildConfigYAML(opts)
	cfgPath := filepath.Join(m.workDir, "config.yaml")
	tmp := cfgPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(yamlContent), 0o600); err != nil {
		return "", fmt.Errorf("写入 mihomo 配置失败: %w", err)
	}
	if err := os.Rename(tmp, cfgPath); err != nil {
		return "", fmt.Errorf("落盘 mihomo 配置失败: %w", err)
	}
	return cfgPath, nil
}

func (m *Manager) ensureProcessRunning(ctx context.Context) error {
	if m.IsRunning() {
		return nil
	}
	return m.restartProcess(ctx)
}

func (m *Manager) restartProcess(ctx context.Context) error {
	binPath, found := DetectBinary(m.baseDir)
	if !found {
		return fmt.Errorf("未检测到 %s 内核", BinaryName(runtime.GOOS))
	}
	cfgPath, err := m.writeConfigFile()
	if err != nil {
		return err
	}

	m.Stop()

	cmd := exec.Command(binPath, "-d", m.workDir, "-f", cfgPath)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &limitedWriter{buf: &stderrBuf, max: 4096}
	cmd.Stdout = io.Discard

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动 mihomo 进程失败: %w", err)
	}

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	m.procMu.Lock()
	m.cmd = cmd
	m.cmdDone = done
	m.procMu.Unlock()

	m.mu.RLock()
	nodeCount := len(m.state.Nodes)
	m.mu.RUnlock()

	// 等待控制端口就绪（节点越多，配置解析与监听初始化越慢）
	deadline := time.Now().Add(scaledStartWait(nodeCount))
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			errDetail := strings.TrimSpace(stderrBuf.String())
			if errDetail != "" {
				return fmt.Errorf("mihomo 进程异常退出: %s", errDetail)
			}
			return errors.New("mihomo 进程启动后立即退出，请检查端口占用或订阅配置")
		default:
		}
		if m.IsRunning() {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return errors.New("等待 mihomo 控制端口响应超时")
}

func (m *Manager) waitForProviderNodes(ctx context.Context, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		nodes, err := m.fetchProviderNodes(ctx)
		if err == nil && len(nodes) > 0 {
			return nodes, nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		return nil, fmt.Errorf("拉取机场订阅节点超时: %w", lastErr)
	}
	return nil, errors.New("订阅拉取完成但未解析到任何代理节点，请检查订阅链接是否有效")
}

func (m *Manager) fetchProviderNodes(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	ctl := m.state.Controller
	secret := m.state.Secret
	m.mu.RUnlock()

	status, raw, err := m.controllerRequest(ctx, http.MethodGet, "/providers/proxies", nil, ctl, secret)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("controller /providers/proxies 返回 HTTP %d", status)
	}
	var resp struct {
		Providers map[string]struct {
			Name    string `json:"name"`
			Proxies []struct {
				Name string `json:"name"`
			} `json:"proxies"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	out := make([]string, 0, 64)
	for provName, p := range resp.Providers {
		if strings.EqualFold(provName, "default") {
			continue
		}
		for _, item := range p.Proxies {
			if n := strings.TrimSpace(item.Name); n != "" {
				out = append(out, n)
			}
		}
	}
	return out, nil
}

func (m *Manager) probeGroupDelay(ctx context.Context, ctl, secret string) map[string]int {
	out := make(map[string]int)
	q := "/group/PROXY/delay?timeout=5000&url=" + url.QueryEscape("https://www.gstatic.com/generate_204")
	status, raw, err := m.controllerRequest(ctx, http.MethodGet, q, nil, ctl, secret)
	if err != nil || status != http.StatusOK {
		return out
	}
	var mRes map[string]int
	if json.Unmarshal(raw, &mRes) == nil {
		for k, v := range mRes {
			if v > 0 {
				out[k] = v
			}
		}
	}
	return out
}

// probeSingleDelay 经节点专属的 PUB-<端口> 组测速；provider 内节点不会出现在 /proxies/<节点名> 下，直接按节点名查询只会 404。
func (m *Manager) probeSingleDelay(ctx context.Context, ctl, secret, groupName string) int {
	p := "/proxies/" + url.PathEscape(groupName) + "/delay?timeout=5000&url=" + url.QueryEscape("https://www.gstatic.com/generate_204")
	status, raw, err := m.controllerRequest(ctx, http.MethodGet, p, nil, ctl, secret)
	if err != nil || status != http.StatusOK {
		return -1
	}
	var res struct {
		Delay int `json:"delay"`
	}
	if json.Unmarshal(raw, &res) == nil && res.Delay > 0 {
		return res.Delay
	}
	return -1
}

func (m *Manager) controllerRequest(ctx context.Context, method, apiPath string, body []byte, ctl, secret string) (int, []byte, error) {
	endpoint := "http://" + strings.TrimPrefix(strings.TrimPrefix(ctl, "http://"), "https://") + apiPath
	var rdr io.Reader
	if len(body) > 0 {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, rdr)
	if err != nil {
		return 0, nil, err
	}
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.httpCtl.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}

type limitedWriter struct {
	buf *bytes.Buffer
	max int
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.buf.Len() < lw.max {
		rem := lw.max - lw.buf.Len()
		if len(p) > rem {
			lw.buf.Write(p[:rem])
		} else {
			lw.buf.Write(p)
		}
	}
	return len(p), nil
}
