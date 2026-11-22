package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

const maxCheckinHistory = 500

// 令牌自动续期的失败退避区间：15m → 30m → 1h → 2h → 4h → 8h → 12h（封顶）。
const (
	refreshRetryBase    = 15 * time.Minute
	refreshRetryMaxBack = 12 * time.Hour
)

// CheckinRecord 记录一次账号签到的详细历史。
type CheckinRecord struct {
	Time           string          `json:"time"`
	Date           string          `json:"date"`
	AccountID      string          `json:"account_id"`
	AccountLabel   string          `json:"account_label"`
	Region         protocol.Region `json:"region"`
	Status         string          `json:"status"` // done | already | failed | skipped
	Points         int64           `json:"points"`
	Message        string          `json:"message"`
	ActiveReported bool            `json:"active_reported"`
	Source         string          `json:"source"` // auto | manual
}

// refreshFailure 记录账号令牌自动续期的连续失败次数与下次允许重试的时间（按账号维度指数退避）。
type refreshFailure struct {
	fails  int
	nextAt time.Time
}

// Engine 驱动 Token 自动续期、每日定时签到、余额同步与凭证目录热重载。
type Engine struct {
	cfgMgr   *config.Manager
	pool     *accounts.Pool
	router   *accounts.Router
	proxyMgr *proxypool.Manager
	logger   *logging.Logger

	mu             sync.RWMutex
	history        []CheckinRecord
	lastAutoDay    string
	runningCheckin bool
	refreshFails   map[string]refreshFailure
}

// New 创建后台调度引擎并加载持久化的签到历史。
func New(cfgMgr *config.Manager, pool *accounts.Pool, router *accounts.Router, proxyMgr *proxypool.Manager, logger *logging.Logger) *Engine {
	e := &Engine{
		cfgMgr:       cfgMgr,
		pool:         pool,
		router:       router,
		proxyMgr:     proxyMgr,
		logger:       logger,
		history:      make([]CheckinRecord, 0, 64),
		refreshFails: make(map[string]refreshFailure),
	}
	e.loadHistory()
	return e
}

func (e *Engine) historyFilePath() string {
	return filepath.Join(e.cfgMgr.Get().DataDir, "checkin_history.json")
}

func (e *Engine) loadHistory() {
	raw, err := os.ReadFile(e.historyFilePath())
	if err != nil {
		return
	}
	var records []CheckinRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return
	}
	e.mu.Lock()
	e.history = records
	e.mu.Unlock()

	// 回填今日账号签到状态到内存池中；当日已存在自动签到记录时恢复 lastAutoDay，
	// 避免每天签到时间点之后重启服务会在 1 分钟内重复触发一轮全量自动签到
	today := time.Now().Format("2006-01-02")
	autoToday := false
	for _, r := range records {
		if r.Date == today {
			e.pool.UpdateCheckinState(r.AccountID, r.Date, r.Status, r.Message)
			if r.Source == "auto" {
				autoToday = true
			}
		}
	}
	if autoToday {
		e.mu.Lock()
		e.lastAutoDay = today
		e.mu.Unlock()
		if e.logger != nil {
			e.logger.Info("Checkin", "检测到今日已执行过自动签到，重启后不再重复触发")
		}
	}
}

func (e *Engine) appendHistory(rec CheckinRecord) {
	e.mu.Lock()
	e.history = append([]CheckinRecord{rec}, e.history...)
	if len(e.history) > maxCheckinHistory {
		e.history = e.history[:maxCheckinHistory]
	}
	snap := append([]CheckinRecord(nil), e.history...)
	e.mu.Unlock()

	dir := filepath.Dir(e.historyFilePath())
	_ = os.MkdirAll(dir, 0o755)
	if data, err := json.MarshalIndent(snap, "", "  "); err == nil {
		tmp := e.historyFilePath() + ".tmp"
		if os.WriteFile(tmp, append(data, '\n'), 0o600) == nil {
			_ = os.Rename(tmp, e.historyFilePath())
		}
	}
}

// History 返回最近 limit 条签到历史（倒序，最新在前）。
func (e *Engine) History(limit int) []CheckinRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if limit <= 0 || limit > len(e.history) {
		limit = len(e.history)
	}
	out := make([]CheckinRecord, limit)
	copy(out, e.history[:limit])
	return out
}

// Start 启动后台定时调度协程。
func (e *Engine) Start(ctx context.Context) {
	// 启动后异步预热：实时同步双区模型目录并拉取一次余额
	go func() {
		warmupCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		_ = e.router.RefreshAllModels(warmupCtx)
		e.RefreshAllBalances(warmupCtx)
	}()

	go func() {
		syncTicker := time.NewTicker(30 * time.Second)
		refreshTicker := time.NewTicker(15 * time.Minute)
		modelTicker := time.NewTicker(30 * time.Minute)
		balanceTicker := time.NewTicker(30 * time.Minute)
		defer syncTicker.Stop()
		defer refreshTicker.Stop()
		defer modelTicker.Stop()
		defer balanceTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-syncTicker.C:
				if err := e.pool.SyncFromDisk(); err != nil && e.logger != nil {
					e.logger.Warn("Scheduler", "扫描凭证目录失败: %v", err)
				}
				e.router.PruneInactiveRegions()
				e.maybeRunDailyCheckin(ctx)
			case <-refreshTicker.C:
				e.refreshExpiringTokens(ctx)
			case <-modelTicker.C:
				probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_ = e.router.RefreshAllModels(probeCtx)
				cancel()
			case <-balanceTicker.C:
				e.refreshBalancesPeriodic(ctx)
			}
		}
	}()
}

func (e *Engine) maybeRunDailyCheckin(ctx context.Context) {
	cfg := e.cfgMgr.Get()
	if !cfg.Checkin.Enabled {
		return
	}
	now := time.Now()
	today := now.Format("2006-01-02")
	currentHHMM := now.Format("15:04")
	if currentHHMM < cfg.Checkin.Time {
		return
	}

	e.mu.Lock()
	if e.lastAutoDay == today || e.runningCheckin {
		e.mu.Unlock()
		return
	}
	e.lastAutoDay = today
	e.mu.Unlock()

	if e.logger != nil {
		e.logger.Info("Checkin", "到达每日自动签到时间（%s），开始执行全量签到任务", cfg.Checkin.Time)
	}
	go func() {
		// 多账号自动签到带 3~10 秒随机错峰，账号较多时总耗时会显著增加，放宽到 10 分钟
		runCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		e.CheckinAll(runCtx, "auto")
	}()
}

func (e *Engine) refreshExpiringTokens(ctx context.Context) {
	now := time.Now().Unix()
	for _, acc := range e.pool.AllAccounts() {
		if acc.Cred.Disabled || acc.Cred.RefreshToken == "" {
			continue
		}
		needRefresh := acc.SessionDead || (acc.Cred.ExpiresAt > 0 && acc.Cred.ExpiresAt-now <= e.tokenRefreshThreshold(&acc))
		if !needRefresh {
			continue
		}
		// 失败指数退避：未到下次允许重试时间则跳过，避免每 15 分钟对同一账号死循环重试刷新接口
		e.mu.RLock()
		rf, hasFail := e.refreshFails[acc.ID]
		e.mu.RUnlock()
		if hasFail && time.Now().Before(rf.nextAt) {
			continue
		}
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := e.RefreshAccountToken(runCtx, acc.ID); err != nil {
			e.mu.Lock()
			cur := e.refreshFails[acc.ID]
			cur.fails++
			delay := refreshRetryBase << min(cur.fails-1, 6)
			if delay > refreshRetryMaxBack {
				delay = refreshRetryMaxBack
			}
			cur.nextAt = time.Now().Add(delay)
			e.refreshFails[acc.ID] = cur
			e.mu.Unlock()
			if e.logger != nil {
				e.logger.Warn("Scheduler", "账号 %s 自动续期失败（第 %d 次，%.0f 分钟后重试）: %v",
					acc.Cred.DisplayLabel(), cur.fails, delay.Minutes(), err)
			}
		} else {
			e.mu.Lock()
			delete(e.refreshFails, acc.ID)
			e.mu.Unlock()
			if e.logger != nil {
				e.logger.Info("Scheduler", "账号 %s Token 自动续期成功", acc.Cred.DisplayLabel())
			}
		}
		cancel()
	}
}

// tokenRefreshThreshold 依据令牌真实签发周期（exp - iat）动态计算提前续期窗口：
//   - 短效令牌（ttl <= 48h，如国际版 24h）：剩余 <= max(2h, ttl/6) 时才续期（24h 令牌即剩余 4h 续期，约每 20 小时续一次）；
//   - 长效令牌（如国内版 28 天）或无法解析时：沿用提前 3 天（72h）的保守窗口。
func (e *Engine) tokenRefreshThreshold(acc *accounts.Account) int64 {
	exp := acc.Cred.ExpiresAt
	iat := protocol.JWTIssuedAtSeconds(acc.Cred.AccessToken)
	if exp > 0 && iat > 0 && exp > iat {
		ttl := exp - iat
		if ttl <= int64((48 * time.Hour).Seconds()) {
			window := ttl / 6
			if window < int64((2 * time.Hour).Seconds()) {
				window = int64((2 * time.Hour).Seconds())
			}
			return window
		}
	}
	return int64((3 * 24 * time.Hour).Seconds())
}

func (e *Engine) baseURLFor(region protocol.Region) string {
	cfg := e.cfgMgr.Get()
	if region.IsGlobal() {
		return protocol.ResolveBaseURL(region, cfg.Upstream.GlobalBaseURL)
	}
	return protocol.ResolveBaseURL(region, cfg.Upstream.CNBaseURL)
}

// RefreshAccountToken 手动或定时刷新单个账号的访问令牌并无损写回。
func (e *Engine) RefreshAccountToken(ctx context.Context, id string) error {
	acc, ok := e.pool.GetByID(id)
	if !ok {
		return fmt.Errorf("账号不存在: %s", id)
	}
	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 30*time.Second)
	if errClient != nil {
		return fmt.Errorf("代理出口解析失败: %w", errClient)
	}

	res, err := protocol.RefreshToken(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.RefreshToken, acc.Cred.DeviceID)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(id, uerr)
		}
		return err
	}
	return e.pool.ApplyRefreshedTokens(id, res.AccessToken, res.RefreshToken, res.ExpiresAt)
}

// RefreshAccountBalance 查询并缓存单个账号的积分余额。
func (e *Engine) RefreshAccountBalance(ctx context.Context, id string) (float64, error) {
	acc, ok := e.pool.GetByID(id)
	if !ok {
		return 0, fmt.Errorf("账号不存在: %s", id)
	}
	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 20*time.Second)
	if errClient != nil {
		return 0, fmt.Errorf("代理出口解析失败: %w", errClient)
	}

	bal, err := protocol.FetchWalletBalance(ctx, client, baseURL, acc.Cred.AccessToken)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(id, uerr)
		}
		return 0, err
	}
	e.pool.UpdateBalance(id, bal)
	return bal, nil
}

// refreshBalancesPeriodic 后台周期性刷新余额：日常调用扣积分后控制台余额可自动跟进。
// 当没有任何未停用、未失效且不在冷却中的账号时直接跳过，避免空转请求。
func (e *Engine) refreshBalancesPeriodic(ctx context.Context) {
	now := time.Now()
	hasActive := false
	for _, acc := range e.pool.AllAccounts() {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		if !acc.CooldownUntil.IsZero() && now.Before(acc.CooldownUntil) {
			continue
		}
		hasActive = true
		break
	}
	if !hasActive {
		return
	}
	if n := e.RefreshAllBalances(ctx); n > 0 && e.logger != nil {
		e.logger.Info("Scheduler", "后台定时余额刷新完成，共更新 %d 个账号", n)
	}
}

// RefreshAllBalances 并发查询所有启用账号的积分余额（并发收敛至 2，避免多账号同出口 IP 瞬时突发）。
func (e *Engine) RefreshAllBalances(ctx context.Context) int {
	list := e.pool.AllAccounts()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 2)
	var mu sync.Mutex
	okCount := 0

	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if _, err := e.RefreshAccountBalance(ctx, id); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}(acc.ID)
	}
	wg.Wait()
	return okCount
}

// CheckinAccount 对单个账号执行每日签到，签到成功后上报登录活跃并刷新余额。
func (e *Engine) CheckinAccount(ctx context.Context, id, source string) CheckinRecord {
	now := time.Now()
	today := now.Format("2006-01-02")
	nowStr := now.Format("2006-01-02 15:04:05")
	if source == "" {
		source = "manual"
	}

	acc, ok := e.pool.GetByID(id)
	if !ok {
		return CheckinRecord{
			Time:      nowStr,
			Date:      today,
			AccountID: id,
			Status:    "failed",
			Message:   "账号不存在",
			Source:    source,
		}
	}

	rec := CheckinRecord{
		Time:         nowStr,
		Date:         today,
		AccountID:    acc.ID,
		AccountLabel: acc.Cred.DisplayLabel(),
		Region:       acc.Cred.Region,
		Source:       source,
	}

	if acc.Cred.Disabled {
		rec.Status = "skipped"
		rec.Message = "账号已停用，跳过签到"
		return rec
	}

	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 20*time.Second)
	if errClient != nil {
		rec.Status = "failed"
		rec.Message = "代理出口不可用: " + errClient.Error()
		e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
		e.appendHistory(rec)
		return rec
	}

	outcome, err := protocol.DailyCheckin(ctx, client, baseURL, acc.Cred.AccessToken)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(acc.ID, uerr)
		}
		rec.Status = "failed"
		rec.Message = err.Error()
		e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
		e.appendHistory(rec)
		return rec
	}

	if outcome.AlreadyCompleted {
		rec.Status = "already"
		rec.Message = outcome.Message
		// 已签时顺带补一次登录活跃上报，确保当日活跃奖励触发
		if errAct := protocol.ReportLoginActive(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.DeviceID); errAct == nil {
			rec.ActiveReported = true
		}
	} else if outcome.Claimed {
		rec.Status = "done"
		rec.Points = outcome.Points
		rec.Message = outcome.Message
		if errAct := protocol.ReportLoginActive(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.DeviceID); errAct == nil {
			rec.ActiveReported = true
		}
		// 签到成功后刷新一次余额
		_, _ = e.RefreshAccountBalance(ctx, acc.ID)
	} else {
		rec.Status = "failed"
		rec.Message = outcome.Message
	}

	e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
	e.appendHistory(rec)
	if e.logger != nil {
		e.logger.Info("Checkin", "账号 [%s] 签到结果: %s (%s)", rec.AccountLabel, rec.Status, rec.Message)
	}
	return rec
}

// CheckinAll 对所有启用且未会话失效的账号批量执行每日签到。
// source 为 auto 时跳过当日已签到成功的账号，并在多账号之间加入随机错峰延迟，
// 避免多账号同一秒并发请求触发上游风控。
func (e *Engine) CheckinAll(ctx context.Context, source string) []CheckinRecord {
	e.mu.Lock()
	if e.runningCheckin {
		e.mu.Unlock()
		return nil
	}
	e.runningCheckin = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.runningCheckin = false
		e.mu.Unlock()
	}()

	auto := source == "auto"
	today := time.Now().Format("2006-01-02")
	list := e.pool.AllAccounts()
	results := make([]CheckinRecord, 0, len(list))
	processed := 0
	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		if auto && acc.LastCheckin == today && (acc.CheckinStatus == "done" || acc.CheckinStatus == "already") {
			continue
		}
		if processed > 0 {
			// 随机 3~10 秒错峰；ctx 被取消时尽快返回
			delay := time.Duration(3+rand.Int64N(8)) * time.Second
			select {
			case <-ctx.Done():
				return results
			case <-time.After(delay):
			}
		}
		rec := e.CheckinAccount(ctx, acc.ID, source)
		results = append(results, rec)
		processed++
	}
	return results
}
