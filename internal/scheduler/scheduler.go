package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
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

const (
	maxCheckinHistory    = 500
	refreshBackoffBase   = 15 * time.Minute
	refreshBackoffMax    = 12 * time.Hour
	balanceRefreshConc   = 2
	balanceRefreshPeriod = 30 * time.Minute
)

type refreshBackoffState struct {
	failures  int
	nextRetry time.Time
}

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
	refreshBackoff map[string]refreshBackoffState
	checkinStagger func() time.Duration
}

// New 创建后台调度引擎并加载持久化的签到历史。
func New(cfgMgr *config.Manager, pool *accounts.Pool, router *accounts.Router, proxyMgr *proxypool.Manager, logger *logging.Logger) *Engine {
	e := &Engine{
		cfgMgr:         cfgMgr,
		pool:           pool,
		router:         router,
		proxyMgr:       proxyMgr,
		logger:         logger,
		history:        make([]CheckinRecord, 0, 64),
		refreshBackoff: make(map[string]refreshBackoffState),
		checkinStagger: defaultCheckinStagger,
	}
	e.loadHistory()
	return e
}

func defaultCheckinStagger() time.Duration {
	// 3~10 秒随机错峰，避免多号同一秒并发签到触发风控
	n, err := rand.Int(rand.Reader, big.NewInt(7001))
	if err != nil {
		return 5 * time.Second
	}
	return 3*time.Second + time.Duration(n.Int64())*time.Millisecond
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
	today := time.Now().Format("2006-01-02")
	autoRanToday := false
	// history 按时间倒序存储，从旧到新回填确保每个账号保留当日最新状态
	for i := len(records) - 1; i >= 0; i-- {
		r := records[i]
		if r.Date == today {
			e.pool.UpdateCheckinState(r.AccountID, r.Date, r.Status, r.Message)
			if r.Source == "auto" {
				autoRanToday = true
			}
		}
	}

	e.mu.Lock()
	e.history = records
	if autoRanToday {
		e.lastAutoDay = today
	}
	e.mu.Unlock()
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
		e.RefreshAllBalancesWithTrigger(warmupCtx, "startup")
	}()

	go func() {
		syncTicker := time.NewTicker(30 * time.Second)
		refreshTicker := time.NewTicker(15 * time.Minute)
		modelTicker := time.NewTicker(30 * time.Minute)
		balanceTicker := time.NewTicker(balanceRefreshPeriod)
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
				if !e.hasBalanceRefreshCandidates() {
					continue
				}
				balCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				_ = e.RefreshAllBalancesWithTrigger(balCtx, "auto")
				cancel()
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
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		e.CheckinAll(runCtx, "auto")
	}()
}

// calcRefreshThresholdSeconds 根据 JWT 真实签发周期动态计算提前续期窗口（秒）：
//   - 短效令牌（ttl <= 48h，如国际版 24h JWT）：剩余 <= max(2h, ttl/6) 时才续期（24h 令牌即剩余 4h 续期）；
//   - 长效令牌（ttl > 48h，如国内版 28 天）：保持提前 3 天（72h）；
//   - 无法解析 iat 时：国际区回落 4h，国内区回落 72h。
func calcRefreshThresholdSeconds(acc accounts.Account) int64 {
	const (
		shortTTLBoundary int64 = 48 * 3600
		minShortWindow   int64 = 2 * 3600
		globalFallback   int64 = 4 * 3600
		longWindow       int64 = 3 * 24 * 3600
	)
	ttl := protocol.JWTTTLSeconds(acc.Cred.AccessToken)
	if ttl > 0 {
		if ttl <= shortTTLBoundary {
			w := ttl / 6
			if w < minShortWindow {
				w = minShortWindow
			}
			return w
		}
		return longWindow
	}
	if acc.Cred.Region.IsGlobal() {
		return globalFallback
	}
	return longWindow
}

func calcRefreshBackoffDelay(failures int) time.Duration {
	if failures <= 1 {
		return refreshBackoffBase
	}
	shift := failures - 1
	if shift > 8 {
		shift = 8
	}
	d := refreshBackoffBase << shift
	if d > refreshBackoffMax || d <= 0 {
		return refreshBackoffMax
	}
	return d
}

func (e *Engine) shouldSkipRefreshBackoff(accountID string, now time.Time) bool {
	e.mu.RLock()
	st, ok := e.refreshBackoff[accountID]
	e.mu.RUnlock()
	return ok && now.Before(st.nextRetry)
}

func (e *Engine) recordRefreshFailure(accountID string, now time.Time) time.Duration {
	e.mu.Lock()
	st := e.refreshBackoff[accountID]
	st.failures++
	delay := calcRefreshBackoffDelay(st.failures)
	st.nextRetry = now.Add(delay)
	e.refreshBackoff[accountID] = st
	e.mu.Unlock()
	return delay
}

func (e *Engine) resetRefreshBackoff(accountID string) {
	e.mu.Lock()
	delete(e.refreshBackoff, accountID)
	e.mu.Unlock()
}

func (e *Engine) refreshExpiringTokens(ctx context.Context) {
	nowTime := time.Now()
	now := nowTime.Unix()
	for _, acc := range e.pool.AllAccounts() {
		if acc.Cred.Disabled || acc.Cred.RefreshToken == "" {
			continue
		}
		if e.shouldSkipRefreshBackoff(acc.ID, nowTime) {
			continue
		}
		threshold := calcRefreshThresholdSeconds(acc)
		needRefresh := acc.SessionDead || (acc.Cred.ExpiresAt > 0 && acc.Cred.ExpiresAt-now <= threshold)
		if !needRefresh {
			continue
		}
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := e.RefreshAccountToken(runCtx, acc.ID); err != nil {
			delay := e.recordRefreshFailure(acc.ID, time.Now())
			if e.logger != nil {
				e.logger.Warn("Scheduler", "账号 %s 自动续期失败（%s 后重试）: %v", acc.Cred.DisplayLabel(), delay, err)
			}
		} else if e.logger != nil {
			e.logger.Info("Scheduler", "账号 %s Token 自动续期成功", acc.Cred.DisplayLabel())
		}
		cancel()
	}
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
	if err := e.pool.ApplyRefreshedTokens(id, res.AccessToken, res.RefreshToken, res.ExpiresAt); err != nil {
		return err
	}
	e.resetRefreshBackoff(id)
	return nil
}

// RefreshAccountBalance 查询并缓存单个账号的积分余额（默认触发源 manual）。
func (e *Engine) RefreshAccountBalance(ctx context.Context, id string) (float64, error) {
	return e.RefreshAccountBalanceWithTrigger(ctx, id, "manual")
}

// RefreshAccountBalanceWithTrigger 查询并缓存单个账号的积分余额，并记录变动审计触发来源。
func (e *Engine) RefreshAccountBalanceWithTrigger(ctx context.Context, id, trigger string) (float64, error) {
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
	e.pool.UpdateBalanceWithTrigger(id, bal, trigger)
	return bal, nil
}

func (e *Engine) hasBalanceRefreshCandidates() bool {
	for _, acc := range e.pool.AllAccounts() {
		if !acc.Cred.Disabled && !acc.SessionDead {
			return true
		}
	}
	return false
}

// RefreshAllBalances 并发查询所有启用账号的积分余额（并发上限收敛为 2，避免同出口 IP 突发请求）。
func (e *Engine) RefreshAllBalances(ctx context.Context) int {
	return e.RefreshAllBalancesWithTrigger(ctx, "manual")
}

// RefreshAllBalancesWithTrigger 并发查询所有启用账号的积分余额，并附带触发来源用于余额变动归因。
func (e *Engine) RefreshAllBalancesWithTrigger(ctx context.Context, trigger string) int {
	list := e.pool.AllAccounts()
	var wg sync.WaitGroup
	sem := make(chan struct{}, balanceRefreshConc)
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
			if _, err := e.RefreshAccountBalanceWithTrigger(ctx, id, trigger); err == nil {
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
		e.pool.UpdateCheckinStateWithPoints(acc.ID, today, rec.Status, rec.Message, rec.Points)
		if errAct := protocol.ReportLoginActive(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.DeviceID); errAct == nil {
			rec.ActiveReported = true
		}
		// 签到成功后刷新一次余额
		_, _ = e.RefreshAccountBalanceWithTrigger(ctx, acc.ID, "checkin")
	} else {
		rec.Status = "failed"
		rec.Message = outcome.Message
	}

	e.pool.UpdateCheckinStateWithPoints(acc.ID, today, rec.Status, rec.Message, rec.Points)
	e.appendHistory(rec)
	if e.logger != nil {
		e.logger.Info("Checkin", "账号 [%s] 签到结果: %s (%s)", rec.AccountLabel, rec.Status, rec.Message)
	}
	return rec
}

// CheckinAll 对所有启用且未会话失效的账号批量执行每日签到。
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

	today := time.Now().Format("2006-01-02")
	list := e.pool.AllAccounts()
	candidates := make([]accounts.Account, 0, len(list))
	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		// 自动签到跳过当日已签到（done/already）的账号，避免重启或重复触发造成冗余请求
		if source == "auto" && acc.LastCheckin == today && (acc.CheckinStatus == "done" || acc.CheckinStatus == "already") {
			continue
		}
		candidates = append(candidates, acc)
	}

	results := make([]CheckinRecord, 0, len(candidates))
	for i, acc := range candidates {
		if i > 0 && source == "auto" && e.checkinStagger != nil {
			if wait := e.checkinStagger(); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return results
				case <-timer.C:
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		rec := e.CheckinAccount(ctx, acc.ID, source)
		results = append(results, rec)
	}
	return results
}
