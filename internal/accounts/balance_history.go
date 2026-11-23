package accounts

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	maxBalanceRecordsPerQueue = 300
	abnormalDropMinAmount     = 500.0
	abnormalDropAvgPerCall    = 200.0
)

// 余额变动归因类型。
const (
	BalanceChangeCheckinGain    = "checkin_gain"    // 签到获得
	BalanceChangeOfficialGrant  = "official_grant"  // 官方发放 / 补发
	BalanceChangeGatewayUsage   = "gateway_usage"   // 网关调用消耗
	BalanceChangeExternalDeduct = "external_deduct" // 官方过期 / 砍额度 / 外部消耗
)

// BalanceChangeRecord 记录单次账号余额/积分变动的审计明细。
type BalanceChangeRecord struct {
	Seq           int64   `json:"seq,omitempty"`
	Time          string  `json:"time"`
	AccountID     string  `json:"account_id"`
	AccountLabel  string  `json:"account_label"`
	Region        string  `json:"region,omitempty"`
	Before        float64 `json:"before"`
	After         float64 `json:"after"`
	Delta         float64 `json:"delta"`
	ChangeType    string  `json:"change_type"`
	ChangeLabel   string  `json:"change_label"`
	Trigger       string  `json:"trigger"`
	IntervalCalls int64   `json:"interval_calls"`
	Note          string  `json:"note"`
}

// BalanceHistorySplit 表示拆分后的双队列余额变动明细（调用消耗 vs 官方/活动/异常变动）。
type BalanceHistorySplit struct {
	Usage         []BalanceChangeRecord `json:"usage_history"`
	Events        []BalanceChangeRecord `json:"event_history"`
	UsageCount    int                   `json:"usage_count"`
	EventCount    int                   `json:"event_count"`
	AbnormalCount int                   `json:"abnormal_count"`
}

// BalanceSnapshot 持久化账号最近一次观测到的余额基准，避免重启后丢失对比基准。
type BalanceSnapshot struct {
	Balance   float64 `json:"balance"`
	UpdatedAt string  `json:"updated_at,omitempty"`
}

type balanceHistoryDisk struct {
	Snapshots     map[string]BalanceSnapshot       `json:"snapshots"`
	UsageRecords  map[string][]BalanceChangeRecord `json:"usage_records"`
	EventRecords  map[string][]BalanceChangeRecord `json:"event_records"`
	LegacyRecords map[string][]BalanceChangeRecord `json:"records,omitempty"` // 兼容旧版单队列数据读取迁移
}

func isUsageBalanceChange(changeType string) bool {
	return strings.TrimSpace(changeType) == BalanceChangeGatewayUsage
}

func (p *Pool) balanceHistoryPath() string {
	dataDir := "./data"
	if p.cfgMgr != nil {
		if d := strings.TrimSpace(p.cfgMgr.Get().DataDir); d != "" {
			dataDir = d
		}
	}
	return filepath.Join(dataDir, "balance_history.json")
}

func roundBalance2(v float64) float64 {
	return math.Round(v*100) / 100
}

func formatBalanceNum(v float64) string {
	r := roundBalance2(v)
	if math.Abs(r-math.Round(r)) < 0.005 {
		return fmt.Sprintf("%.0f", r)
	}
	return fmt.Sprintf("%.2f", r)
}

func triggerDisplayName(trigger string) string {
	switch strings.ToLower(strings.TrimSpace(trigger)) {
	case "auto":
		return "定时巡检"
	case "manual":
		return "手动刷新"
	case "checkin":
		return "签到后刷新"
	case "login", "import":
		return "登录/入库刷新"
	case "startup":
		return "启动巡检"
	default:
		if strings.TrimSpace(trigger) == "" {
			return "余额刷新"
		}
		return strings.TrimSpace(trigger)
	}
}

func classifyBalanceChange(delta float64, intervalCalls int64, trigger string, recentCheckin bool, lastCheckinPoints int64) (changeType, changeLabel, note string) {
	trig := strings.ToLower(strings.TrimSpace(trigger))
	if delta > 0 {
		matchPoints := lastCheckinPoints > 0 && math.Abs(delta-float64(lastCheckinPoints)) < 0.01
		if trig == "checkin" || recentCheckin || matchPoints {
			return BalanceChangeCheckinGain, "每日签到奖励",
				fmt.Sprintf("每日签到奖励（+%s 积分，由签到动作触发）", formatBalanceNum(delta))
		}
		return BalanceChangeOfficialGrant, "系统赠送 / 活动发放",
			fmt.Sprintf("系统赠送 / 活动发放（+%s 积分，非签到动作触发的余额增加）", formatBalanceNum(delta))
	}
	drop := -delta
	if intervalCalls <= 0 {
		return BalanceChangeExternalDeduct, "无调用扣减（疑似赠送积分过期 / 上游回收 / 其他端消费）",
			"距离上次查余额期间本网关零调用，余额减少疑似赠送积分过期、上游回收或在其他端消费"
	}
	avgPerCall := drop / float64(intervalCalls)
	if drop >= abnormalDropMinAmount && avgPerCall >= abnormalDropAvgPerCall {
		return BalanceChangeExternalDeduct, "无调用扣减（疑似赠送积分过期 / 上游回收 / 其他端消费）",
			fmt.Sprintf("期间本网关仅调用 %d 次却扣减 %s 积分，疑似赠送积分过期、上游回收或在其他端消费", intervalCalls, formatBalanceNum(drop))
	}
	label := fmt.Sprintf("对话调用消耗（期间共 %d 次请求）", intervalCalls)
	return BalanceChangeGatewayUsage, label,
		fmt.Sprintf("对话调用消耗（期间共 %d 次请求，消耗 %s 积分）", intervalCalls, formatBalanceNum(drop))
}

func trimBalanceQueue(list []BalanceChangeRecord) []BalanceChangeRecord {
	if len(list) > maxBalanceRecordsPerQueue {
		return list[:maxBalanceRecordsPerQueue]
	}
	return list
}

func (p *Pool) ensureBalanceMapsLocked() {
	if p.balanceSnapshots == nil {
		p.balanceSnapshots = make(map[string]BalanceSnapshot)
	}
	if p.balanceUsage == nil {
		p.balanceUsage = make(map[string][]BalanceChangeRecord)
	}
	if p.balanceEvents == nil {
		p.balanceEvents = make(map[string][]BalanceChangeRecord)
	}
}

func (p *Pool) loadBalanceHistoryLocked() {
	p.ensureBalanceMapsLocked()
	raw, err := os.ReadFile(p.balanceHistoryPath())
	if err != nil {
		return
	}
	var disk balanceHistoryDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		return
	}
	if disk.Snapshots != nil {
		p.balanceSnapshots = disk.Snapshots
	}
	for id, list := range disk.UsageRecords {
		p.balanceUsage[id] = trimBalanceQueue(append([]BalanceChangeRecord(nil), list...))
		for _, r := range list {
			if r.Seq > p.balanceSeq {
				p.balanceSeq = r.Seq
			}
		}
	}
	for id, list := range disk.EventRecords {
		p.balanceEvents[id] = trimBalanceQueue(append([]BalanceChangeRecord(nil), list...))
		for _, r := range list {
			if r.Seq > p.balanceSeq {
				p.balanceSeq = r.Seq
			}
		}
	}
	migrated := false
	for id, legacy := range disk.LegacyRecords {
		if len(legacy) == 0 {
			continue
		}
		if len(p.balanceUsage[id]) > 0 || len(p.balanceEvents[id]) > 0 {
			continue
		}
		var uList, eList []BalanceChangeRecord
		for _, rec := range legacy {
			if isUsageBalanceChange(rec.ChangeType) {
				uList = append(uList, rec)
			} else {
				eList = append(eList, rec)
			}
		}
		if len(uList) > 0 {
			p.balanceUsage[id] = trimBalanceQueue(uList)
		}
		if len(eList) > 0 {
			p.balanceEvents[id] = trimBalanceQueue(eList)
		}
		migrated = true
	}
	if migrated {
		_ = p.saveBalanceHistoryLocked()
	}

	for id, snap := range p.balanceSnapshots {
		if acc, ok := p.items[id]; ok {
			acc.Balance = snap.Balance
			acc.BalanceKnown = true
			if snap.UpdatedAt != "" {
				if t, errT := time.Parse("2006-01-02 15:04:05", snap.UpdatedAt); errT == nil {
					acc.BalanceAt = t
				}
			}
			acc.LastBalanceReqs = acc.TotalRequests
		}
	}
}

func (p *Pool) saveBalanceHistoryLocked() error {
	p.ensureBalanceMapsLocked()
	disk := balanceHistoryDisk{
		Snapshots:    p.balanceSnapshots,
		UsageRecords: p.balanceUsage,
		EventRecords: p.balanceEvents,
	}
	b, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	path := p.balanceHistoryPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (p *Pool) applyBalanceUpdateLocked(acc *Account, newBal float64, trigger string) {
	if acc == nil {
		return
	}
	p.ensureBalanceMapsLocked()

	now := time.Now()
	nowStr := now.Format("2006-01-02 15:04:05")
	if strings.TrimSpace(trigger) == "" {
		trigger = "manual"
	}

	hadPrev := acc.BalanceKnown
	prevBal := acc.Balance
	if !hadPrev {
		if snap, ok := p.balanceSnapshots[acc.ID]; ok {
			hadPrev = true
			prevBal = snap.Balance
		}
	}

	intervalCalls := acc.TotalRequests - acc.LastBalanceReqs
	if intervalCalls < 0 {
		intervalCalls = 0
	}

	acc.Balance = newBal
	acc.BalanceKnown = true
	acc.BalanceAt = now
	acc.LastBalanceReqs = acc.TotalRequests

	p.balanceSnapshots[acc.ID] = BalanceSnapshot{
		Balance:   roundBalance2(newBal),
		UpdatedAt: nowStr,
	}

	if hadPrev {
		delta := roundBalance2(newBal - prevBal)
		if math.Abs(delta) >= 0.005 {
			recentCheckin := !acc.LastCheckinAt.IsZero() && now.Sub(acc.LastCheckinAt) <= 10*time.Minute
			cType, cLabel, note := classifyBalanceChange(delta, intervalCalls, trigger, recentCheckin, acc.LastCheckinPoints)
			if cType == BalanceChangeCheckinGain {
				acc.LastCheckinAt = time.Time{}
				acc.LastCheckinPoints = 0
			}
			p.balanceSeq++
			rec := BalanceChangeRecord{
				Seq:           p.balanceSeq,
				Time:          nowStr,
				AccountID:     acc.ID,
				AccountLabel:  acc.Cred.DisplayLabel(),
				Region:        string(acc.Cred.Region),
				Before:        roundBalance2(prevBal),
				After:         roundBalance2(newBal),
				Delta:         delta,
				ChangeType:    cType,
				ChangeLabel:   cLabel,
				Trigger:       strings.ToLower(strings.TrimSpace(trigger)),
				IntervalCalls: intervalCalls,
				Note:          note,
			}
			if isUsageBalanceChange(cType) {
				p.balanceUsage[acc.ID] = trimBalanceQueue(append([]BalanceChangeRecord{rec}, p.balanceUsage[acc.ID]...))
			} else {
				p.balanceEvents[acc.ID] = trimBalanceQueue(append([]BalanceChangeRecord{rec}, p.balanceEvents[acc.ID]...))
			}
		}
	}

	_ = p.saveBalanceHistoryLocked()
}

func (p *Pool) deleteBalanceHistoryLocked(accountID string) {
	changed := false
	if _, ok := p.balanceSnapshots[accountID]; ok {
		delete(p.balanceSnapshots, accountID)
		changed = true
	}
	if _, ok := p.balanceUsage[accountID]; ok {
		delete(p.balanceUsage, accountID)
		changed = true
	}
	if _, ok := p.balanceEvents[accountID]; ok {
		delete(p.balanceEvents, accountID)
		changed = true
	}
	if changed {
		_ = p.saveBalanceHistoryLocked()
	}
}

func copyRecordsWithLimit(src []BalanceChangeRecord, limit int) []BalanceChangeRecord {
	if len(src) == 0 {
		return []BalanceChangeRecord{}
	}
	if limit > 0 && len(src) > limit {
		src = src[:limit]
	}
	out := make([]BalanceChangeRecord, len(src))
	copy(out, src)
	return out
}

func sortBalanceRecordsDesc(list []BalanceChangeRecord) {
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].Time != list[j].Time {
			return list[i].Time > list[j].Time
		}
		return list[i].Seq > list[j].Seq
	})
}

func mergeQueuesSorted(m map[string][]BalanceChangeRecord, limit int) []BalanceChangeRecord {
	merged := make([]BalanceChangeRecord, 0, 64)
	for _, list := range m {
		merged = append(merged, list...)
	}
	sortBalanceRecordsDesc(merged)
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// BalanceHistorySplit 返回拆分后的双队列余额变动记录（调用消耗 vs 官方/活动/异常变动，均按时间倒序）。
func (p *Pool) BalanceHistorySplit(accountID string, limit int) BalanceHistorySplit {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if limit <= 0 || limit > maxBalanceRecordsPerQueue {
		limit = maxBalanceRecordsPerQueue
	}
	id := strings.TrimSpace(accountID)
	var usage, events []BalanceChangeRecord
	if id != "" {
		usage = copyRecordsWithLimit(p.balanceUsage[id], limit)
		events = copyRecordsWithLimit(p.balanceEvents[id], limit)
	} else {
		usage = mergeQueuesSorted(p.balanceUsage, limit)
		events = mergeQueuesSorted(p.balanceEvents, limit)
	}
	abnormal := 0
	for _, r := range events {
		if r.ChangeType == BalanceChangeExternalDeduct {
			abnormal++
		}
	}
	return BalanceHistorySplit{
		Usage:         usage,
		Events:        events,
		UsageCount:    len(usage),
		EventCount:    len(events),
		AbnormalCount: abnormal,
	}
}

// ClearBalanceHistory 一键清空指定账号的积分变动流水（保留最新余额基准，确保后续变动仍能准确比对）。
func (p *Pool) ClearBalanceHistory(accountID string) int {
	id := strings.TrimSpace(accountID)
	if id == "" {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureBalanceMapsLocked()
	cleared := len(p.balanceUsage[id]) + len(p.balanceEvents[id])
	delete(p.balanceUsage, id)
	delete(p.balanceEvents, id)
	if cleared > 0 {
		_ = p.saveBalanceHistoryLocked()
	}
	return cleared
}

// BalanceHistory 返回合并后的余额变动审计记录（按时间倒序，向后兼容）。
func (p *Pool) BalanceHistory(accountID string, limit int) []BalanceChangeRecord {
	if limit <= 0 {
		limit = maxBalanceRecordsPerQueue
	}
	split := p.BalanceHistorySplit(accountID, maxBalanceRecordsPerQueue)
	merged := make([]BalanceChangeRecord, 0, len(split.Usage)+len(split.Events))
	merged = append(merged, split.Usage...)
	merged = append(merged, split.Events...)
	sortBalanceRecordsDesc(merged)
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}
