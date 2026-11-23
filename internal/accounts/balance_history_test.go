package accounts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/protocol"
)

func TestPoolBalanceHistorySplitQueuesCapLegacyMigrationAndDelete(t *testing.T) {
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgMgr, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	_, err = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		return nil
	})
	if err != nil {
		t.Fatalf("Update config: %v", err)
	}

	pool, err := NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	view, err := pool.SaveCredential(&Credential{
		Region:      protocol.RegionGlobal,
		AccessToken: "tok_v101_bal",
		Alias:       "审计号",
	}, nil)
	if err != nil {
		t.Fatalf("SaveCredential: %v", err)
	}

	bump := func(bal float64, addReqs int64, trigger string) {
		pool.mu.Lock()
		pool.items[view.ID].TotalRequests += addReqs
		pool.mu.Unlock()
		pool.UpdateBalanceWithTrigger(view.ID, bal, trigger)
	}

	bump(5000, 0, "login")
	bump(5000, 0, "auto")
	if s := pool.BalanceHistorySplit(view.ID, 300); s.UsageCount != 0 || s.EventCount != 0 {
		t.Fatalf("期望 0 条，实际 %+v", s)
	}

	bump(5100, 0, "checkin")
	bump(5600, 0, "auto")
	bump(5540, 3, "auto")
	bump(4900, 0, "auto")
	bump(3900, 1, "manual")

	s := pool.BalanceHistorySplit(view.ID, 300)
	if s.UsageCount != 1 || s.EventCount != 4 || s.AbnormalCount != 2 {
		t.Fatalf("双队列分流不符: %+v", s)
	}

	// 写入 330 条调用消耗，队列 A 裁剪 300 且不挤压队列 B
	for i := 0; i < 330; i++ {
		bump(float64(3899-i), 1, "auto")
	}
	s2 := pool.BalanceHistorySplit(view.ID, 500)
	if s2.UsageCount != maxBalanceRecordsPerQueue || s2.EventCount != 4 {
		t.Fatalf("高频调用消耗不应挤压队列 B: usage=%d event=%d", s2.UsageCount, s2.EventCount)
	}

	// 写入 320 条异常变动，队列 B 裁剪 300 且不挤压队列 A
	for i := 0; i < 320; i++ {
		bump(float64(3500-i), 0, "auto")
	}
	s3 := pool.BalanceHistorySplit(view.ID, 500)
	if s3.UsageCount != maxBalanceRecordsPerQueue || s3.EventCount != maxBalanceRecordsPerQueue {
		t.Fatalf("双队列应各保留 300 条: usage=%d event=%d", s3.UsageCount, s3.EventCount)
	}

	// 重载恢复 + 删除清理
	pool2, err := NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewPool reload: %v", err)
	}
	if sr := pool2.BalanceHistorySplit(view.ID, 500); sr.UsageCount != maxBalanceRecordsPerQueue || sr.EventCount != maxBalanceRecordsPerQueue {
		t.Fatalf("重载后双队列数量不符: %+v", sr)
	}
	if err := pool2.Delete(view.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if sd := pool2.BalanceHistorySplit(view.ID, 500); sd.UsageCount != 0 || sd.EventCount != 0 {
		t.Fatalf("删除账号后双队列应清空: %+v", sd)
	}

	// 兼容旧版单队列 records 字段自动分流迁移
	legacyDir := t.TempDir()
	legacyData := filepath.Join(legacyDir, "data")
	legacyAuth := filepath.Join(legacyDir, "auths")
	_ = os.MkdirAll(legacyData, 0o755)
	_ = os.MkdirAll(legacyAuth, 0o755)
	legacyCfg, _ := config.Load(filepath.Join(legacyDir, "config.json"))
	_, _ = legacyCfg.Update(func(c *config.Config) error {
		c.AuthDir = legacyAuth
		c.DataDir = legacyData
		return nil
	})
	legacyJSON := `{"snapshots":{"a1":{"balance":900}},"records":{"a1":[{"time":"2026-11-22 10:00:00","account_id":"a1","change_type":"external_deduct","delta":-100},{"time":"2026-11-22 09:00:00","account_id":"a1","change_type":"gateway_usage","delta":-20}]}}`
	_ = os.WriteFile(filepath.Join(legacyData, "balance_history.json"), []byte(legacyJSON), 0o600)
	poolLeg, err := NewPool(legacyCfg, nil)
	if err != nil {
		t.Fatalf("NewPool legacy: %v", err)
	}
	if sl := poolLeg.BalanceHistorySplit("a1", 300); sl.UsageCount != 1 || sl.EventCount != 1 || sl.AbnormalCount != 1 {
		t.Fatalf("旧版数据迁移异常: %+v", sl)
	}
}

func TestBalanceHistoryFourAttributionsAndClear(t *testing.T) {
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	_ = os.MkdirAll(authDir, 0o755)
	cfgMgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	_, _ = cfgMgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		return nil
	})
	pool, err := NewPool(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	view, err := pool.SaveCredential(&Credential{
		Region:      protocol.RegionCN,
		AccessToken: "tok_v110_attr",
		Alias:       "归因测试号",
	}, nil)
	if err != nil {
		t.Fatalf("SaveCredential: %v", err)
	}

	// 初始基准 1000
	pool.UpdateBalanceWithTrigger(view.ID, 1000, "startup")

	// 场景 1：每日签到奖励（+50）
	pool.UpdateCheckinStateWithPoints(view.ID, "2026-11-23", "done", "签到成功", 50)
	pool.UpdateBalanceWithTrigger(view.ID, 1050, "checkin")

	// 场景 2：系统赠送 / 活动发放（非签到 +10000）
	pool.UpdateBalanceWithTrigger(view.ID, 11050, "auto")

	// 场景 3：对话调用消耗（期间 4 次请求，-80）
	pool.mu.Lock()
	pool.items[view.ID].TotalRequests += 4
	pool.mu.Unlock()
	pool.UpdateBalanceWithTrigger(view.ID, 10970, "auto")

	// 场景 4：无调用扣减（期间 0 次请求，-300）
	pool.UpdateBalanceWithTrigger(view.ID, 10670, "manual")

	hist := pool.BalanceHistory(view.ID, 50)
	if len(hist) != 4 {
		t.Fatalf("期望 4 条积分流水，实际 %d: %+v", len(hist), hist)
	}
	// 倒序检查四条归因标签
	wantTypes := []string{
		BalanceChangeExternalDeduct,
		BalanceChangeGatewayUsage,
		BalanceChangeOfficialGrant,
		BalanceChangeCheckinGain,
	}
	wantSubstr := []string{
		"无调用扣减",
		"对话调用消耗（期间共 4 次请求）",
		"系统赠送 / 活动发放",
		"每日签到奖励",
	}
	for i := range wantTypes {
		if hist[i].ChangeType != wantTypes[i] || !strings.Contains(hist[i].ChangeLabel, wantSubstr[i]) {
			t.Fatalf("第 %d 条归因不符: got type=%q label=%q, want type=%q substr=%q",
				i, hist[i].ChangeType, hist[i].ChangeLabel, wantTypes[i], wantSubstr[i])
		}
	}

	// 验证一键清空单号流水（保留当前余额基准 10670，再次变动仍可准确计算差值）
	cleared := pool.ClearBalanceHistory(view.ID)
	if cleared != 4 {
		t.Fatalf("期望清空 4 条流水，实际 %d", cleared)
	}
	if len(pool.BalanceHistory(view.ID, 50)) != 0 {
		t.Fatalf("清空后应无历史记录")
	}
	pool.UpdateBalanceWithTrigger(view.ID, 10770, "auto")
	afterClear := pool.BalanceHistory(view.ID, 50)
	if len(afterClear) != 1 || afterClear[0].Before != 10670 || afterClear[0].After != 10770 || afterClear[0].Delta != 100 {
		t.Fatalf("清空后保留基准校验失败: %+v", afterClear)
	}
}
