package connectors

import (
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func TestPgImpact(t *testing.T) {
	const gb = int64(1 << 30)
	cases := []struct {
		name      string
		active    *bool
		retained  *float64
		safe      *float64
		walStatus *string
		wantRisk  string
		wantLost  bool
		wantNoCap bool
		wantRoom  *int64
	}{
		{name: "healthy, capped", active: ptr(true), retained: ptr(1e6), safe: ptr(5e8), walStatus: ptr("reserved"), wantRoom: ptr(int64(5e8))},
		{name: "no cap on 13+", active: ptr(true), retained: ptr(1e6), walStatus: ptr("reserved"), wantNoCap: true},
		{name: "before 13: room unknown, never no-cap", active: ptr(true), retained: ptr(1e6)},
		{name: "negative safe size clamps to zero", active: ptr(true), retained: ptr(1e6), safe: ptr(-10.0), walStatus: ptr("extended"), wantRoom: ptr(int64(0))},
		{name: "unreserved", active: ptr(true), retained: ptr(1e6), safe: ptr(0.0), walStatus: ptr("unreserved"), wantRisk: "about to be invalidated", wantRoom: ptr(int64(0))},
		{name: "lost is not no-cap", active: ptr(false), walStatus: ptr("lost"), wantRisk: "changes were lost", wantLost: true},
		{name: "over the limit", active: ptr(false), retained: ptr(float64(2 * gb)), safe: ptr(1e9), walStatus: ptr("reserved"), wantRisk: "limit 1.0 GB", wantRoom: ptr(int64(1e9))},
		{name: "inactive slot, nothing retained yet", wantRoom: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pgImpact("bz_abc", c.active, c.retained, c.safe, c.walStatus, gb)
			if got.Slot == nil || *got.Slot != "bz_abc" || got.LimitBytes == nil || *got.LimitBytes != gb {
				t.Fatalf("slot/limit: %+v", got)
			}
			if got.NoCap != c.wantNoCap || got.Lost != c.wantLost {
				t.Fatalf("no_cap=%v lost=%v", got.NoCap, got.Lost)
			}
			if (got.HeadroomBytes == nil) != (c.wantRoom == nil) || (c.wantRoom != nil && *got.HeadroomBytes != *c.wantRoom) {
				t.Fatalf("headroom %v, want %v", got.HeadroomBytes, c.wantRoom)
			}
			if c.wantRisk == "" && got.Risk != nil || c.wantRisk != "" && (got.Risk == nil || !strings.Contains(*got.Risk, c.wantRisk)) {
				t.Fatalf("risk %v, want %q", got.Risk, c.wantRisk)
			}
		})
	}
}

func TestMysqlImpact(t *testing.T) {
	logs := []binlogFile{{"bin.000001", 100}, {"bin.000002", 200}, {"bin.000003", 300}}
	if got := mysqlImpact(nil, ptr("bin.000001"), 1000, false); got.Summary != "No binary logs reported." || got.LimitBytes != nil {
		t.Fatalf("%+v", got)
	}
	if got := mysqlImpact(logs, nil, 1000, false); !strings.Contains(got.Summary, "position not stored yet") || got.RetainedBytes != nil {
		t.Fatalf("%+v", got)
	}
	cases := []struct {
		name         string
		logs         []binlogFile
		file         string
		limit        int64
		wantStatus   string
		wantRetained int64
		wantRisk     string
	}{
		{name: "newest file", logs: logs, file: "bin.000003", limit: 1000, wantStatus: "ok", wantRetained: 300},
		{name: "middle file", logs: logs, file: "bin.000002", limit: 1000, wantStatus: "ok", wantRetained: 500},
		{name: "oldest of several", logs: logs, file: "bin.000001", limit: 1000, wantStatus: "oldest", wantRetained: 600, wantRisk: "oldest binlog"},
		{name: "a lone file is never purged", logs: logs[:1], file: "bin.000001", limit: 1000, wantStatus: "ok", wantRetained: 100},
		{name: "over the limit", logs: logs, file: "bin.000002", limit: 400, wantStatus: "ok", wantRetained: 500, wantRisk: "waiting to be read"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mysqlImpact(c.logs, ptr(c.file), c.limit, false)
			if got.Lost || got.LogStatus == nil || *got.LogStatus != c.wantStatus || *got.RetainedBytes != c.wantRetained {
				t.Fatalf("%+v", got)
			}
			if *got.ReadingFile != c.file || *got.LimitBytes != c.limit {
				t.Fatalf("file/limit: %+v", got)
			}
			if c.wantRisk == "" && got.Risk != nil || c.wantRisk != "" && (got.Risk == nil || !strings.Contains(*got.Risk, c.wantRisk)) {
				t.Fatalf("risk %v, want %q", got.Risk, c.wantRisk)
			}
		})
	}
	purged := mysqlImpact(logs, ptr("bin.000000"), 1000, false)
	if !purged.Lost || purged.Risk == nil || *purged.ReadingFile != "bin.000000" || purged.RetainedBytes != nil {
		t.Fatalf("%+v", purged)
	}
}

func TestMongoImpact(t *testing.T) {
	const h = int64(3600)
	cases := []struct {
		name       string
		first      int64
		last       int64
		size, cap  int64
		wantStatus string
		wantWindow *int64
		wantRoom   *int64
		wantRisk   bool
	}{
		{name: "filling", first: 0, last: 2 * h, size: 10, cap: 100, wantStatus: "filling", wantRoom: ptr(int64(90))},
		{name: "full, long window", first: 0, last: 48 * h, size: 95, cap: 100, wantStatus: "window 48 h", wantWindow: ptr(48 * h), wantRoom: ptr(int64(5))},
		{name: "full, short window", first: 0, last: 5 * h, size: 100, cap: 100, wantStatus: "window 5 h", wantWindow: ptr(5 * h), wantRoom: ptr(int64(0)), wantRisk: true},
		{name: "capacity unknown", first: 0, last: h, size: 10, cap: 0, wantStatus: "filling"},
		{name: "clock skew never goes negative", first: 10, last: 5, size: 100, cap: 100, wantStatus: "window 0 h", wantWindow: ptr(int64(0)), wantRoom: ptr(int64(0)), wantRisk: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mongoImpact(c.first, c.last, c.size, c.cap)
			if *got.LogStatus != c.wantStatus || *got.RetainedBytes != c.size || got.LimitBytes != nil {
				t.Fatalf("%+v", got)
			}
			if (got.WindowSeconds == nil) != (c.wantWindow == nil) || (c.wantWindow != nil && *got.WindowSeconds != *c.wantWindow) {
				t.Fatalf("window %v", got.WindowSeconds)
			}
			if (got.HeadroomBytes == nil) != (c.wantRoom == nil) || (c.wantRoom != nil && *got.HeadroomBytes != *c.wantRoom) {
				t.Fatalf("headroom %v", got.HeadroomBytes)
			}
			if (got.Risk != nil) != c.wantRisk {
				t.Fatalf("risk %v", got.Risk)
			}
		})
	}
}

func TestMssqlImpact(t *testing.T) {
	const old, mid, newer = "0000002D00000A380003", "0000002E000001000001", "0000003000000F000002"
	pos := func(s string) *string { return &s }
	cases := []struct {
		name     string
		mins     []string
		position *string
		ret      *int64
		reuse    *string
		used     *int64
		size     *int64
		wantKept *bool
		wantRisk string
		wantLost bool
	}{
		{name: "inside the window", mins: []string{old}, position: pos("0000002e:00000100:0001"), ret: ptr(int64(4320)), reuse: ptr("NOTHING"), wantKept: ptr(true)},
		{name: "exactly at the oldest change", mins: []string{mid}, position: pos("0000002e:00000100:0001"), wantKept: ptr(true)},
		{name: "cleaned up past the position", mins: []string{newer}, position: pos("0000002e:00000100:0001"), wantKept: ptr(false), wantLost: true, wantRisk: "they are lost"},
		{name: "a newer capture instance doesn't make it lost", mins: []string{newer, old}, position: pos("0000002e:00000100:0001"), wantKept: ptr(true)},
		{name: "unreadable capture instance (zeros) is skipped", mins: []string{"00000000000000000000"}, position: pos("0000002e:00000100:0001")},
		{name: "no position stored yet", mins: []string{old}},
		{name: "malformed position stays unknown", mins: []string{old}, position: pos("not-an-lsn")},
		{name: "log full and waiting on replication", reuse: ptr("REPLICATION"), used: ptr(int64(85)), size: ptr(int64(100)), wantRisk: "85% full"},
		{name: "log full for another reason is not CDC's", reuse: ptr("LOG_BACKUP"), used: ptr(int64(95)), size: ptr(int64(100))},
		{name: "nothing readable", wantRisk: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mssqlImpact(c.mins, c.position, c.ret, c.reuse, c.used, c.size)
			if (got.PositionKept == nil) != (c.wantKept == nil) || (c.wantKept != nil && *got.PositionKept != *c.wantKept) {
				t.Fatalf("position_kept %v, want %v", got.PositionKept, c.wantKept)
			}
			if got.Lost != c.wantLost {
				t.Fatalf("lost %v", got.Lost)
			}
			if c.wantRisk == "" && got.Risk != nil || c.wantRisk != "" && (got.Risk == nil || !strings.Contains(*got.Risk, c.wantRisk)) {
				t.Fatalf("risk %v, want %q", got.Risk, c.wantRisk)
			}
			if got.RetainedBytes != nil || got.LimitBytes != nil {
				t.Fatalf("SQL Server keeps nothing per pipeline: %+v", got)
			}
		})
	}
	got := mssqlImpact(nil, nil, ptr(int64(4320)), ptr("NOTHING"), ptr(int64(10)), ptr(int64(100)))
	if *got.WindowSeconds != 3*86400 || *got.LogStatus != "nothing" || *got.LogUsedBytes != 10 || *got.LogSizeBytes != 100 {
		t.Fatalf("%+v", got)
	}
	if mssqlImpact(nil, nil, nil, nil, nil, nil).Summary != "Nothing could be measured (needs db_owner on the database)." {
		t.Fatal("summary")
	}
}

func TestMySQLImpactOnFlinkCDC(t *testing.T) {
	got := mysqlImpact([]binlogFile{{"mysql-bin.000001", 10}, {"mysql-bin.000002", 20}}, nil, 1000, true)
	if !strings.Contains(got.Summary, "Flink CDC keeps its position in its saved state") || got.RetainedBytes != nil || got.Risk != nil {
		t.Fatalf("%+v", got)
	}
}
