package services

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

func tuned(kv ...string) domain.PipelineSpec {
	s := domain.DefaultSpec()
	s.Tuning = map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		s.Tuning[kv[i]] = kv[i+1]
	}
	return s
}

func TestSettingVersionCheck(t *testing.T) {
	if c := settingVersionCheck(tuned("debezium.source.poll.interval.ms", "250"), "quay.io/debezium/server:3.7.0.Final"); c != nil {
		t.Fatal("no version bound, nothing to check", c)
	}
	spec := tuned("debezium.source.skip.messages.without.change", "true", "debezium.source.max.queue.size.in.bytes", "1048576")
	if c := settingVersionCheck(spec, "quay.io/debezium/server:3.7.0.Final"); c == nil || c.Status != checks.Pass {
		t.Fatal(c)
	}
	if c := settingVersionCheck(spec, "quay.io/debezium/server:1.9.7.Final"); c == nil || c.Status != checks.Fail || !c.Blocking ||
		!strings.Contains(c.Message, "debezium.source.skip.messages.without.change (from Debezium 2.0)") || strings.Contains(c.Message, "in.bytes") {
		t.Fatal(c)
	}
	// C15: an image BeziSync can't place is Unknown, never a pass.
	if c := settingVersionCheck(spec, "registry.example/our-debezium:latest"); c == nil || c.Status != checks.Unknown || c.Blocking {
		t.Fatal(c)
	}
}

func TestThreadsCheck(t *testing.T) {
	draft := &models.Pipeline{}
	spec := tuned("debezium.source.snapshot.max.threads", "4")
	if c := threadsCheck(draft, tuned(), 100, 0, true); c != nil {
		t.Fatal("one thread is the stream's own connection", c)
	}
	// C14: unreadable is Unknown.
	if c := threadsCheck(draft, spec, 0, 0, false); c == nil || c.Status != checks.Unknown {
		t.Fatal(c)
	}
	if c := threadsCheck(draft, spec, 100, 10, true); c == nil || c.Status != checks.Pass || !strings.Contains(c.Message, "need 5 connections; 90 of 100") {
		t.Fatal(c)
	}
	if c := threadsCheck(draft, spec, 100, 97, true); c == nil || c.Status != checks.Warn || !strings.Contains(c.Fix.Code, "at most 2") {
		t.Fatal(c)
	}
	// No copy: the threads never run.
	noCopy := spec
	noCopy.Snapshot.Mode = "no_data"
	if c := threadsCheck(draft, noCopy, 10, 9, true); c != nil {
		t.Fatal(c)
	}
	v := uuid.New()
	if c := threadsCheck(&models.Pipeline{DeployedVersionID: &v}, spec, 10, 9, true); c != nil {
		t.Fatal("a redeploy copies nothing", c)
	}
}

func TestMemoryChecks(t *testing.T) {
	spec := domain.DefaultSpec() // small: 512m heap, queue 8192
	compiled := &compiler.Compiled{Properties: map[string]string{"debezium.source.max.queue.size": "8192"}}
	// C17: estimated from the tables, and said so; nothing without an estimate for BeziSync's own queue.
	if out := memoryChecks(spec, compiled, 0); out != nil {
		t.Fatal(out)
	}
	if out := memoryChecks(spec, compiled, 200); len(out) != 1 || out[0].Status != checks.Pass || !strings.Contains(out[0].Message, "estimated") {
		t.Fatal(out)
	}
	if out := memoryChecks(spec, compiled, 20_000); len(out) < 1 || out[0].Status != checks.Warn || !strings.Contains(out[0].Message, "average row of 20000 bytes") {
		t.Fatal(out)
	}
	// A byte limit caps it.
	capped := &compiler.Compiled{Properties: map[string]string{"debezium.source.max.queue.size": "8192", "debezium.source.max.queue.size.in.bytes": "33554432"}}
	if out := memoryChecks(spec, capped, 20_000); out[0].Status != checks.Pass {
		t.Fatal(out)
	}
	// A tuned queue without an estimate is Unknown.
	tq := tuned("debezium.source.max.queue.size", "65536")
	if out := memoryChecks(tq, compiled, 0); len(out) != 1 || out[0].Status != checks.Unknown {
		t.Fatal(out)
	}
	// A fetch of big rows that crowds the heap.
	big := spec
	big.Snapshot.FetchSize = 10_000
	if out := memoryChecks(big, compiled, 20_000); len(out) != 2 || out[1].Code != "setting_fetch_memory" {
		t.Fatal(out)
	}
}

func TestRecommendations(t *testing.T) {
	draft := &models.Pipeline{}
	now := time.Now()
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"shop.a", "shop.b", "shop.c"}
	recs := recommendations(draft, spec, "postgresql", 30_000_000, 3, 100, 20, true, now)
	if len(recs) != 1 || recs[0].Key != "debezium.source.snapshot.max.threads" || recs[0].Value != "3" || !recs[0].EvidenceAt.Equal(now) {
		t.Fatal(recs)
	}
	// No evidence of connections, small tables, a single table, a source without threads: none.
	for name, r := range map[string][]Recommendation{
		"unknown connections": recommendations(draft, spec, "postgresql", 30_000_000, 3, 0, 0, false, now),
		"small":               recommendations(draft, spec, "postgresql", 1000, 3, 100, 20, true, now),
		"one table":           recommendations(draft, spec, "postgresql", 30_000_000, 1, 100, 20, true, now),
		"spanner":             recommendations(draft, spec, "spanner", 30_000_000, 3, 100, 20, true, now),
		"busy source":         recommendations(draft, spec, "postgresql", 30_000_000, 3, 100, 98, true, now),
	} {
		if len(r) != 0 {
			t.Errorf("%s: %v", name, r)
		}
	}
	// C18: skip unchanged updates only with columns left out.
	if r := recommendations(draft, domain.DefaultSpec(), "postgresql", 0, 1, 0, 0, false, now); len(r) != 0 {
		t.Fatal(r)
	}
	excl := domain.DefaultSpec()
	excl.Source.Columns = map[string]domain.ColumnRules{"shop.orders": {Exclude: []string{"notes"}}}
	if r := recommendations(draft, excl, "postgresql", 0, 1, 0, 0, false, now); len(r) != 1 || r[0].Key != "debezium.source.skip.messages.without.change" {
		t.Fatal(r)
	}
	excl.Tuning = map[string]string{"debezium.source.skip.messages.without.change": "false"}
	if r := recommendations(draft, excl, "postgresql", 0, 1, 0, 0, false, now); len(r) != 0 {
		t.Fatal("a choice already made is not recommended again", r)
	}
	_ = connectors.Discovery{}
}

func TestAverageRow(t *testing.T) {
	rows, bytes := int64(1000), int64(200_000)
	found := connectors.Discovery{Tables: []connectors.TableInfo{{Schema: "shop", Name: "orders", RowsEstimate: &rows, BytesEstimate: &bytes}, {Schema: "shop", Name: "other"}}}
	if avg, n := averageRow(found, []string{"shop.orders", "shop.other"}); avg != 200 || n != 1000 {
		t.Fatal(avg, n)
	}
	if avg, _ := averageRow(found, []string{"shop.other"}); avg != 0 {
		t.Fatal(avg)
	}
}
