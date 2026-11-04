package domain_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

type domainFixture struct {
	Metrics []struct {
		Text   string          `json:"text"`
		Parsed json.RawMessage `json:"parsed"`
	} `json:"metrics"`
	Diagnose []struct {
		Logs   string         `json:"logs"`
		Reason *domain.Reason `json:"reason"`
	} `json:"diagnose"`
	Derive []struct {
		Input   map[string]json.RawMessage `json:"input"`
		Verdict struct {
			Lifecycle string          `json:"lifecycle"`
			Health    string          `json:"health"`
			Summary   string          `json:"summary"`
			Reasons   []domain.Reason `json:"reasons"`
		} `json:"verdict"`
	} `json:"derive"`
	HumanBytes []struct {
		In  *float64 `json:"in"`
		Out string   `json:"out"`
	} `json:"human_bytes"`
	Windows []struct {
		Window   string  `json:"window"`
		At       string  `json:"at"`
		Contains bool    `json:"contains"`
		Next     *string `json:"next"`
	} `json:"windows"`
	Schema []struct {
		Table    string  `json:"table"`
		Kind     string  `json:"kind"`
		Column   *string `json:"column"`
		Before   *string `json:"before"`
		After    *string `json:"after"`
		Breaking bool    `json:"breaking"`
		Describe string  `json:"describe"`
	} `json:"schema"`
	Policies []struct {
		Policies json.RawMessage `json:"policies"`
		Spec     json.RawMessage `json:"spec"`
		KW       struct {
			PII   map[string][]string `json:"pii_columns"`
			Other int                 `json:"other_pipelines_on_source"`
		} `json:"kw"`
		Dump   json.RawMessage `json:"dump"`
		Checks json.RawMessage `json:"checks"`
	} `json:"policies"`
	BadPolicies []struct {
		Input  json.RawMessage `json:"input"`
		Errors []struct {
			Loc  []string `json:"loc"`
			Type string   `json:"type"`
			Msg  string   `json:"msg"`
		} `json:"errors"`
	} `json:"bad_policies"`
	Manifests []struct {
		Text   string          `json:"text"`
		Parsed json.RawMessage `json:"parsed"`
		Error  *struct {
			Message string           `json:"message"`
			Errors  []map[string]any `json:"errors"`
		} `json:"error"`
	} `json:"manifests"`
	ToYAML struct {
		SpecA json.RawMessage `json:"spec_a"`
		SpecB json.RawMessage `json:"spec_b"`
		YAML  string          `json:"yaml"`
	} `json:"to_yaml"`
	SpecDiff struct {
		New    []string `json:"new"`
		Change []string `json:"change"`
		Same   []string `json:"same"`
	} `json:"spec_diff"`
}

func load(t *testing.T) domainFixture {
	t.Helper()
	var fx domainFixture
	contract.Load(t, "domain", &fx)
	return fx
}

func generic(t *testing.T, raw []byte) any {
	t.Helper()
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func sameJSON(t *testing.T, got any, want []byte) (string, string, bool) {
	t.Helper()
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := pyjson.Dumps(generic(t, raw))
	b, _ := pyjson.Dumps(generic(t, want))
	return a, b, a == b
}

func TestParseMetricsAndDiagnose(t *testing.T) {
	fx := load(t)
	for _, c := range fx.Metrics {
		if a, b, ok := sameJSON(t, domain.ParseMetrics(c.Text).AsDict(), c.Parsed); !ok {
			t.Errorf("parse_metrics(%q)\n got %s\nwant %s", c.Text, a, b)
		}
	}
	for _, c := range fx.Diagnose {
		got := domain.Diagnose(c.Logs)
		if !reflect.DeepEqual(got, c.Reason) {
			t.Errorf("diagnose(%q) = %+v, want %+v", c.Logs, got, c.Reason)
		}
	}
}

func metricsFrom(t *testing.T, raw json.RawMessage) *domain.Metrics {
	t.Helper()
	if string(raw) == "null" {
		return nil
	}
	var m struct {
		Connected       *bool    `json:"connected"`
		SnapshotRunning *bool    `json:"snapshot_running"`
		MSBehind        *float64 `json:"ms_behind_source"`
		QueueRemaining  *float64 `json:"queue_remaining"`
		QueueTotal      *float64 `json:"queue_total"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return &domain.Metrics{Connected: m.Connected, SnapshotRunning: m.SnapshotRunning, MSBehindSource: m.MSBehind,
		QueueRemaining: m.QueueRemaining, QueueTotal: m.QueueTotal}
}

func TestDeriveMatchesPython(t *testing.T) {
	fx := load(t)
	for i, c := range fx.Derive {
		in := domain.DeriveInput{}
		str := func(k string) string {
			var s *string
			if raw, ok := c.Input[k]; ok {
				_ = json.Unmarshal(raw, &s)
			}
			if s == nil {
				return ""
			}
			return *s
		}
		now, _ := pytime.Parse(str("now"))
		in.Now = now
		_ = json.Unmarshal(c.Input["deployed"], &in.Deployed)
		in.Desired = str("desired")
		at, _ := pytime.Parse(str("deployed_at"))
		in.DeployedAt = &at
		in.Phase = str("phase")
		_ = json.Unmarshal(c.Input["restarts"], &in.Restarts)
		_ = json.Unmarshal(c.Input["health_up"], &in.HealthUp)
		in.Metrics = metricsFrom(t, c.Input["metrics"])
		in.Logs = str("logs")
		if raw, ok := c.Input["latency_target_ms"]; ok {
			_ = json.Unmarshal(raw, &in.LatencyTargetMS)
		}
		in.RuntimeError = str("runtime_error")
		if raw, ok := c.Input["source_risk"]; ok {
			var r domain.Reason
			_ = json.Unmarshal(raw, &r)
			in.SourceRisk = &r
		}
		in.WorkloadReason = str("workload_reason")
		in.LastLifecycle = str("last_lifecycle")
		got := domain.Derive(in)
		want := c.Verdict
		if got.Lifecycle != want.Lifecycle || got.Health != want.Health || got.Summary != want.Summary ||
			!reflect.DeepEqual(nilIfEmpty(got.Reasons), nilIfEmpty(want.Reasons)) {
			t.Errorf("case %d: got %+v\nwant %+v", i, got, want)
		}
	}
}

func nilIfEmpty(r []domain.Reason) []domain.Reason {
	if len(r) == 0 {
		return nil
	}
	return r
}

func TestHumanBytesWindowsSchema(t *testing.T) {
	fx := load(t)
	for _, c := range fx.HumanBytes {
		if got := domain.HumanBytes(c.In); got != c.Out {
			t.Errorf("human_bytes(%v) = %q, want %q", c.In, got, c.Out)
		}
	}
	for _, c := range fx.Windows {
		at, err := pytime.Parse(c.At)
		if err != nil {
			t.Fatal(err)
		}
		in, err := domain.WindowContains(c.Window, at)
		if err != nil || in != c.Contains {
			t.Errorf("contains(%s, %s) = %v %v", c.Window, c.At, in, err)
		}
		next, _ := domain.WindowNextStart(c.Window, at)
		var got *string
		if next != nil {
			s := pytime.ISOFormat(*next)
			got = &s
		}
		if !reflect.DeepEqual(got, c.Next) {
			t.Errorf("next_start(%s, %s) = %v, want %v", c.Window, c.At, got, c.Next)
		}
	}
	old := domain.Schema{
		"public.a": {"id": {"type": "int", "nullable": false}, "x": {"type": "text", "nullable": true}, "y": {"type": "int"}},
		"public.b": {"id": {"type": "int"}}, "public.c": {},
	}
	new := domain.Schema{
		"public.a": {"id": {"type": "bigint", "nullable": false}, "x": {"type": "text", "nullable": false}, "z": {"type": "jsonb"}},
		"public.c": {"k": {"type": "uuid", "nullable": true}},
	}
	changes := domain.DiffSchema(old, new)
	if len(changes) != len(fx.Schema) {
		t.Fatalf("%d changes, want %d", len(changes), len(fx.Schema))
	}
	for i, c := range changes {
		w := fx.Schema[i]
		if c.Table != w.Table || c.Kind != w.Kind || !reflect.DeepEqual(c.Column, w.Column) || !reflect.DeepEqual(c.Before, w.Before) ||
			!reflect.DeepEqual(c.After, w.After) || c.Breaking != w.Breaking || c.Describe() != w.Describe {
			t.Errorf("change %d: got %+v %q, want %+v", i, c, c.Describe(), w)
		}
	}
}

// retirePIIPolicy adapts a recorded case to the retired require_pii_masking policy (personal
// data is now protected per pipeline): the key leaves the dump and its checks leave the
// report. nil checks mean only those were recorded, so the report is now policy_ok alone.
func retirePIIPolicy(t *testing.T, dump, report []byte) ([]byte, []byte) {
	t.Helper()
	var d map[string]any
	if err := json.Unmarshal(dump, &d); err != nil {
		t.Fatal(err)
	}
	delete(d, "require_pii_masking")
	var cs []map[string]any
	if err := json.Unmarshal(report, &cs); err != nil {
		t.Fatal(err)
	}
	kept := []map[string]any{}
	for _, c := range cs {
		if code, _ := c["code"].(string); !strings.HasPrefix(code, "policy_pii_") {
			kept = append(kept, c)
		}
	}
	outDump, _ := json.Marshal(d)
	if len(kept) == 0 {
		return outDump, nil
	}
	outChecks, _ := json.Marshal(kept)
	return outDump, outChecks
}

func TestPoliciesMatchPython(t *testing.T) {
	fx := load(t)
	rt := uuid.MustParse("12345678-1234-1234-1234-1234567890ab")
	for _, c := range fx.Policies {
		v := &val.V{}
		p, ok := domain.ParsePolicies(v, nil, generic(t, c.Policies))
		if !ok {
			t.Fatalf("%s: %v", c.Policies, v.Errors)
		}
		wantDump, wantChecks := retirePIIPolicy(t, c.Dump, c.Checks)
		if a, b, ok := sameJSON(t, p, wantDump); !ok {
			t.Errorf("dump\n got %s\nwant %s", a, b)
		}
		spec, ok := domain.ParseSpec(v, nil, generic(t, c.Spec))
		if !ok {
			t.Fatal(v.Errors)
		}
		got := domain.EvaluatePolicies(p, spec, &rt, "postgresql", "kafka", c.KW.Other)
		if wantChecks == nil {
			if len(got) != 1 || got[0].Code != "policy_ok" {
				t.Errorf("%s: got %v, want only policy_ok", c.Policies, got)
			}
			continue
		}
		if a, b, ok := sameJSON(t, got, wantChecks); !ok {
			t.Errorf("%s:\n got %s\nwant %s", c.Policies, a, b)
		}
	}
	for _, c := range fx.BadPolicies {
		v := &val.V{}
		if _, ok := domain.ParsePolicies(v, nil, generic(t, c.Input)); ok {
			t.Errorf("%s accepted", c.Input)
			continue
		}
		var got, want []string
		for _, e := range v.Errors {
			got = append(got, e.Message+" @ "+join(e.Loc)+" "+*e.Type)
		}
		for _, e := range c.Errors {
			want = append(want, e.Msg+" @ "+join(e.Loc)+" "+e.Type)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n got %v\nwant %v", c.Input, got, want)
		}
	}
}

func TestManifestsMatchPython(t *testing.T) {
	fx := load(t)
	for _, c := range fx.Manifests {
		docs, err := domain.ParseDocuments(c.Text)
		var parsed []map[string]any
		if err == nil {
			for _, d := range docs {
				p, perr := domain.FromDocument(d)
				if perr != nil {
					err = perr
					break
				}
				parsed = append(parsed, map[string]any{
					"name": p.Name, "description": p.Description, "owner": p.Owner, "labels": p.Labels,
					"source": p.SourceConnection, "sink": p.SinkConnection, "runtime": p.Runtime, "spec": p.Spec,
				})
			}
		}
		if c.Error != nil {
			me := &domain.ManifestError{}
			ok := errors.As(err, &me)
			if !ok {
				t.Errorf("%q: got %v, want error %q", c.Text, err, c.Error.Message)
				continue
			}
			if c.Error.Message != me.Message && !(len(me.Message) > 14 && me.Message[:14] == "Not valid YAML" && c.Error.Message[:14] == "Not valid YAML") {
				t.Errorf("%q: message %q, want %q", c.Text, me.Message, c.Error.Message)
			}
			want, _ := json.Marshal(c.Error.Errors)
			if a, b, ok := sameJSON(t, nilIfNone(me.Errors), want); !ok {
				t.Errorf("%q: errors\n got %s\nwant %s", c.Text, a, b)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected %v", c.Text, err)
			continue
		}
		if a, b, ok := sameJSON(t, parsed, c.Parsed); !ok {
			t.Errorf("%q:\n got %s\nwant %s", c.Text, a, b)
		}
	}
	specA, _ := domain.ParseSpec(&val.V{}, nil, generic(t, fx.ToYAML.SpecA))
	specB, _ := domain.ParseSpec(&val.V{}, nil, generic(t, fx.ToYAML.SpecB))
	sink, rt := "kafka", "rt"
	doc := domain.ToDocument("p1", "default", "d", "me", map[string]string{"team": "x", "a": "b"}, "pg", &sink, &rt, specA, "")
	doc2 := domain.ToDocument("p2", "default", "", "", map[string]string{}, "pg", nil, nil, specB, "")
	if got := domain.ToYAML([]any{doc, doc2}); got != fx.ToYAML.YAML {
		t.Errorf("to_yaml\n--- got\n%s\n--- want\n%s", got, fx.ToYAML.YAML)
	}
	if got := domain.SpecDiff(nil, specA, "v0", "v1"); !reflect.DeepEqual(got, fx.SpecDiff.New) {
		t.Errorf("diff new\n got %q\nwant %q", got, fx.SpecDiff.New)
	}
	if got := domain.SpecDiff(&specA, specB, "v1", "v2"); !reflect.DeepEqual(got, fx.SpecDiff.Change) {
		t.Errorf("diff change\n got %q\nwant %q", got, fx.SpecDiff.Change)
	}
	if got := domain.SpecDiff(&specA, specA, "v1", "v1"); len(got) != len(fx.SpecDiff.Same) {
		t.Errorf("diff same %q, want %q", got, fx.SpecDiff.Same)
	}
}

func nilIfNone(e []map[string]any) []map[string]any {
	if e == nil {
		return []map[string]any{}
	}
	return e
}

func TestDeriveImageDownload(t *testing.T) {
	now := time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC)
	long := now.Add(-2 * domain.DeployGrace) // a download may outlast the deploy grace period
	note := "Downloading the image quay.io/debezium/server:3.6.0.Final (6m0s so far)."
	up := true

	// No container yet: deploying, however long the download takes.
	got := domain.Derive(domain.DeriveInput{Now: now, Deployed: true, Desired: "running", DeployedAt: &long, Phase: "missing", ImagePull: note})
	if got.Lifecycle != domain.LifecycleDeploying || got.Health != domain.HealthUnknown || got.Summary != "Downloading the connector image…" ||
		len(got.Reasons) != 1 || got.Reasons[0].Code != "image_pull" || got.Reasons[0].Message != note+" The connector starts once it finishes." {
		t.Fatalf("missing while downloading: %+v", got)
	}
	// Without a download the same state is a connector that never started.
	if got := domain.Derive(domain.DeriveInput{Now: now, Deployed: true, Desired: "running", DeployedAt: &long, Phase: "missing"}); got.Lifecycle != domain.LifecycleFailed {
		t.Fatalf("missing: %+v", got)
	}
	// The old connector keeps streaming meanwhile: its verdict stands, with the download noted.
	connected := true
	got = domain.Derive(domain.DeriveInput{Now: now, Deployed: true, Desired: "running", DeployedAt: &long, Phase: "running", HealthUp: &up,
		Metrics: &domain.Metrics{Connected: &connected}, ImagePull: note})
	if got.Lifecycle != domain.LifecycleStreaming || got.Health != domain.HealthHealthy || len(got.Reasons) != 1 ||
		got.Reasons[0] != (domain.Reason{Code: "image_pull", Severity: "info", Message: note + " The connector is replaced with the new version once it finishes."}) {
		t.Fatalf("running while downloading: %+v", got)
	}
	// Paused or unreachable: the download is not the story.
	for _, in := range []domain.DeriveInput{
		{Now: now, Deployed: true, Desired: "paused", DeployedAt: &long, Phase: "missing", ImagePull: note},
		{Now: now, Deployed: true, Desired: "running", DeployedAt: &long, RuntimeError: "down", ImagePull: note},
	} {
		for _, r := range domain.Derive(in).Reasons {
			if r.Code == "image_pull" {
				t.Fatalf("%+v reports the download", in)
			}
		}
	}
}

// A health check that gives no answer is reported as that, with BeziSync's reason, not as
// "no metrics" while metrics are arriving.
func TestDeriveSaysWhyHealthIsMissing(t *testing.T) {
	now := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	long := now.Add(-2 * domain.DeployGrace)
	young := now.Add(-time.Minute)
	up, connected := true, true
	metrics := &domain.Metrics{Connected: &connected}
	why := "The agent reads health on port 8080; this connector serves it on 9000. Upgrade the agent."
	base := domain.DeriveInput{Now: now, Deployed: true, Desired: "running", DeployedAt: &long, Phase: "running"}

	in := base
	in.Metrics, in.HealthError = metrics, why
	got := domain.Derive(in)
	want := domain.Reason{Code: "no_health", Severity: "warn", Message: "Running, but its health check gave no answer: " + why}
	if got.Lifecycle != domain.LifecycleStreaming || got.Health != domain.HealthUnknown || got.Summary != "Running — health unknown." ||
		len(got.Reasons) != 1 || got.Reasons[0] != want {
		t.Fatalf("metrics without health: %+v", got)
	}

	// Neither answers: both are said.
	in = base
	in.HealthError = why
	got = domain.Derive(in)
	if got.Summary != "Running — no health or metrics." || len(got.Reasons) != 2 || got.Reasons[0] != want || got.Reasons[1].Code != "no_metrics" {
		t.Fatalf("neither: %+v", got)
	}

	// Health without metrics stays "no metrics".
	in = base
	in.HealthUp = &up
	if got = domain.Derive(in); got.Summary != "Running — no metrics." || len(got.Reasons) != 1 || got.Reasons[0].Code != "no_metrics" {
		t.Fatalf("health without metrics: %+v", got)
	}

	// A connector still starting is not blamed yet.
	in = base
	in.DeployedAt, in.Metrics, in.HealthError = &young, metrics, why
	if got = domain.Derive(in); got.Lifecycle != domain.LifecycleDeploying || len(got.Reasons) != 0 {
		t.Fatalf("young: %+v", got)
	}
}

// Debezium counts BeziSync's signal table among the snapshot's tables but never copies it, so
// the counts read "1 of 2 remaining" after a one-table copy finished.
func TestSnapshotCountsOnlyTheChosenTables(t *testing.T) {
	n := func(v int) *int { return &v }
	b := func(v bool) *bool { return &v }
	for _, tc := range []struct {
		name             string
		in               domain.Metrics
		chosen           int
		total, remaining *int
	}{
		{"copied (MySQL, PostgreSQL)", domain.Metrics{SnapshotRunning: b(false), SnapshotCompleted: b(true), SnapshotTablesTotal: n(2), SnapshotTablesRemaining: n(1)}, 1, n(1), n(0)},
		{"copying", domain.Metrics{SnapshotRunning: b(true), SnapshotTablesTotal: n(4), SnapshotTablesRemaining: n(4)}, 3, n(3), n(3)},
		{"copying, signal and canary counted", domain.Metrics{SnapshotRunning: b(true), SnapshotTablesTotal: n(4), SnapshotTablesRemaining: n(3)}, 2, n(2), n(1)},
		{"nothing extra counted", domain.Metrics{SnapshotRunning: b(true), SnapshotTablesTotal: n(2), SnapshotTablesRemaining: n(1)}, 2, n(2), n(1)},
		{"part of the tables (an on-demand copy) that finished", domain.Metrics{SnapshotRunning: b(false), SnapshotCompleted: b(true), SnapshotTablesTotal: n(2), SnapshotTablesRemaining: n(1)}, 5, n(2), n(0)},
		{"no snapshot", domain.Metrics{SnapshotRunning: b(false), SnapshotTablesTotal: n(0), SnapshotTablesRemaining: n(0)}, 1, n(0), n(0)},
		{"not reported", domain.Metrics{}, 1, nil, nil},
	} {
		m := tc.in
		m.OnlyChosenTables(tc.chosen)
		if !reflect.DeepEqual(m.SnapshotTablesTotal, tc.total) || !reflect.DeepEqual(m.SnapshotTablesRemaining, tc.remaining) {
			t.Errorf("%s: total %v remaining %v", tc.name, m.AsDict()["snapshot_tables_total"], m.AsDict()["snapshot_tables_remaining"])
		}
	}
}

// An engine that explains itself (Flink CDC) replaces Debezium's log diagnosis: starting is
// deploying while young, then a failure with its own words; a failed job says why.
func TestDeriveEngineReasons(t *testing.T) {
	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	young, old := now.Add(-time.Second), now.Add(-2*domain.DeployGrace)
	down := false
	starting := &domain.Reason{Code: "health_down", Message: "Waiting for TaskManagers: 1 of 2 connected.", Severity: "error"}
	in := domain.DeriveInput{Now: now, Deployed: true, Desired: "running", DeployedAt: &young, Phase: "running", HealthUp: &down, EngineDown: starting,
		Logs: "ERROR Cannot create connection: Communications link failure"}
	if got := domain.Derive(in); got.Lifecycle != domain.LifecycleDeploying {
		t.Fatalf("young: %+v", got)
	}
	in.DeployedAt = &old
	if got := domain.Derive(in); got.Lifecycle != domain.LifecycleFailed || got.Summary != starting.Message || got.Reasons[0].Code != "health_down" {
		t.Fatalf("old: %+v", got)
	}
	// Resumed a minute ago: still starting, however old the deploy.
	resumed := now.Add(-time.Minute)
	in.StartedAt = &resumed
	if got := domain.Derive(in); got.Lifecycle != domain.LifecycleDeploying {
		t.Fatalf("resumed: %+v", got)
	}
	in.StartedAt = nil
	failed := &domain.Reason{Code: "destination_renamed", Message: "The destination table changed name.", Severity: "error"}
	in.DeployedAt, in.EngineDown = &young, failed
	if got := domain.Derive(in); got.Lifecycle != domain.LifecycleFailed || got.Reasons[0] != *failed {
		t.Fatalf("failed job: %+v", got)
	}
	in.Phase = "restarting"
	if got := domain.Derive(in); got.Reasons[0] != *failed {
		t.Fatalf("restarting workload: %+v", got)
	}
}

// A Flink CDC pipeline's manifest says so; Debezium manifests don't change.
func TestManifestEngine(t *testing.T) {
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"public.orders"}
	img := "registry.example/flink:1"
	spec.FlinkCDCImage = &img
	doc := domain.ToDocument("lake", "default", "", "", nil, "pg", nil, nil, spec, "flink-cdc")
	body, _ := doc.Get("spec")
	if e, _ := body.(*om.Map).Get("engine"); e != "flink-cdc" {
		t.Fatal(doc)
	}
	if i, _ := body.(*om.Map).Get("flinkCdcImage"); i != img {
		t.Fatal(doc)
	}
	docs, err := domain.ParseDocuments(domain.ToYAML([]any{doc}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := domain.FromDocument(docs[0])
	if err != nil || parsed.Engine != "flink-cdc" || parsed.Spec.FlinkCDCImage == nil || *parsed.Spec.FlinkCDCImage != img {
		t.Fatalf("%+v %v", parsed, err)
	}
	deb := domain.ToDocument("orders", "default", "", "", nil, "pg", nil, nil, domain.DefaultSpec(), "debezium")
	if b, _ := deb.Get("spec"); b.(*om.Map).Keys()[0] == "engine" {
		t.Fatal("a Debezium manifest names its engine")
	}
	docs[0]["spec"].(map[string]any)["engine"] = "spark"
	if _, err := domain.FromDocument(docs[0]); err == nil || !strings.Contains(err.Error(), "debezium or flink-cdc") {
		t.Fatal(err)
	}
}
