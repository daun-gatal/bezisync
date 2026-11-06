package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func TestSpecTuning(t *testing.T) {
	parse := func(raw map[string]any) (domain.PipelineSpec, *val.V) {
		v := &val.V{}
		s, _ := domain.ParseSpec(v, nil, raw)
		return s, v
	}
	base := func(tuning any) map[string]any {
		return map[string]any{"source": map[string]any{"tables": map[string]any{"include": []any{"a.b"}}}, "tuning": tuning}
	}
	// C12: an empty or missing tuning is not dumped, so stored specs and their hashes don't change.
	for _, raw := range []map[string]any{{"source": map[string]any{"tables": map[string]any{"include": []any{"a.b"}}}}, base(map[string]any{}), base(nil)} {
		s, v := parse(raw)
		if len(v.Errors) > 0 || s.Tuning != nil {
			t.Fatal(v.Errors, s.Tuning)
		}
		if b, _ := json.Marshal(s); strings.Contains(string(b), "tuning") {
			t.Fatal(string(b))
		}
	}
	s, v := parse(base(map[string]any{"z.b": "1", "debezium.source.poll.interval.ms": "250"}))
	if len(v.Errors) > 0 || s.Tuning["debezium.source.poll.interval.ms"] != "250" {
		t.Fatal(v.Errors)
	}
	b, _ := json.Marshal(s)
	// Keys in JSONB order (shorter first), like every map the spec stores.
	if !strings.HasSuffix(string(b), `"tuning":{"z.b":"1","debezium.source.poll.interval.ms":"250"}}`) {
		t.Fatal(string(b))
	}
	// Clone copies it.
	c := s.Clone()
	c.Tuning["z.b"] = "2"
	if s.Tuning["z.b"] != "1" {
		t.Fatal("clone shares tuning")
	}
	for _, bad := range []map[string]any{
		{"Debezium.source.x": "1"}, {"nodots": "1"}, {"a.b ": "1"}, {"a.b": "${BZ_STATE_PASSWORD}"}, {"a.b": "1\n2"},
		{"a.b": strings.Repeat("9", 257)}, {"a.b": 5},
	} {
		if _, v := parse(base(bad)); len(v.Errors) == 0 {
			t.Errorf("%v accepted", bad)
		}
	}
}

// Config-as-code carries tuning: export then apply gives back the same settings.
func TestTuningRoundTripsThroughYAML(t *testing.T) {
	spec := domain.DefaultSpec()
	spec.Source.Tables.Include = []string{"public.orders"}
	spec.Tuning = map[string]string{"debezium.source.snapshot.max.threads": "4", "debezium.source.poll.interval.ms": "250"}
	doc := domain.ToDocument("orders-cdc", "prod", "", "", nil, "orders-db", nil, nil, spec, "debezium")
	text := domain.ToYAML([]any{doc})
	if !strings.Contains(text, "tuning:") {
		t.Fatal(text)
	}
	docs, err := domain.ParseDocuments(text)
	if err != nil || len(docs) != 1 {
		t.Fatal(err)
	}
	parsed, err := domain.FromDocument(docs[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Spec.Tuning) != 2 || parsed.Spec.Tuning["debezium.source.snapshot.max.threads"] != "4" {
		t.Fatal(parsed.Spec.Tuning)
	}
}

func TestSettingChanges(t *testing.T) {
	old := domain.DefaultSpec()
	old.Tuning = map[string]string{"debezium.source.poll.interval.ms": "500", "debezium.source.snapshot.max.threads": "1"}
	old.Advanced = map[string]string{"debezium.source.driver.a": "1", "debezium.source.driver.b": "1"}
	next := old.Clone()
	next.Tuning = map[string]string{"debezium.source.snapshot.max.threads": "4"}                        // poll removed, threads changed
	next.Advanced = map[string]string{"debezium.source.driver.b": "1", "debezium.source.driver.c": "2"} // a removed, c added
	got := domain.SettingChanges(old, next)
	str := func(p *string) string {
		if p == nil {
			return "<unset>"
		}
		return *p
	}
	var lines []string
	for _, c := range got {
		lines = append(lines, c.Key+" "+str(c.From)+"→"+str(c.To)+" "+string(c.Impact)+map[bool]string{true: " unverified", false: ""}[c.Unverified])
	}
	want := []string{
		"debezium.source.poll.interval.ms 500→<unset> restart",
		"debezium.source.snapshot.max.threads 1→4 first_copy",
		"debezium.source.driver.a 1→<unset> restart unverified",
		"debezium.source.driver.c <unset>→2 restart unverified",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("got\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
	if got[1].Title != "Snapshot threads" || domain.HeaviestImpact(got) != "first_copy" {
		t.Fatal(got[1].Title, domain.HeaviestImpact(got))
	}
	if domain.SummarizeSettings(&old, old) != nil || domain.HeaviestImpact(nil) != "" {
		t.Fatal("no change, no summary")
	}
	// Flink settings count too; a first deploy lists everything set.
	f := domain.DefaultSpec()
	f.FlinkCDC.FlinkConf = map[string]string{"heartbeat.timeout": "60 s"}
	s := domain.SummarizeSettings(nil, f)
	if s == nil || len(s.Settings) != 1 || s.Settings[0].Title != "Heartbeat timeout" || s.Impact != "restart" || s.Settings[0].From != nil {
		t.Fatal(s)
	}
}
