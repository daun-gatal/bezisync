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
