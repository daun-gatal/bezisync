package domain

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/val"
)

func parseJSON(t *testing.T, s string) (PipelineSpec, *val.V, bool) {
	t.Helper()
	var raw any
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		t.Fatal(err)
	}
	v := &val.V{}
	spec, ok := ParseSpec(v, nil, raw)
	return spec, v, ok
}

func TestFlinkCDCOmittedWhenUnset(t *testing.T) {
	out, err := DefaultSpec().MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "flink_cdc") {
		t.Fatalf("an untouched spec must not dump flink_cdc: %s", out)
	}
	before := SpecHash(DefaultSpec())
	s := DefaultSpec()
	s.FlinkCDC = FlinkCDCSpec{}
	if SpecHash(s) != before {
		t.Fatal("an empty Flink CDC block must not change the spec hash")
	}
}

func TestFlinkCDCRoundTrip(t *testing.T) {
	spec, v, ok := parseJSON(t, `{"flink_cdc": {"schema_changes": "stop", "new_columns": "send", "freshness_seconds": 2,
		"commit_seconds": 300, "flink_conf": {"execution.checkpointing.timeout": "20 min"}}, "flink_cdc_image": "ghcr.io/x/flink:1"}`)
	if !ok {
		t.Fatalf("errors: %v", v.Errors)
	}
	want := FlinkCDCSpec{SchemaChanges: "stop", NewColumns: "send", FreshnessSeconds: 2, CommitSeconds: 300,
		FlinkConf: map[string]string{"execution.checkpointing.timeout": "20 min"}}
	if spec.FlinkCDC.SchemaChanges != want.SchemaChanges || spec.FlinkCDC.NewColumns != want.NewColumns ||
		spec.FlinkCDC.FreshnessSeconds != 2 || spec.FlinkCDC.CommitSeconds != 300 || spec.FlinkCDC.FlinkConf["execution.checkpointing.timeout"] != "20 min" {
		t.Fatalf("decoded %+v", spec.FlinkCDC)
	}
	if spec.FlinkCDCImage == nil || *spec.FlinkCDCImage != "ghcr.io/x/flink:1" {
		t.Fatal("flink_cdc_image")
	}
	out, err := spec.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	again, v2, ok := parseJSON(t, string(out))
	if !ok || !again.Equal(spec) {
		t.Fatalf("round trip changed the spec: %v %s", v2.Errors, out)
	}
	clone := spec.Clone()
	clone.FlinkCDC.FlinkConf["x"] = "y"
	*clone.FlinkCDCImage = "other"
	if _, leaked := spec.FlinkCDC.FlinkConf["x"]; leaked || *spec.FlinkCDCImage != "ghcr.io/x/flink:1" {
		t.Fatal("Clone must deep-copy the Flink CDC fields")
	}
}

func TestFlinkCDCEffective(t *testing.T) {
	e := FlinkCDCSpec{FreshnessSeconds: 30}.Effective()
	if e.SchemaChanges != "apply" || e.NewColumns != "hold" || e.FreshnessSeconds != 30 || e.CommitSeconds != 60 || e.FlinkConf == nil {
		t.Fatalf("effective %+v", e)
	}
	if !(FlinkCDCSpec{}).Empty() || (FlinkCDCSpec{NewColumns: "send"}).Empty() || (FlinkCDCSpec{FlinkConf: map[string]string{"a": "b"}}).Empty() {
		t.Fatal("Empty")
	}
}

func TestFlinkCDCInvalid(t *testing.T) {
	cases := map[string]string{
		`{"flink_cdc": {"schema_changes": "evolve"}}`: "flink_cdc.schema_changes",
		`{"flink_cdc": {"new_columns": "drop"}}`:      "flink_cdc.new_columns",
		`{"flink_cdc": {"freshness_seconds": 7}}`:     "flink_cdc.freshness_seconds",
		`{"flink_cdc": {"commit_seconds": "a"}}`:      "flink_cdc.commit_seconds",
		`{"flink_cdc": {"other": 1}}`:                 "flink_cdc.other",
		`{"flink_cdc": {"flink_conf": {"a": 1}}}`:     "flink_cdc.flink_conf.a",
		`{"flink_cdc_image": "bad image"}`:            "flink_cdc_image",
		`{"flink_cdc": []}`:                           "flink_cdc",
	}
	for body, loc := range cases {
		_, v, ok := parseJSON(t, body)
		if ok {
			t.Errorf("%s: accepted", body)
			continue
		}
		found := false
		for _, e := range v.Errors {
			if strings.Join(e.Loc, ".") == loc {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no error at %s: %+v", body, loc, v.Errors)
		}
	}
}
