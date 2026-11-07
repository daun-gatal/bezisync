package domain

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// P1, P2: ADR 0002.
func TestLoadPoliciesFailsClosed(t *testing.T) {
	if p := LoadPolicies(nil); p.Unreadable || !p.AllowForceDeploy {
		t.Fatalf("nil: %+v", p)
	}
	if p := LoadPolicies(map[string]any{}); p.Unreadable {
		t.Fatalf("empty: %+v", p)
	}
	p := LoadPolicies(map[string]any{"allowed_sink_types": []any{"kafka"}, "allowed_engines": "debezium", "require_approval": false, "from_a_newer_release": true})
	if !p.Unreadable || len(p.Problems) != 2 {
		t.Fatalf("problems: %+v", p.Problems)
	}
	if p.AllowedEngines == nil || len(p.AllowedEngines) != 0 || len(p.AllowedSinkTypes) != 1 || p.RequireApproval || !p.AllowForceDeploy {
		t.Fatalf("kept or strict: %+v", p)
	}
	if c := EngineViolation(p, "debezium"); c == nil || c.Code != "policy_unreadable" {
		t.Fatal(c)
	}
	if v := SaveViolations(p, nil, "postgresql", "kafka"); len(v) != 1 || v[0].Code != "policy_unreadable" || v[0].Fix.Who != "Admin" {
		t.Fatal(v)
	}
	dump, _ := p.MarshalJSON()
	readable := LoadPolicies(map[string]any{"allowed_sink_types": []any{"kafka"}})
	again, _ := readable.MarshalJSON()
	if string(dump) == string(again) || readable.Unreadable {
		t.Fatal(string(dump))
	}
	if _, ok := ParsePolicies(&val.V{}, nil, map[string]any{"unreadable": true, "problems": []any{"x"}}); !ok {
		t.Fatal("GET's read-only keys must round-trip")
	}
}
