package domain_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

type specCase struct {
	Input  json.RawMessage `json:"input"`
	Errors []struct {
		Loc  []string `json:"loc"`
		Type string   `json:"type"`
		Msg  string   `json:"msg"`
	} `json:"errors"`
	Dump             json.RawMessage `json:"dump"`
	Hash             string          `json:"hash"`
	SummaryFirst     json.RawMessage `json:"summary_first"`
	SummaryVsDefault json.RawMessage `json:"summary_vs_default"`
}

func canon(t *testing.T, raw []byte) string {
	t.Helper()
	g, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	s, err := pyjson.Dumps(g)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSpecMatchesPydantic(t *testing.T) {
	var fx struct {
		Cases []specCase `json:"cases"`
	}
	contract.Load(t, "spec", &fx)
	base := domain.DefaultSpec()
	for _, c := range fx.Cases {
		raw, err := pyjson.DecodeGeneric(c.Input)
		if err != nil {
			t.Fatal(err)
		}
		v := &val.V{}
		spec, ok := domain.ParseSpec(v, nil, raw)
		if len(c.Errors) > 0 {
			if ok {
				t.Errorf("%s: accepted, Pydantic rejected with %v", c.Input, c.Errors)
				continue
			}
			var got, want []string
			for _, e := range v.Errors {
				got = append(got, e.Message+" @ "+join(e.Loc)+" ("+*e.Type+")")
			}
			for _, e := range c.Errors {
				want = append(want, e.Msg+" @ "+join(e.Loc)+" ("+e.Type+")")
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s:\n got %v\nwant %v", c.Input, got, want)
			}
			continue
		}
		if !ok {
			t.Errorf("%s: rejected with %v, Pydantic accepted", c.Input, v.Errors)
			continue
		}
		gotDump, _ := json.Marshal(spec)
		if canon(t, gotDump) != canon(t, c.Dump) {
			t.Errorf("%s: dump\n got %s\nwant %s", c.Input, canon(t, gotDump), canon(t, c.Dump))
		}
		if h := domain.SpecHash(spec); h != c.Hash {
			t.Errorf("%s: hash %s, want %s", c.Input, h, c.Hash)
		}
		first, _ := json.Marshal(domain.SummarizeChange(nil, spec))
		if canon(t, first) != canon(t, c.SummaryFirst) {
			t.Errorf("%s: first summary\n got %s\nwant %s", c.Input, first, c.SummaryFirst)
		}
		vs, _ := json.Marshal(domain.SummarizeChange(&base, spec))
		if canon(t, vs) != canon(t, c.SummaryVsDefault) {
			t.Errorf("%s: summary\n got %s\nwant %s", c.Input, vs, c.SummaryVsDefault)
		}
	}
}

func join(l []string) string {
	out := ""
	for i, p := range l {
		if i > 0 {
			out += "."
		}
		out += p
	}
	return out
}
