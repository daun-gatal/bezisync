package pyjson_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

func TestMatchesPythonJSONDumps(t *testing.T) {
	var fx struct {
		Cases []struct {
			ValueJSON string `json:"value_json"`
			Dumps     string `json:"dumps"`
			Compact   string `json:"compact"`
		} `json:"cases"`
	}
	contract.Load(t, "pyjson", &fx)
	if len(fx.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range fx.Cases {
		v, err := pyjson.DecodeGeneric([]byte(c.ValueJSON))
		if err != nil {
			t.Fatalf("decode %s: %v", c.ValueJSON, err)
		}
		got, err := pyjson.Dumps(v)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.Dumps {
			t.Errorf("Dumps(%s)\n got %s\nwant %s", c.ValueJSON, got, c.Dumps)
		}
		got, err = pyjson.DumpsCompact(v)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.Compact {
			t.Errorf("DumpsCompact(%s)\n got %s\nwant %s", c.ValueJSON, got, c.Compact)
		}
	}
}

func TestFloatFieldsKeepTheirFraction(t *testing.T) {
	type spec struct {
		Limit pyjson.Float `json:"limit"`
		Count int          `json:"count"`
	}
	got, err := pyjson.DumpsCompact(spec{Limit: 10, Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"count":3,"limit":10.0}`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func FuzzFloatReprRoundTrips(f *testing.F) {
	for _, s := range []float64{0.1, 1e16, 1e-5, 123.456, -2.5e-300} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v float64) {
		s, err := pyjson.FloatRepr(v)
		if err != nil {
			t.Fatal(err)
		}
		if s == "NaN" || s == "Infinity" || s == "-Infinity" {
			return
		}
		back, err := pyjson.DecodeGeneric([]byte(s))
		if err != nil {
			t.Fatalf("%v rendered as invalid JSON %q", v, s)
		}
		again, err := pyjson.Dumps(back)
		if err != nil || again != s {
			t.Fatalf("%v: %q re-renders as %q", v, s, again)
		}
	})
}
