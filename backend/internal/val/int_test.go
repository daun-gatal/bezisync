package val_test

import (
	"encoding/json"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Integers outside the platform's int are refused, never wrapped.
func TestIntRefusesValuesPastTheRange(t *testing.T) {
	for _, tc := range []struct {
		raw  any
		want int
		err  string
	}{
		{json.Number("9223372036854775807"), 9223372036854775807, ""},
		{json.Number("-9223372036854775808"), -9223372036854775808, ""},
		{json.Number("1e3"), 1000, ""},
		{"5.0", 5, ""},
		{"1_000", 1000, ""},
		{json.Number("9223372036854775808"), 0, "int_parsing_size"},
		{json.Number("1e19"), 0, "int_parsing_size"},
		{json.Number("-1e19"), 0, "int_parsing_size"},
		{json.Number("9.223372036854775807e18"), 0, "int_parsing_size"}, // rounds to 2^63
		{"10000000000000000000.0", 0, "int_parsing_size"},
		{1e19, 0, "int_parsing_size"},
	} {
		v := &val.V{}
		got, ok := v.Int(nil, tc.raw)
		if tc.err == "" {
			if !ok || got != tc.want {
				t.Errorf("Int(%v) = %d, %v (%v), want %d", tc.raw, got, ok, v.Errors, tc.want)
			}
			continue
		}
		if ok || len(v.Errors) != 1 || *v.Errors[0].Type != tc.err {
			t.Errorf("Int(%v) = %d, %v (%v), want %s", tc.raw, got, ok, v.Errors, tc.err)
		}
	}
}
