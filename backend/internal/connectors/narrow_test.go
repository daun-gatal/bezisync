package connectors

import (
	"database/sql"
	"math"
	"testing"
)

// Narrowing a setting clamps instead of wrapping.
func TestNarrowingClamps(t *testing.T) {
	if int32Of(500) != 500 || int32Of(math.MaxInt32+1) != math.MaxInt32 || int32Of(math.MinInt32-1) != math.MinInt32 {
		t.Fatal("int32Of")
	}
	if int16Of(3) != 3 || int16Of(math.MaxInt16+1) != math.MaxInt16 || int16Of(math.MinInt16-1) != math.MinInt16 {
		t.Fatal("int16Of")
	}
}

// MySQL size estimates past int64 show as the largest int64, never as a negative number.
func TestNullIntNeverWraps(t *testing.T) {
	for in, want := range map[string]int64{
		"42": 42, "18446744073709551615": math.MaxInt64, "9223372036854775808": math.MaxInt64,
		"1.5e19": math.MaxInt64, "9.223372036854775807e18": math.MaxInt64, "-1e19": math.MinInt64, "12.9": 12,
	} {
		got := nullInt(sql.NullString{String: in, Valid: true})
		if got == nil {
			t.Errorf("nullInt(%s) = nil, want %d", in, want)
		} else if *got != want {
			t.Errorf("nullInt(%s) = %d, want %d", in, *got, want)
		}
	}
	for _, s := range []sql.NullString{{}, {String: "x", Valid: true}, {String: "NaN", Valid: true}} {
		if got := nullInt(s); got != nil {
			t.Errorf("nullInt(%q) = %d, want nil", s.String, *got)
		}
	}
}
