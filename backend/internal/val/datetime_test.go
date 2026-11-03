package val

import (
	"testing"
	"time"
)

// Expected values were produced with Pydantic's TypeAdapter(datetime).
func TestParseDatetimeMatchesPydantic(t *testing.T) {
	ok := map[string]string{
		"2026-01-01":                       "2026-01-01T00:00:00Z",
		"2026-01-01 10:00":                 "2026-01-01T10:00:00Z",
		"2026-01-01T10:00:00.123456+02:00": "2026-01-01T10:00:00.123456+02:00",
		"2026-01-01t10:00:00z":             "2026-01-01T10:00:00Z",
		"2026-01-01T10:00:00.1234567":      "2026-01-01T10:00:00.123456Z",
		"2026-01-01T10:00:00+0200":         "2026-01-01T10:00:00+02:00",
		"2026-01-01T10:00:00,5":            "2026-01-01T10:00:00.5Z",
		"2026-01-01_10:00:00":              "2026-01-01T10:00:00Z",
		"2026-01-01T23:59:59-23:59":        "2026-01-01T23:59:59-23:59",
		"1700000000":                       "2023-11-14T22:13:20Z",
		"1700000000.5":                     "2023-11-14T22:13:20.5Z",
		"1700000000000":                    "2023-11-14T22:13:20Z",
		"20000000001":                      "1970-08-20T11:33:20.001Z",
		"2026":                             "1970-01-01T00:33:46Z",
		"-1":                               "1969-12-31T23:59:59Z",
		"+5":                               "1970-01-01T00:00:05Z",
		"1.5e3":                            "1970-01-01T00:25:00Z",
		"2024-02-29":                       "2024-02-29T00:00:00Z",
	}
	for in, want := range ok {
		got, msg := ParseDatetime(in)
		if msg != "" || got.Format(time.RFC3339Nano) != want {
			t.Errorf("%q: got %s %q, want %s", in, got.Format(time.RFC3339Nano), msg, want)
		}
	}
	bad := map[string]string{
		"abc":                       "input is too short",
		"":                          "input is too short",
		"2026-1-1":                  "input is too short",
		"1e3":                       "input is too short",
		"2026-13-01":                "month value is outside expected range of 1-12",
		"2026-02-30":                "day value is outside expected range",
		"2023-02-29":                "day value is outside expected range",
		"2026-01-01T25:00:00":       "unexpected extra characters at the end of the input",
		"2026-01-01T10:00:00+25:00": "unexpected extra characters at the end of the input",
		"2026-01-01T10:00:00+02":    "unexpected extra characters at the end of the input",
		"2026-01-01T10:00:60":       "unexpected extra characters at the end of the input",
		"2026-01-01T10:00:00.":      "unexpected extra characters at the end of the input",
		"2026-01-01T10:00:00 Z":     "unexpected extra characters at the end of the input",
		"2026-01-01T1:00":           "unexpected extra characters at the end of the input",
		"2026-01-01T":               "unexpected extra characters at the end of the input",
		"x2026-01-01":               "invalid character in year",
		" 2026-01-01":               "invalid character in year",
		"99999999999999999999":      "invalid date separator, expected `-`",
	}
	for in, want := range bad {
		if _, msg := ParseDatetime(in); msg != want {
			t.Errorf("%q: got %q, want %q", in, msg, want)
		}
	}
}
