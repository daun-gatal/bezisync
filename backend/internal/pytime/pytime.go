// Package pytime formats times exactly like the Python implementation did, so API
// responses and stored values keep their shape.
package pytime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ISOFormat is Python's datetime.isoformat() for an aware UTC datetime:
// "2026-10-30T06:14:19.624674+00:00", without the fraction when microseconds are 0.
func ISOFormat(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		base += fmt.Sprintf(".%06d", us)
	}
	return base + "+00:00"
}

// Pydantic is how Pydantic v2 serializes an aware UTC datetime:
// "2026-10-30T06:14:19.624674Z", without the fraction when microseconds are 0.
func Pydantic(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		base += fmt.Sprintf(".%06d", us)
	}
	return base + "Z"
}

// Time marshals as Pydantic does; use it in API response types.
type Time struct{ time.Time }

// T wraps a time.
func T(t time.Time) Time { return Time{t} }

// P wraps an optional time (nil stays null).
func P(t *time.Time) *Time {
	if t == nil {
		return nil
	}
	return &Time{*t}
}

// MarshalJSON implements json.Marshaler.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(Pydantic(t.Time))
}

// UnmarshalJSON accepts ISO 8601 / RFC 3339 times.
func (t *Time) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	v, err := Parse(s)
	if err != nil {
		return err
	}
	t.Time = v
	return nil
}

// Parse reads Python isoformat / Pydantic / RFC 3339 strings (naive values are UTC).
func Parse(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "Z") || strings.HasSuffix(s, "z") {
		s = s[:len(s)-1] + "+00:00"
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02",
	} {
		if v, err := time.Parse(layout, s); err == nil {
			return v.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid datetime %q", s)
}
