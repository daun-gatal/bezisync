package val

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// Datetime validates a datetime string like Pydantic (speedate): RFC 3339-ish datetimes,
// bare dates (midnight) and Unix timestamps (seconds, or milliseconds above 2e10).
// Naive values are taken as UTC.
func (v *V) Datetime(loc Loc, raw any) (time.Time, bool) {
	s, ok := raw.(string)
	if !ok {
		v.Fail(loc, "datetime_type", "Input should be a valid datetime")
		return time.Time{}, false
	}
	t, msg := ParseDatetime(s)
	if msg != "" {
		v.Fail(loc, "datetime_from_date_parsing", "Input should be a valid datetime or date, "+msg)
		return time.Time{}, false
	}
	return t, true
}

// ParseDatetime parses s; msg is speedate's error description when invalid.
func ParseDatetime(s string) (time.Time, string) {
	if t, ok := parseTimestamp(s); ok {
		return t, ""
	}
	if t, ok := parseFullDatetime(s); ok {
		return t, ""
	}
	y, m, d, msg := parseDate(s)
	if msg != "" {
		return time.Time{}, msg
	}
	if len(s) > 10 {
		return time.Time{}, "unexpected extra characters at the end of the input"
	}
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), ""
}

func parseTimestamp(s string) (time.Time, bool) {
	body := strings.TrimLeft(s, "+-")
	if len(s)-len(body) > 1 || body == "" {
		return time.Time{}, false
	}
	var ts float64
	if strings.Trim(body, "0123456789") == "" {
		n, err := strconv.ParseInt(strings.TrimPrefix(s, "+"), 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		ts = float64(n)
	} else if strings.Contains(body, ".") {
		f, err := strconv.ParseFloat(strings.TrimPrefix(s, "+"), 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return time.Time{}, false
		}
		ts = f
	} else {
		return time.Time{}, false
	}
	if math.Abs(ts) > 2e10 {
		ts /= 1000
	}
	sec := math.Floor(ts)
	us := math.Round((ts - sec) * 1e6)
	return time.Unix(int64(sec), int64(us)*1000).UTC(), true
}

func digits(s string) (int, bool) {
	n := 0
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

func daysIn(y, m int) int {
	return time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func parseDate(s string) (y, m, d int, msg string) {
	if len(s) < 10 {
		return 0, 0, 0, "input is too short"
	}
	y, ok := digits(s[0:4])
	if !ok {
		return 0, 0, 0, "invalid character in year"
	}
	if s[4] != '-' {
		return 0, 0, 0, "invalid date separator, expected `-`"
	}
	if m, ok = digits(s[5:7]); !ok {
		return 0, 0, 0, "invalid character in month"
	}
	if s[7] != '-' {
		return 0, 0, 0, "invalid date separator, expected `-`"
	}
	if d, ok = digits(s[8:10]); !ok {
		return 0, 0, 0, "invalid character in day"
	}
	if m < 1 || m > 12 {
		return 0, 0, 0, "month value is outside expected range of 1-12"
	}
	if d < 1 || d > daysIn(y, m) {
		return 0, 0, 0, "day value is outside expected range"
	}
	return y, m, d, ""
}

func parseFullDatetime(s string) (time.Time, bool) {
	y, mo, d, msg := parseDate(s)
	if msg != "" || len(s) < 16 || strings.IndexByte("Tt _", s[10]) < 0 {
		return time.Time{}, false
	}
	rest := s[11:]
	two := func(at int) (int, bool) {
		if len(rest) < at+2 {
			return 0, false
		}
		return digits(rest[at : at+2])
	}
	h, ok1 := two(0)
	mi, ok2 := two(3)
	if !ok1 || !ok2 || rest[2] != ':' || h > 23 || mi > 59 {
		return time.Time{}, false
	}
	rest = rest[5:]
	sec, ns := 0, 0
	if rest != "" && rest[0] == ':' {
		v, ok := two(1)
		if !ok || v > 59 {
			return time.Time{}, false
		}
		sec, rest = v, rest[3:]
		if rest != "" && (rest[0] == '.' || rest[0] == ',') {
			i := 1
			for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
				i++
			}
			frac := rest[1:i]
			if frac == "" {
				return time.Time{}, false
			}
			frac = (frac + "000000")[:6]
			us, _ := digits(frac)
			ns, rest = us*1000, rest[i:]
		}
	}
	loc := time.UTC
	switch {
	case rest == "":
	case rest == "Z" || rest == "z":
	case rest[0] == '+' || rest[0] == '-':
		tz := rest[1:]
		if len(tz) == 5 && tz[2] == ':' {
			tz = tz[:2] + tz[3:]
		}
		if len(tz) != 4 {
			return time.Time{}, false
		}
		th, ok1 := digits(tz[:2])
		tm, ok2 := digits(tz[2:])
		if !ok1 || !ok2 || th > 23 || tm > 59 {
			return time.Time{}, false
		}
		off := th*3600 + tm*60
		if rest[0] == '-' {
			off = -off
		}
		loc = time.FixedZone("", off)
	default:
		return time.Time{}, false
	}
	return time.Date(y, time.Month(mo), d, h, mi, sec, ns, loc), true
}
