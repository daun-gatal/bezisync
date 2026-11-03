package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Maintenance windows ("HH:MM-HH:MM", UTC, may wrap midnight) for source-heavy work.

type clock struct{ h, m int }

func (c clock) minutes() int { return c.h*60 + c.m }

func parseClock(s string) (clock, error) {
	// time.fromisoformat("HH:MM"): two-digit hour 00-23 and minute 00-59.
	hh, mm, ok := strings.Cut(s, ":")
	if !ok || len(hh) != 2 || len(mm) != 2 {
		return clock{}, fmt.Errorf("invalid isoformat string: %q", s)
	}
	h, err1 := strconv.Atoi(hh)
	m, err2 := strconv.Atoi(mm)
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return clock{}, fmt.Errorf("invalid isoformat string: %q", s)
	}
	return clock{h, m}, nil
}

// ParseWindow returns the start and end of "HH:MM-HH:MM".
func ParseWindow(window string) (start, end clock, err error) {
	a, b, ok := strings.Cut(window, "-")
	if !ok || strings.Contains(b, "-") {
		return clock{}, clock{}, fmt.Errorf("invalid window %q", window)
	}
	if start, err = parseClock(a); err != nil {
		return
	}
	end, err = parseClock(b)
	return
}

// WindowContains reports whether at (UTC wall clock) is inside the window.
func WindowContains(window string, at time.Time) (bool, error) {
	start, end, err := ParseWindow(window)
	if err != nil {
		return false, err
	}
	at = at.UTC()
	// Compare HH:MM:SS.ffffff against HH:MM, like Python time comparisons.
	now := time.Duration(at.Hour())*time.Hour + time.Duration(at.Minute())*time.Minute +
		time.Duration(at.Second())*time.Second + time.Duration(at.Nanosecond()/1000)*time.Microsecond
	s := time.Duration(start.minutes()) * time.Minute
	e := time.Duration(end.minutes()) * time.Minute
	if s <= e {
		return s <= now && now < e, nil
	}
	return now >= s || now < e, nil // wraps midnight, e.g. 22:00-06:00
}

// WindowNextStart is nil when at is inside the window, else when it next opens.
func WindowNextStart(window string, at time.Time) (*time.Time, error) {
	inside, err := WindowContains(window, at)
	if err != nil || inside {
		return nil, err
	}
	start, _, _ := ParseWindow(window)
	at = at.UTC()
	candidate := time.Date(at.Year(), at.Month(), at.Day(), start.h, start.m, 0, 0, time.UTC)
	if !candidate.After(at) {
		candidate = candidate.Add(24 * time.Hour)
	}
	return &candidate, nil
}
