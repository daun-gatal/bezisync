// Package ratelimit is a small in-process limiter for brute-force sensitive endpoints.
// Per process is a first line of defence; several replicas should also limit at ingress.
package ratelimit

import (
	"sync"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Limiter allows a number of attempts per key within a sliding window.
type Limiter struct {
	attempts int
	window   time.Duration
	detail   string
	mu       sync.Mutex
	hits     map[string][]time.Time
}

// New creates a limiter.
func New(attempts int, window time.Duration, detail string) *Limiter {
	if detail == "" {
		detail = "Too many attempts. Wait a minute and try again."
	}
	return &Limiter{attempts: attempts, window: window, detail: detail, hits: map[string][]time.Time{}}
}

// Check records an attempt or fails with 429 rate_limited.
func (l *Limiter) Check(key string) error {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	var hits []time.Time
	for _, t := range l.hits[key] {
		if now.Sub(t) < l.window {
			hits = append(hits, t)
		}
	}
	if len(hits) >= l.attempts {
		l.hits[key] = hits
		return problem.New(429, "rate_limited", l.detail)
	}
	l.hits[key] = append(hits, now)
	if len(l.hits) > 10_000 {
		for k, v := range l.hits {
			if len(v) == 0 || now.Sub(v[len(v)-1]) >= l.window {
				delete(l.hits, k)
			}
		}
	}
	return nil
}

// Reset forgets a key.
func (l *Limiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.hits, key)
}

// Clear forgets every key (tests).
func (l *Limiter) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hits = map[string][]time.Time{}
}

// The limiters of the API.
var (
	Login    = New(10, time.Minute, "")
	Check    = New(30, time.Minute, "")
	Feedback = New(10, time.Hour, "You have sent a lot of feedback this hour. Try again later.")
	// OAuthRegister limits MCP agent registrations per client address.
	OAuthRegister = New(20, time.Hour, "Too many agents registered from this address. Try again later.")
	// OAuthToken limits token requests per client address.
	OAuthToken = New(120, time.Minute, "")
)
