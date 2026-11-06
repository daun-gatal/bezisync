# ratelimit

## Overview
Package `ratelimit` is a small in-process sliding-window limiter for brute-force sensitive
endpoints (login, connection checks, feedback, MCP OAuth). It is per process: with several
API replicas each one counts separately, so ingress should also rate limit.

## Interface
- `[ratelimit.go:Limiter]` / `[ratelimit.go:New]` — a limiter of N attempts per key per window with a custom 429 detail (default "Too many attempts. Wait a minute and try again.").
- `[ratelimit.go:Limiter.Check]` — records an attempt or returns a 429 `rate_limited` `*problem.Problem`.
- `[ratelimit.go:Limiter.Reset]` (forget one key), `[ratelimit.go:Limiter.Clear]` (forget all; tests).
- The API's limiters: `[ratelimit.go:Login]` (10/min), `[ratelimit.go:Check]` (30/min), `[ratelimit.go:Feedback]` (10/hour), `[ratelimit.go:OAuthRegister]` (20/hour per client address), `[ratelimit.go:OAuthToken]` (120/min per client address).

## Invariants
- A refused attempt is not recorded; only allowed attempts extend the window (`[ratelimit.go:Limiter.Check]`).
- The key is chosen by the caller; this package does not derive it, so callers must not key on a client-controlled value.
- Memory is bounded loosely: once more than 10,000 keys exist, keys whose newest hit is outside the window are swept (`[ratelimit.go:Limiter.Check]`).
- All state is guarded by one mutex per limiter (`[ratelimit.go:Limiter]`).

## Side Effects
- In-memory state only (package-level limiters live for the process). No I/O.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/ratelimit/` and by the API tests: `make verify-quick GO_PKGS="./internal/api/"`.
