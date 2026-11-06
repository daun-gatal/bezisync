# pytime

## Overview
Formats and parses times exactly like the former Python implementation, so API responses and
stored values keep their shape. API response types use `Time`; other code uses the two
formatters directly.

## Interface
- `[pytime.go:ISOFormat]` — Python `datetime.isoformat()` for UTC: `2026-10-30T06:14:19.624674+00:00`.
- `[pytime.go:Pydantic]` — Pydantic v2's form: `2026-10-30T06:14:19.624674Z`.
- `[pytime.go:Time]` / `[pytime.go:T]` / `[pytime.go:P]` — a time that marshals as `Pydantic`; `P` keeps nil as `null`.
- `[pytime.go:Parse]` — reads isoformat, Pydantic and RFC 3339 strings, plus date-only and minute-precision forms.

## Invariants
- Output is always UTC with microsecond precision, and the fraction is omitted when the microseconds are 0 (`[pytime.go:ISOFormat]`, `[pytime.go:Pydantic]`).
- `[pytime.go:Parse]` treats naive values as UTC and returns UTC; a trailing `Z`/`z` is read as `+00:00`.

## Side Effects
- None.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/pytime/` plus `make verify-quick GO_PKGS="./internal/domain/ ./internal/services/"`.
