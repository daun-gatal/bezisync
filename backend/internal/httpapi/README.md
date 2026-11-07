# httpapi

## Overview
Package `httpapi` is the HTTP framework under the API: the chi router with problem-shaped
404/405, the outer middleware (request id, security headers, access log, panic recovery),
optional credentialed CORS, JSON and RFC 9457 problem writers, and FastAPI-compatible body
and query-parameter readers. [`api`](../api/README.md) builds every route on it; errors are
[`problem`](../problem/README.md) values.

## Interface
- `[httpapi.go:Middleware]` — request id, security headers, `cache-control: no-store` on `/api/`, access log, panic → 500 problem.
- `[httpapi.go:CORS]` — credentialed CORS for configured origins, answering preflights itself.
- `[httpapi.go:NewRouter]` — chi router whose 404/405 are `http_404` / `http_405` problems.
- `[httpapi.go:Handler]` / `[httpapi.go:H]` — handlers return an error; `[httpapi.go:Fail]` renders it.
- `[httpapi.go:WriteProblem]`, `[httpapi.go:JSON]`, `[httpapi.go:NoContent]` — response writers.
- `[httpapi.go:Body]` — reads a JSON body (max 32 MiB) as generic values; `[httpapi.go:PathUUID]` — a UUID path parameter.
- `[httpapi.go:Q]` / `[httpapi.go:Query]` — query readers (`Str`, `OptStr`, `StrRules`, `Int`, `Bool`, `UUID`, `Literal`, `Required`, `RequiredInt`) collecting errors for `[httpapi.go:Query.Err]`.
- `[httpapi.go:RequestID]` / `[httpapi.go:WithRequestID]` — the request id in a context.
- `[httpapi.go:ClientHost]` — the client address used for rate-limit keys.
- `[httpapi.go:CheckProduction]` — refuses to start when `config.ValidateForProduction` reports problems.

## Invariants
- Every response carries `x-request-id` (the client's `x-request-id` if sent, unvalidated, else a new dash-less UUID), `x-content-type-options: nosniff`, `referrer-policy: same-origin` and `x-frame-options: DENY` (`[httpapi.go:Middleware]`).
- Any error that is not a `*problem.Problem` is logged and answered as 500 `internal_error` without its message (`[httpapi.go:Fail]`); a panic does the same unless headers were already written, and `http.ErrAbortHandler` is re-panicked (`[httpapi.go:Middleware]`).
- CORS reflects a listed `Origin` with `Access-Control-Allow-Credentials: true`; with `*` in the list, any other origin gets a literal `Access-Control-Allow-Origin: *` and no credentials (`[httpapi.go:CORS]`), and `bezisync api` warns at startup. A preflight (`OPTIONS` with `Access-Control-Request-Method`) from an allowed origin is answered 200 here with a fixed method list, the requested headers echoed and `Access-Control-Max-Age: 600`; other origins pass through without CORS headers.
- An empty required body is 422 `missing` at `["body"]`; malformed JSON is 422 `json_invalid` at `["body", <offset>]`; a read error is 400 `http_400` (`[httpapi.go:Body]`). Bodies are read through a 32 MiB limit that truncates rather than refuses, so a larger body fails as `json_invalid`, not 413 (`[httpapi.go:maxBody]`).
- Query readers return the default on invalid input and record the error; handlers must call `Err()` before using values. Repeated parameters use the last value (`[httpapi.go:Query.Err]`).
- `X-Forwarded-For` is trusted only when the peer is `127.0.0.1` / `::1`, and then only its last entry (`[httpapi.go:ClientHost]`).
- JSON output does not HTML-escape and has no trailing newline; a value that cannot be encoded panics, which `Middleware` turns into a 500 (`[httpapi.go:JSON]`).

## Side Effects
- Logs: one `request` line per `/api/` request (logger `bezisync.http`), and `unhandled error` lines with stack traces (logger `bezisync.core.errors`).
- Reads `config.Get()` in `CheckProduction`. No DB or network access.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/httpapi/` and by the API tests: `make verify-quick GO_PKGS="./internal/api/"`.
