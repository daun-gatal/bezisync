# problem

## Overview
Package `problem` is the single error type the API returns to clients: an RFC 9457 problem
document with a stable snake_case `code`, a plain-sentence `detail` and the request id.
Handlers and services return a `*Problem`; [`httpapi`](../httpapi/README.md) renders it as
`application/problem+json`, and anything that is not a `*Problem` becomes a logged 500.

## Interface
- `[problem.go:Problem]` — status, code, detail, optional `Errors` list and `Extra` top-level members; implements `error`.
- `[problem.go:New]`, `[problem.go:NotFound]` (404 `not_found`, "<what> was not found."), `[problem.go:Conflict]` (409 with extra members) — constructors.
- `[problem.go:Problem.WithExtra]`, `[problem.go:Problem.WithErrors]` — add body members / an errors list.
- `[problem.go:Problem.Body]` — renders the document map (`type`, `title`, `status`, `code`, `detail`, `request_id`, extras, `errors`).
- `[problem.go:Validation]` — the 422 `validation_failed` problem built from `[problem.go:FieldError]` (`loc` / `message` / `type`, FastAPI-shaped).
- `[problem.go:Title]` — the fixed title for a status (`"Error"` for unmapped statuses).

## Invariants
- `type` is always `https://bezisync.io/problems/<code>`; codes are part of the public contract, so never rename one (`[problem.go:Problem.Body]`).
- `request_id` is `null`, not `""`, when no id is known (`[problem.go:nilIfEmpty]`).
- `Extra` members are copied over the standard members, so an extra key named like a standard one overrides it (`[problem.go:Problem.Body]`).
- `errors` is emitted only when non-empty; a validation problem's `type` is `null` when the field error has none (`[problem.go:Validation]`).

## Side Effects
- None.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/problem/` and by the API tests: `make verify-quick GO_PKGS="./internal/api/"`.
