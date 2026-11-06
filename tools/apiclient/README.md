# apiclient

## Overview
A minimal signed-in session against the BeziSync REST API for the repository's own tools (the demo smoke test). It keeps the session cookies in a jar and echoes the CSRF cookie as a header, the same way the browser client does, so tools exercise the API exactly as a person would. It is part of the dependency-free `tools/` module and is not shipped in any image.

## Interface
- `[apiclient.go:New]` — validates the base URL and returns a `[apiclient.go:Client]` for `{base}/api/v1`.
- `[apiclient.go:Client.Call]` — sends JSON, decodes the JSON answer into `out`, extra headers as name/value pairs; failures are `*[apiclient.go:Error]` (status, problem `code`, `detail`).
- `[apiclient.go:Client.Login]` / `[apiclient.go:Client.Logout]` — `POST /auth/login` with email and password; `POST /auth/logout`, ignoring failures.

## Invariants
- `[apiclient.go:New]` rejects any URL that is not `http://` or `https://`.
- `[apiclient.go:Client.Call]` sends `Accept: application/json` (and `Content-Type` with a body) and, on every non-GET request, `X-CSRF-Token` from the `bz_csrf` cookie when the jar holds one; a 2xx with an empty body leaves `out` untouched.
- Error answers decode as RFC 9457 problems (`detail`, falling back to `title`); a non-JSON body is kept as the detail, cut to 500 bytes.
- The HTTP client times out after 120 s per request.

## Side Effects
- HTTP requests to the given BeziSync server; holds session cookies in memory only.

## Verification
- Run: No co-located tests; covered by `cd tools && go vet ./apiclient/` and exercised end to end by `make demo-smoke` (`cmd/demo-smoke`).
