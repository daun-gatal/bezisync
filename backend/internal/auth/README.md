# auth

## Overview
Package `auth` authenticates every API request and resolves project access. A caller is a
browser session (cookie `bz_session` plus a double-submit CSRF token), a bearer API token
(`bzt_…`), or an MCP OAuth access token (`bzo_…`, MCP endpoint only). Authorisation is by
explicit `Perm`, derived from project role and the Admin flag. [`api`](../api/README.md) and
[`mcpserver`](../mcpserver/README.md) call it; secrets are hashed with [`security`](../security/README.md).

## Interface
- `[auth.go:Current]` — authenticates a request into a `[auth.go:Principal]` (user plus session, API token or OAuth grant), or a 401/403 problem.
- `[auth.go:StartSession]` / `[auth.go:EndSession]` — create a session row and set its cookies / delete it and clear them.
- `[auth.go:Project]` — resolves a project for a principal into a `[auth.go:ProjectAccess]`; `[auth.go:ProjectAccess.Require]` (403 `forbidden`) and `[auth.go:ProjectAccess.Can]` check a permission.
- `[auth.go:RequireSuperuser]` — 403 unless the user is an Admin (`IsSuperuser`).
- `[auth.go:Perm]`, the permission constants, `[auth.go:AllPerms]`, `[auth.go:RolePermissions]` (viewer ⊂ operator ⊂ editor), `[auth.go:PermissionsFor]`, `[auth.go:PermSet]` / `[auth.go:PermSet.Sorted]`.
- `[auth.go:WithMCP]` / `[auth.go:ViaMCP]` — mark and test that a request came through the MCP endpoint.
- `[auth.go:Principal.Label]` — the audit actor label: the email, or `email (token <name>)` for API tokens and OAuth grants (a grant is named `MCP · <client name>`).
- Constants `[auth.go:SessionCookie]`, `[auth.go:CSRFCookie]`, `[auth.go:CSRFHeader]`, `[auth.go:APITokenPrefix]`, `[auth.go:OAuthAccessPrefix]`; helpers `[auth.go:GetUser]`, `[auth.go:IsTx]`.

## Invariants
- A bearer `Authorization` header takes precedence over the session cookie; a `bzo_` token is accepted only when the context carries `WithMCP` (`[auth.go:Current]`, `[auth.go:fromOAuth]`).
- Tokens, sessions and OAuth grants are looked up by `security.TokenHashes` (every master key); a token or session hit under an old key is rewritten to the current-key hash (`[auth.go:fromToken]`, `[auth.go:fromSession]`). An OAuth grant's `access_hash` is not rewritten (`[auth.go:fromOAuth]`).
- Expired, unknown, or inactive-user credentials all fail with 401 (`invalid_token` / `session_expired`); no credential at all is 401 `not_authenticated` (`[auth.go:Current]`). An OAuth grant also needs both its access and grant expiry in the future, its client `allowed`, and no revocation (`[auth.go:fromOAuth]`).
- Session requests with a non-safe method (anything but GET/HEAD/OPTIONS) must send `x-csrf-token` equal to the session's CSRF token, compared in constant time, else 403 `csrf_failed` (`[auth.go:fromSession]`); bearer tokens need no CSRF. Sessions have a fixed expiry (`SessionTTLHours` from sign-in); use does not extend them.
- Read-only tokens and grants are refused (403 `read_only_token`) on non-safe methods, and inside a project keep only `*.view` permissions (`[auth.go:fromToken]`, `[auth.go:Project]`).
- Unknown project, a token scoped to another project, and no permissions all return 404 `not_found` — never 403 (`[auth.go:Project]`).
- Admins hold every permission in every project without membership; `pipeline.approve` comes from the membership's `CanApprove`, not the role (`[auth.go:PermissionsFor]`).
- Check permissions, never role names (`[auth.go:ProjectAccess.Require]`).
- Session cookie is HttpOnly; the CSRF cookie is readable by the SPA; both are `SameSite=Lax` and `Secure` follows `CookieSecure` (`[auth.go:StartSession]`).

## Side Effects
- DB writes: `user_sessions` (insert on sign-in, delete on sign-out, `token_hash` update after key rotation); `api_tokens` (`token_hash`, `last_used_at`, at most once a minute, even on read-only requests); `oauth_grants.last_used_at` (at most once a minute).
- DB reads: `users`, `api_tokens`, `user_sessions`, `oauth_grants`, `oauth_clients`, `projects`, `memberships`.
- Sets/clears the `bz_session` and `bz_csrf` cookies. Reads `SessionTTLHours`, `CookieSecure` from `config.Get()`.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/auth/` and by the API tests: `make verify-quick GO_PKGS="./internal/api/ ./internal/mcpserver/"`.
