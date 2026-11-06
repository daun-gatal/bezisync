# mcpserver

## Overview
Package `mcpserver` is BeziSync's Model Context Protocol endpoint, mounted by [`api`](../api/README.md)
at `POST /api/v1/mcp`. Each request builds a stateless MCP server holding only the tools the caller
may use. A tool never touches the database or services directly: it calls BeziSync's own REST
router in process with the caller's `Authorization` header, so permissions, validation, policies,
approvals and audit apply exactly as for the UI and CLI. No tool takes or returns a credential.

## Interface
- `[server.go:Handler]` — the MCP HTTP handler; takes the REST router that tools call.
- `[server.go:Path]` — `/api/v1/mcp`; `[server.go:ResourceURL]` / `[server.go:ResourceMetadataURL]` — the OAuth resource indicator and its RFC 9728 metadata URL.
- `[catalog.go:Catalog]` — every tool in a stable order (built once); `[catalog.go:Find]` — a tool by name. `list_engine_settings` (`pipeline.view`) reads `GET …/pipelines/{id}/settings`; settings are changed with `update_pipeline` (`spec.tuning`, or `spec.flink_cdc.flink_conf`), so the same checks, limits and approvals apply.
- `[tools.go:Tool]` — name, kind, required `Perm`, `Admin` flag, `Project` argument, params and the `Call` that makes REST calls; `[tools.go:Tool.Group]` — its Settings → MCP group.
- `[tools.go:Kind]` — `[tools.go:Read]`, `[tools.go:Write]`, `[tools.go:Destructive]`; `[tools.go:Param]` — one argument schema.
- `[server.go:Enabled]` — whether an Admin left a tool on (default: read and write on, destructive off).

## Invariants
- MCP turned off answers 404 `mcp_disabled`; a browser `Origin` must be in the allowed origins or the public URL's origin, else 403 `origin_not_allowed` (`[server.go:Handler]`).
- Only bearer `bzt_` tokens (when `AllowAPITokens`) and `bzo_` OAuth tokens (when `OAuthEnabled`) are accepted; 401s carry a `WWW-Authenticate` challenge with the resource metadata URL when OAuth is on (`[server.go:authenticate]`).
- Authentication is done as a GET so read-only tokens get in; their tool list is read tools only and every inner REST call is checked with its real method (`[server.go:authenticate]`, `[server.go:caller.allowed]`).
- A tool is listed only if enabled, non-read for writable tokens only, `Admin` tools for Admins only, and `Perm` held in at least one project from `GET /auth/me` (`[server.go:caller.allowed]`); the REST API still enforces the per-project permission on each call. `delete_pipeline` asks for `pipeline.edit` while `DELETE /pipelines/{id}` needs `pipeline.deploy`, so it can be listed and then refused with 403 — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- Inner calls carry `auth.WithMCP` (so OAuth tokens are accepted) and a fresh chi routing context (`[server.go:invokeWith]`).
- Arguments are checked against declared params: unknown names, missing required ones, JSON types and enums are tool errors, not protocol errors (`[tools.go:Tool.check]`). Input schemas set `additionalProperties: false` (`[tools.go:Tool.mcpTool]`).
- `project` resolves only among the caller's projects (slug or id; optional when there is exactly one) (`[tools.go:call.projectID]`); id arguments for sub-resources must be UUIDs (`[tools.go:call.sub]`).
- Secrets enter only as `secret_refs` values starting with `ref+`; anything else is refused (`[tools.go:call.refs]`). The catalog has no tools that create or change API/agent tokens, runtime or secret-store credentials, alert channels, sign-in providers, passwords or MCP settings (`[catalog.go:buildCatalog]`); the read tools near them (`list_alert_channels`, `list_secret_store_secrets`, `list_secret_keys`, `check_secret_reference`) return names, kinds, paths and key names, never values.
- Versioned updates send `If-Match`: the given `expected_version`, else the current version read just before (last write wins) (`[tools.go:call.withVersion]`).
- A failing tool returns an MCP tool error (`IsError`), never a protocol error (`[server.go:caller.handler]`). Tool results are indented JSON cut at 200,000 bytes (`[server.go:maxResult]`); REST problems become readable text with field errors and failing preflight checks (`[server.go:problemText]`).

## Side Effects
- Reads MCP settings via `services.GetMCPSettings` on every request.
- In-process REST calls (`GET /api/v1/auth/me` per request, plus each tool's calls); those handlers write whatever their routes write (DB rows, audit events, runtime actions).
- `auth.Current` may update `api_tokens` / `oauth_grants` usage columns. Reads `PublicURL` from `config.Get()`.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/mcpserver/ ./internal/api/"`
