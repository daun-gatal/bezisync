# api

## Overview
Package `api` is the versioned REST API (mounted under `/api/v1`), the system routes
(`/healthz`, `/readyz`, `/metrics`, docs) and the OAuth discovery documents. Handlers are thin:
they authenticate with [`auth`](../auth/README.md), validate with `val` in FastAPI-compatible shapes
via [`httpapi`](../httpapi/README.md), and call `internal/services`; errors are
[`problem`](../problem/README.md) values. The embedded `openapi.json` is the contract the frontend
client is generated from. The MCP endpoint ([`mcpserver`](../mcpserver/README.md)) is mounted here
and calls this same router in process.

## Interface
- `[api.go:Router]` — builds the API process handler: middleware, optional CORS, system routes, every `/api/v1` route group, `POST /api/v1/mcp`, OAuth discovery.
- `[api.go:OpenAPI]` — the embedded `openapi.json`; `[system.go:systemRoutes]` also serves it at `/api/v1/openapi.json` with Swagger UI at `/api/docs`, plus `/healthz`, `/readyz` (503 `database_unavailable` when `select 1` fails), `/metrics`, and the unauthenticated `/api/v1/system/info` (version, environment, default image, telemetry flag) and `/api/v1/system/licenses`; `/api/v1/system/telemetry` is Admin-only.
- Route groups, one file per resource:
  - `[auth.go:authRoutes]` (login, logout, `/auth/me`), `[auth.go:tokenRoutes]` (personal API tokens), `[auth.go:ssoRoutes]` (`/auth/providers`, OIDC/OAuth2 login + callback), `[auth.go:samlRoutes]` (SP metadata, ACS).
  - `[sso_providers.go:ssoProviderRoutes]` (`/auth/settings`, `/sso-providers` incl. test; Admin) with per-kind config parsing in `sso_kinds.go`.
  - `[projects.go:projectRoutes]` (projects, members, events, audit), `[users.go:userRoutes]` (people and per-project access; Admin).
  - `[connections.go:connectionRoutes]`, `[secret_stores.go:secretStoreRoutes]`, `[copy.go:copyRoutes]` (copy connections / secret stores to another project), `[runtimes.go:runtimeRoutes]` (incl. agent tokens and tests).
  - `[pipelines.go:pipelineRoutes]` (CRUD, versions, preflight, deploy, pause/resume/restart, config, logs, events, snapshots, metrics, offsets, positions, diff, schema), `[manifests.go:manifestRoutes]` (export/apply/promote), `[transforms.go:transformRoutes]` (transform preview), `[images.go:imageRoutes]`, `[templates.go:templateRoutes]`, `[approvals.go:approvalRoutes]` (change requests, policies, `POST …/policies/setting-limits:impact` for `project.manage`).
  - `[settings.go:settingsRoutes]` (`pipeline.view`): `GET …/pipelines/{id}/settings` (`services.PipelineSettings`), `POST …/settings:classify` (pure, at most `[settings.go:maxClassify]` keys, values never echoed) and `POST …/cluster:pack` (`services.PreviewCluster`). `POST …/cluster:shrink` (`pipeline.operate`) is in `pipelineRoutes`; `POST …/debezium-images:effects` (`services.ImageEffects`) is in `imageRoutes`.
  - `[alerts.go:alertRoutes]` (alerts, rules, channels, notifications), `[stream.go:streamRoutes]` (SSE), `[catalog.go:catalogRoutes]` (`/engines`, Debezium catalog), `[feedback.go:feedbackRoutes]`, `[agent.go:agentRoutes]` (`POST /agent/poll`).
  - `[mcp.go:mcpRoutes]` (MCP info/settings/tools/clients/grants, OAuth consent, `/oauth/register|authorize|token|revoke`) and `[mcp.go:oauthDiscoveryRoutes]` (RFC 8414 / 9728 documents).
- Response shapes (ordered JSON fields), e.g. `[auth.go:MeRead]`, `[projects.go:ProjectRead]`, `[pipelines.go:PipelineRead]`, `[connections.go:ConnectionRead]`, `[runtimes.go:RuntimeRead]` (with `placement_profiles`), `[pipelines.go:FootprintRead]` (a pipeline's peak `footprint`), `[mcp.go:MCPSettingsRead]`, `[projects.go:Page]`.
- Constants: `[agent.go:AgentPollSeconds]` (2), `[projects.go:SlugRe]` (slug grammar).
- Shared handler helpers: `[api.go:principal]`, `[api.go:superuser]`, `[api.go:project]`, `[pipelines.go:pipelineAction]`, `[pipelines.go:viewPipeline]`, `[api.go:body]` / `[api.go:optBody]`, `[api.go:paginate]`, `[api.go:tx]` / `[api.go:dryTx]`, `[connections.go:parseIfMatch]`, `[auth.go:safeNext]`, `[api.go:obj]`.

## Invariants
- Project routes resolve `{project_id}` with `[api.go:project]` (unknown and unauthorised are both 404) and then ask for a `Perm` via `Require`; never a role name. Admin-only routes use `[api.go:superuser]` (users, `POST`/`DELETE /projects`, SSO providers, Debezium catalog writes, MCP settings/tools/clients and `GET /mcp/grants?all=true`, feedback list/resolve, telemetry).
- Most pipeline mutations go through `[pipelines.go:pipelineAction]`: path id, project, permission, then the change and the re-read in one transaction. `PATCH` and `deploy` validate the body first but still report an authentication/project failure before a validation error.
- Versioned edits require `If-Match` with the edited version: missing is 428 `precondition_required`, malformed 400 `bad_if_match` (`[connections.go:parseIfMatch]`).
- Deploy creates a change request (202) instead of deploying when the project requires approval (`[pipelines.go:pipelineRoutes]`). `DELETE /pipelines/{id}` needs `pipeline.deploy`, while MCP `delete_pipeline` is offered on `pipeline.edit` — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- `DELETE /projects/{id}` is refused (409 `project_busy`) while a `cleanup` job is `queued` or `running`, and `DELETE /runtimes/{id}` (409 `runtime_cleanup_pending`) while any job of its pipelines is; a job left `running` by a crashed controller is never reclaimed, so both stay blocked — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues) (`[projects.go:projectRoutes]`, `[runtimes.go:runtimeRoutes]`).
- Writes run inside `[api.go:tx]`; `dry_run` requests run the same code in a transaction that is rolled back (`[api.go:dryTx]`). Audit events are written with `services.Record` here or in the called service.
- Lists use keyset pagination through `services.PageSQL` with the limit clamped by `services.ClampLimit` (`[api.go:paginate]`).
- Login: password sign-in can be turned off (403 `password_login_disabled`); attempts are limited per client address + lower-cased email and the counter resets on success; any failure is the same 401 `invalid_credentials` (`[auth.go:authRoutes]`).
- Connection checks/discovery are limited per user (`ratelimit.Check`); OAuth register/token per client address (`[connections.go:connectionRoutes]`, `[mcp.go:oauthProtocolRoutes]`).
- Post-sign-in redirects only go to same-site paths: leading `/`, not `//`, no backslash, else `/` (`[auth.go:safeNext]`). SSO errors redirect to `/login?error=…` rather than returning JSON.
- SAML: the ACS body is capped at `4*samlsp.MaxMetadata+4096`; the response is verified and marked used in its own transaction before the user is synced, so a later failure cannot make it reusable; the relay cookie is `SameSite=None` only when cookies are Secure (`[auth.go:samlRoutes]`, `[auth.go:samlRelayCookie]`).
- OAuth (MCP agents): only public clients with PKCE; an authorize request or code exchange naming another `resource` is `invalid_target`; authorize errors go back to the agent's redirect URI only once the client and that URI are known, else to `/oauth/consent?error=…`; a refused token request still commits what the error path wrote (reuse revocation) (`[mcp.go:oauthProtocolRoutes]`). Register and token bodies are capped at 64 KiB.
- `POST /agent/poll` authenticates with a `bza_` runtime agent token (rehashed after key rotation), requires `mode = agent` and a matching kind, and returns decrypted workload specs and pending requests only for that runtime; Flink CDC workloads are withheld from agents that do not report the feature. The body is validated before the token, so a malformed poll is 422 even without a token (`[agent.go:agentRoutes]`).
- `/metrics` is 404 unless `BEZISYNC_METRICS_TOKEN` is set, and then needs that bearer token (constant-time compare) (`[system.go:prometheus]`).
- The SSE stream checks `pipeline.view` once when it opens (`[stream.go:streamRoutes]`).
- Routes and `openapi.json` must agree (`TestRoutesMatchOpenAPI`); change both together and run `make openapi`.

## Side Effects
- DB writes, directly or via services: `users`, `user_sessions` (via `auth`), `api_tokens`, `runtimes` (agent heartbeat/state, agent token hash), `agent_requests` (results), OAuth tables, and every resource the routes manage; audit events via `services.Record`.
- Cookies: `bz_session` / `bz_csrf` (via `auth`), `bz_oidc` (path `/api/v1/auth/oidc`), `bz_saml` (path ACS).
- Outbound calls through services and the SSRF guard: connection/runtime/secret-store checks, alert channel tests, IdP discovery/token/userinfo (`oidc`); SAML metadata fetches and SSO provider tests are made here with `ssrf.HTTPClient` (10 s) (`[sso_kinds.go:fetchMetadata]`, `[sso_kinds.go:testProvider]`).
- Process state: in-memory rate limiters; each open SSE stream keeps its request goroutine and queries `pipelines` on a 2 s poll ticker, with a 15 s ping ticker (`[stream.go:streamPoll]`, `[stream.go:streamPing]`). `GET /system/telemetry` persists the install id on first use.
- Reads `config.Get()`: `CORSOrigins`, `CookieSecure`, `PublicURL`, `AgentURL`, `MetricsToken`, `Environment`, `AllowPlainHTTP`, telemetry settings, `Lab`. Logs via `slog` (e.g. `bezisync.api.stream`).

## Verification
- Run: `make verify-quick GO_PKGS="./internal/api/"`
