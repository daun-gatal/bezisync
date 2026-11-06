# backend/internal

## Overview

The Go packages behind the one `bezisync` binary (`cmd/bezisync`): the API with MCP, the
controller, the agent, the Flink CDC helper and the CLI. Packages are internal to the module.
Handlers stay thin and call `services/`, which calls connectors, the compiler and runtime drivers;
PostgreSQL is the store and the job queue. `testdb/` is a test-only helper (no contract).

## Subdirectories & Boundaries

### Entry and CLI

- cli: [./cli/README.md](./cli/README.md) - The `bezisync` command line (cobra).
- version: [./version/README.md](./version/README.md) - Holds the BeziSync release version compiled into the binary.
- legal: [./legal/README.md](./legal/README.md) - Carries the licences BeziSync ships in its binary: BeziSync's own NOTICE and Apache License, then every linked third-party Go module with its licence and notices.

### HTTP surface

- api: [./api/README.md](./api/README.md) - Package `api` is the versioned REST API (mounted under `/api/v1`), the system routes (`/healthz`, `/readyz`, `/metrics`, docs) and the OAuth discovery documents.
- httpapi: [./httpapi/README.md](./httpapi/README.md) - Package `httpapi` is the HTTP framework under the API: the chi router with problem-shaped 404/405, the outer middleware (request id, security headers, access log, panic recovery), optional credentialed CORS, JSON and RFC 9457 problem writers, and FastAPI-compatible body and query-parameter readers.
- mcpserver: [./mcpserver/README.md](./mcpserver/README.md) - Package `mcpserver` is BeziSync's Model Context Protocol endpoint, mounted by [`api`](./api/README.md) at `POST /api/v1/mcp`.
- problem: [./problem/README.md](./problem/README.md) - Package `problem` is the single error type the API returns to clients: an RFC 9457 problem document with a stable snake_case `code`, a plain-sentence `detail` and the request id.
- ratelimit: [./ratelimit/README.md](./ratelimit/README.md) - Package `ratelimit` is a small in-process sliding-window limiter for brute-force sensitive endpoints (login, connection checks, feedback, MCP OAuth).

### Identity and security

- auth: [./auth/README.md](./auth/README.md) - Package `auth` authenticates every API request and resolves project access.
- oidc: [./oidc/README.md](./oidc/README.md) - Package `oidc` implements SSO sign-in with an authorization code and PKCE, for OpenID Connect providers (ID token verified against the issuer's JWKS) and plain OAuth2 providers such as GitHub (identity read from a userinfo endpoint).
- samlsp: [./samlsp/README.md](./samlsp/README.md) - Package `samlsp` is BeziSync as a SAML 2.0 service provider (Settings → Sign-in, kind `saml`).
- security: [./security/README.md](./security/README.md) - Package `security` holds the cryptographic primitives: Argon2 password hashing, opaque bearer tokens and their keyed hashes, and AES-256-GCM encryption of stored secrets under the master key.
- secrets: [./secrets/README.md](./secrets/README.md) - Reads connection secrets kept in external stores (HashiCorp Vault KV v2, AWS Secrets Manager, Google Secret Manager, Kubernetes Secrets).
- ssrf: [./ssrf/README.md](./ssrf/README.md) - Package `ssrf` guards every connection BeziSync opens on a user's behalf (connection checks, webhooks, secret stores, HTTP sinks, connector previews).

### Product logic

- services: [./services/README.md](./services/README.md) - The business logic behind the API handlers, the MCP tools, the CLI and the controller: pipelines (versions, preflight, deploy, pause/resume, delete, offsets), connections and their secrets, external secret-store references, runtimes and the agent-backed driver, approvals, alerts and notifications, audit, the image catalog, manifests, copy between projects, SSO, MCP settings and OAuth, telemetry and key rotation.
- domain: [./domain/README.md](./domain/README.md) - The pure product rules, with no database or network access: health derivation, the pipeline spec (parsing, validation, canonical dump and hash), transform steps, project policies, personal-data checks, transform previews, maintenance windows, schema diffs and the `kind: Pipeline` manifest.
- checks: [./checks/README.md](./checks/README.md) - The preflight check model shared by connectors, deploy preflight, secret stores and project policies: one `Check` per finding (stable code, plain message, technical detail, why, copyable fix) collected into a `Report`.
- engine: [./engine/README.md](./engine/README.md) - Names the two change-data-capture engines (Debezium Server and Flink CDC), which source/destination pairs each can run, and what each can do for a pipeline.
- models: [./models/README.md](./models/README.md) - Mirrors the database schema: one struct per table, with columns mapped by `db` tags.

### Connectors and compilation

- connectors: [./connectors/README.md](./connectors/README.md) - The connector protocol and every source and sink type BeziSync supports.
- compiler: [./compiler/README.md](./compiler/README.md) - Turns a pipeline spec plus its resolved source and sink connections into a runnable workload.
- flinkhelper: [./flinkhelper/README.md](./flinkhelper/README.md) - The process BeziSync injects into every container of a Flink CDC pipeline (`bezisync engine flink-cdc jobmanager|taskmanager`, copied in with `bezisync install-self`).

### Runtime and reconciliation

- controller: [./controller/README.md](./controller/README.md) - `bezisync controller`: the leader-elected loop that turns the desired state the API writes into running workloads and evidence.
- runtime: [./runtime/README.md](./runtime/README.md) - The drivers for where pipeline workloads run: a Docker Engine (local or remote, optional TLS) and a Kubernetes namespace (in-cluster, kubeconfig, or GKE/EKS bearer tokens minted in process).
- agent: [./agent/README.md](./agent/README.md) - `bezisync agent`: the outbound-only process that runs next to a remote Docker host or Kubernetes cluster for runtimes in agent mode.

### Storage and configuration

- db: [./db/README.md](./db/README.md) - Owns the PostgreSQL connection pool, transactions, generic query helpers and the JSONB column types.
- migrate: [./migrate/README.md](./migrate/README.md) - Applies the database schema with goose from the embedded SQL files in [sql](./migrate/sql/README.md).
- config: [./config/README.md](./config/README.md) - Reads the process configuration from `BEZISYNC_*` environment variables, plus an optional `.env` file in the working directory that real variables override.
- logging: [./logging/README.md](./logging/README.md) - Configures the process's structured logs on top of `log/slog`.

### Validation and compatibility

- val: [./val/README.md](./val/README.md) - Request-body validation that reproduces Pydantic v2 lax-mode behaviour: the same error locations, `type` codes and messages, so API clients see the same 422 responses the Python implementation produced.
- schemas: [./schemas/README.md](./schemas/README.md) - Embeds the JSON Schemas of the secret-store, runtime and alert-channel models (`model_schemas.json`, captured from the Python implementation).
- schemaval: [./schemaval/README.md](./schemaval/README.md) - Validates data against a Pydantic model's JSON Schema with Pydantic semantics (lax coercion, defaults filled, unknown keys ignored, identical errors) and returns the equivalent of `model_dump(mode="json")`.
- pyjson: [./pyjson/README.md](./pyjson/README.md) - Encodes values exactly like Python's `json.dumps`.
- pyyaml: [./pyyaml/README.md](./pyyaml/README.md) - Reads and writes YAML exactly like PyYAML's `safe_load` / `safe_dump`, so exported manifests and spec diffs are byte-identical to those of the Python implementation.
- pytime: [./pytime/README.md](./pytime/README.md) - Formats and parses times exactly like the former Python implementation, so API responses and stored values keep their shape.
- om: [./om/README.md](./om/README.md) - An insertion-ordered JSON object for documents whose key order users see: YAML manifests and responses that must keep Python's dict order.
- difflib: [./difflib/README.md](./difflib/README.md) - A port of Python's `difflib.SequenceMatcher` and `unified_diff` over lists of lines, so the spec diffs BeziSync shows are identical to those of the former Python implementation.
- contract: [./contract/README.md](./contract/README.md) - Test-only helper that loads the JSON fixtures captured from the former Python implementation, so cross-implementation tests can assert that Go produces the same output.
