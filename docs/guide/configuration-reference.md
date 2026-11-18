# Configuration reference

BeziSync is configured with environment variables. All of them are prefixed with `BEZISYNC_`;
the tables below leave the prefix out.

## Environment and databases

| Variable | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `development` | `production` enforces secure settings |
| `DATABASE_URL` | local Postgres | Metadata database: a PostgreSQL URL (`postgresql://…`; the `postgresql+psycopg://` form works too) |
| `STATE_DATABASE_URL` | = `DATABASE_URL` | Connector offsets and schema history |
| `CONNECTOR_STATE_DATABASE_URL_OVERRIDE` | — | The state DB as connectors reach it, if different |

## Security and browser access

| Variable | Default | Purpose |
|---|---|---|
| `MASTER_KEY` / `PREVIOUS_MASTER_KEYS` | dev key / — | Secret encryption (base64, 32 bytes) and rotation |
| `PUBLIC_URL`, `COOKIE_SECURE`, `SESSION_TTL_HOURS` | `http://localhost:8080`, `false`, `12` | Browser access |
| `ALLOW_PLAIN_HTTP` | `false` | Production over http when the network encrypts traffic (e.g. Tailscale) |
| `BOOTSTRAP_ADMIN_EMAIL` / `_PASSWORD` | `admin@bezisync.local` / — | First administrator |
| `SSRF_ALLOW_CIDRS` / `SSRF_EXTRA_DENY_CIDRS` | — | Networks BeziSync may (not) reach for connections, secret stores and alerts: checks and every later connection (loopback, link-local and cloud metadata are refused by default) |

## Images and agents

| Variable | Default | Purpose |
|---|---|---|
| `DEBEZIUM_DEFAULT_IMAGE` | `quay.io/debezium/server:3.7.0.Final` | Image when neither pipeline nor runtime picks one, unless an Admin chose a catalog default |
| `ICEBERG_IMAGE` | the pinned `ghcr.io/memiiso/debezium-server-iceberg` build | Image of Apache Iceberg pipelines (e.g. a mirror in a private registry) |
| `AGENT_URL` / `AGENT_IMAGE` / `AGENT_CHART` / `AGENT_CHART_VERSION` | `PUBLIC_URL` / this release's `bezisync` image / OCI chart / the agent chart version the control-plane chart was released with (unset: the newest) | Used in generated agent install commands |

## Monitoring, alerts and operations

| Variable | Default | Purpose |
|---|---|---|
| `METRICS_TOKEN`, `METRICS_RETENTION_DAYS` | —, `30` | Prometheus endpoint and chart history |
| `SMTP_*` | — | Email alert channel |
| `TELEMETRY_ENABLED`, `TELEMETRY_URL` | `false`, — | Opt-in anonymous statistics |
| `CONTROLLER_TICK_SECONDS`, `LOG_LEVEL`, `LOG_JSON` | `5`, `INFO`, `true` | Operations |

## Labs and the demo Sandbox

| Variable | Default | Purpose |
|---|---|---|
| `LAB` | `false` | An open lab: the sign-in page invites anyone in and links the public testing policy (`/testing-policy`); feedback is addressed to the BeziSync team |
| `SANDBOX_*` | — | Demo stack: create the Sandbox project (`SANDBOX_SOURCE_*` and `SANDBOX_KAFKA_BOOTSTRAP`), once, on its first start |
| `SANDBOX_MYSQL_*`, `SANDBOX_LEGACY_*`, `SANDBOX_WAREHOUSE_*`, `SANDBOX_HTTP_URL` | — | Optional Sandbox extras, created with it: a MySQL source `demo-mysql`; a PostgreSQL source `legacy-postgres` that isn't ready for change data capture (preflight shows the fixes); a JDBC destination `demo-warehouse` and an HTTP destination `demo-http`, each with a template. Each needs its `_HOST` (or the URL); `_PORT`, `_USER` and `_DATABASE` have the lab's defaults |
| `SANDBOX_TEAM_PASSWORD` | — | Also add Sandbox teammates `editor@`, `operator@` and `viewer@bezisync.local`, one per project role, signing in with this password (12 characters or more; an email already in use is skipped) |

---

[All documentation](../README.md)
