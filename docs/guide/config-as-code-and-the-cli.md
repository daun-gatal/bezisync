# Config-as-code and the CLI

Pipelines are data: export them as YAML, keep them in git, and check and apply them from CI with
the `bezisync remote` CLI, with the same checks and permissions as the UI.

## Export a pipeline

Every pipeline exports as YAML, from **History → Export YAML** or with `bezisync remote export`:

```yaml
apiVersion: bezisync.io/v1
kind: Pipeline
metadata: {name: orders-cdc, owner: payments-oncall, labels: {team: payments}}
spec:
  runtime: prod-k8s
  source: {connection: orders-db, tables: {include: [public.orders]}, columns: {public.orders: {mask: [email]}}}
  sink: {connection: events, naming: "cdc.{schema}.{table}", format: {type: json}}
  snapshot: {mode: initial}
  policies: {resources: medium}
  tuning: {debezium.source.snapshot.max.threads: "4"}
```

Connections and runtimes are referenced by name.

## Apply from the UI or CI

Apply manifests from **Import YAML**, or from CI with the CLI:

```bash
bezisync remote login --url https://bezisync.example.com --project prod --token <API token>
bezisync remote diff -f pipelines/                # what would change (a file, a directory of *.yaml, or -)
bezisync remote apply -f pipelines.yaml --deploy  # all or nothing; respects approvals
bezisync remote get
```

`diff` (and `apply`) print, for each pipeline:

- the spec diff;
- each setting a deploy changes, from what to what, with what it does to the running pipeline;
- what BeziSync makes of each advanced property: passed through, or refused and why;
- what would **break** a deployed pipeline: a setting that starts it over, another destination
  naming, another event format, or tables no longer captured.

When something would break, `diff` exits **3** (so CI can stop for a person), and
`apply --deploy` changes nothing, until `--confirm` is given.

The CLI sends manifests as written, so fields it doesn't know reach the server unchanged.

## Get the CLI

The CLI is the `bezisync` binary in the `bezisync` image, a single static Linux binary. Either:

- run it from the image, mounting your manifests, for example
  `docker run --rm -v "$PWD:/w" -w /w -e BEZISYNC_URL -e BEZISYNC_TOKEN
  ghcr.io/daun-gatal/bezisync remote get`; or
- copy it out once: `docker create`, then `docker cp <container>:/usr/local/bin/bezisync .`.

## API tokens

Create tokens under **Account → API tokens** (user menu).

| Property | Detail |
|---|---|
| Scope | Project-scoped, optionally read-only |
| Expiry | 30 days to a year; "Never" is possible but flagged |
| Display | A new token is shown once |

In CI, use `BEZISYNC_URL`, `BEZISYNC_TOKEN` and `BEZISYNC_PROJECT`.

## Promote to another project

**Promote** (History tab) copies a pipeline's latest version into another project as a new
version there, mapping its connections. When the other project has no matching source or
destination, it offers to copy this pipeline's own.

## Tuning settings

`tuning` holds typed engine settings from BeziSync's registry. `GET /api/v1/engines` lists them
per engine, with their bounds and what changing one does to a running pipeline.

Each setting must:

- apply to the pipeline's engine, source and destination;
- have its value written the one way BeziSync stores it (`4`, not `04`).

Otherwise saving refuses it and says why.

| Engine | Keys |
|---|---|
| Debezium | Start with `debezium.` |
| Flink CDC | Start with the block they go in (`source.`, `sink.`, `pipeline.`). Flink settings stay under `flink_cdc.flink_conf` |

A setting always wins over an advanced property with the same key.

Preflight (and the version diff) lists every setting a deploy changes: typed settings, Flink
settings and advanced properties, from what to what. For each it says what the change does to
the running pipeline: applies live, restarts it, matters only for the first copy, takes effect on
the next copy, or starts it over. It also names the heaviest of those (`settings_change`).

### Tuning from an AI agent

Agents read what a pipeline may tune with the MCP tool `list_engine_settings` (or
`GET …/pipelines/{id}/settings`). It returns:

- its engine's settings for its source and destination, each with its bounds, what changing it
  does, the current value and this project's limit (a hidden one is left out);
- whether advanced properties are allowed (their keys, never their values).

Agents change them with `update_pipeline`, which applies the same checks, limits, policies and
approvals as for their person.

---

[All documentation](../README.md)
