# compiler

## Overview
Turns a pipeline spec plus its resolved source and sink connections into a runnable workload.
For Debezium that is `application.properties` (secrets only as `${ENV}` references) plus the
env that carries them, secret files, resources and an optional SSH tunnel; for Flink CDC it is
`pipeline.yaml`, `flink-conf.yaml` and `settings.json` for `flinkhelper`. The same functions
back deploys, the Review step and exports. It applies the sink capabilities and transform
checks from `connectors` and the engine tables from `engine`; it reads only process
configuration (`config.Get()`), no database or network.

## Interface
- `[compiler.go:Compile]` — Debezium Server compile; `image ""` means `DebeziumDefaultImage`.
- `[compiler.go:Compiled]` — the workload (`Properties`, `Env`, `Image`, `Resources`, `Destinations`, `Artifacts`, `Files`, `Tunnel`, `HealthPort`, `Flink`); `Render`, `ConfigHash`, `RedactedEnv`; `[compiler.go:FromFlink]` wraps a Flink compile for shared callers.
- `[compiler.go:RenderProperties]` — Java properties rendering (sorted, escaped, generated header).
- `[compiler.go:StateStore]` — JDBC offset (and schema-history) store properties for the state database.
- `[compiler.go:Resources]` / `[compiler.go:Resource]` — Debezium `small`/`medium`/`large` presets; `[compiler.go:ManagedPatterns]` — keys `spec.advanced` may not set; shared constants `[compiler.go:StateSchema]`, `[compiler.go:MetricsPort]`, `[compiler.go:SecretDir]`, `[compiler.go:HTTPPort]`.
- `[snapshot.go:SnapshotProps]` — snapshot properties in the source's own mode names from its `capture` caps.
- `[transforms.go:FilterCondition]` — Groovy expression for a typed row/operation filter.
- `[flinkcdc.go:CompileFlink]` / `[flinkcdc.go:FlinkInput]` / `[flinkcdc.go:FlinkCompiled]` — Flink CDC compile (`ConfigHash`, `Config`, `Topology`); `[flinkcdc.go:FlinkStopGrace]` / `[flinkcdc.go:FlinkStopTimeout]`.
- Test fixtures: `testdata/flink/` holds golden Flink outputs (`UPDATE_GOLDEN=1` rewrites them).

## Invariants
- `application.properties` never holds a secret value; credentials travel in `Env` (`BZ_STATE_USER`/`BZ_STATE_PASSWORD`, `BZ_SSH_KEY`, connector env) and `Files` (`[compiler.go:StateStore]`, `[compiler.go:Compile]`). `RedactedEnv` masks every value (`[compiler.go:RedactedEnv]`).
- `spec.advanced` keys must start with `debezium.` or `quarkus.` and must not match `ManagedPatterns` (identity, state, credentials, sink type, producer auth, HTTP port, the whole transform/predicate chain); violations are one 422 `managed_property` (`[compiler.go:validateAdvanced]`). Accepted advanced keys are merged last and override other compiled properties, except `tombstones.on.delete=false` for `NoTombstones` sinks, which is re-applied after (`[compiler.go:Compile]`).
- The Debezium transform chain is always `bzmeta` (drops heartbeats and BeziSync's own `bezisync.*` tables except `canary_*`, plus a source's `ManagedTopics`), then the spec steps, then `bzroute` (`[compiler.go:Compile]`, `[transforms.go:stepProps]`).
- Typed filters quote every field and literal as single-quoted Groovy strings, so input never becomes code (`[transforms.go:groovyString]`); untyped filters and `custom` steps pass the user's condition/class/properties through unchanged (`[transforms.go:stepProps]`).
- `ConfigHash` must stay byte-identical to the Python implementation (`pyjson.Dumps` of properties, env, image, resources, files, tunnel; first 16 hex chars of SHA-256) (`[compiler.go:ConfigHash]`). Flink's hash covers pipeline, conf, settings, env, files, image, size and tunnel (`[flinkcdc.go:ConfigHash]`).
- Compile fails with 422 when no table is selected, a sink option/naming/transform/format rule fails, a registry URL is missing for avro/protobuf, the SSH fields are incomplete, the snapshot mode is not offered, a value format or key-column choice is not offered by the source, a script filter has no condition, or a typed filter uses column rules on a source without `RowFilters` (`[compiler.go:Compile]`, `[snapshot.go:SnapshotProps]`, `[transforms.go:stepProps]`, `[transforms.go:sourceOptions]`). A source and sink may not mount the same secret file or (Flink) set the same env var; those are plain errors, not 422s.
- The sink's capabilities shape the workload: an `unwrap` step the sink needs is merged into the user's step or appended (`[sinkcaps.go:withSinkUnwrap]`), and `NoTombstones` / `TimePrecision` sinks get `tombstones.on.delete=false` and a readable `time.precision.mode` unless the user chose a non-default mode (`[sinkcaps.go:sinkSourceProps]`).
- Flink CDC compile refuses: no registered image (`engine_unavailable`), a pair without `FlinkSource`/`FlinkSink`, `advanced`, value formats, a copy delay/window, a snapshot mode with no Flink startup mode, a start-from-time on sources other than `mysql`/`sqlserver`, Flink settings outside the catalogue, destination name collisions, masks, script filters and outbox/flatten/custom steps, an operation filter that keeps only one of insert/snapshot, column choices when the deployed columns are unknown, and destinations with `NoDefaults` unless new columns are held (`[flinkcdc.go:CompileFlink]`, `[flinkcdc.go:startupOptions]`, `[flinktransforms.go:flinkTransforms]`, `[flinktransforms.go:flinkFilter]`). An unknown size falls back to `small`.
- Flink transforms quote names as back-quoted identifiers and values as SQL literals (`[flinktransforms.go:sqlIdent]`, `[flinktransforms.go:sqlString]`); `pipeline.yaml` values are JSON-quoted scalars except bare `${BZ_…}` placeholders, which the helper fills (`[flinktransforms.go:yamlScalar]`).
- `flink-conf.yaml` puts BeziSync's own keys (RPC/blob ports, REST bound to `127.0.0.1`, memory, slots, parallelism, checkpoint dirs, `classloader.resolve-order: parent-first`, internal mutual TLS with the pipeline keystore) before the catalogue keys and rejects line breaks in any value (`[flinkcdc.go:flinkConf]`). Keystore passwords go only into the secret settings file, never `flink-conf.yaml` (`[flinkcdc.go:CompileFlink]`).
- Flink `settings.json` always restores `latest` above `RestoreAfter`, keeps 5 checkpoints, uses `runtime.FlinkStateDir`, health port `HTTPPort` and `FlinkStopTimeout` (`[flinkcdc.go:CompileFlink]`).

## Side Effects
- Reads process configuration via `config.Get()`: `ConnectorStateDatabaseURL` (state database URL, whose credentials are copied into `Env`), `AgentImage` (tunnel image), `DebeziumDefaultImage` (`[compiler.go:StateStore]`, `[flinkcdc.go:flinkStateEnv]`). Otherwise none.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/compiler/"`
