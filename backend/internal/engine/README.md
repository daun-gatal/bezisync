# engine

## Overview
Names the two change-data-capture engines (Debezium Server and Flink CDC), which
source/destination pairs each can run, and what each can do for a pipeline. It also owns the
catalogue of Flink settings a pipeline may choose and the Flink cluster size presets. Pure
tables and functions: services, API, compiler, runtime drivers, UI, CLI and MCP all read their
engine answers from here so they offer the same actions.

## Interface
- `[engine.go:ID]` — stored engine name (`Debezium` = `"debezium"`, `FlinkCDC` = `"flink-cdc"`); `Valid`, `Title`; `[engine.go:All]` is offer order.
- `[engine.go:Supports]` / `[engine.go:Choices]` / `[engine.go:Default]` — which engines run a source → sink pair, in offer order; the default is the first choice.
- `[engine.go:Runs]` — sources/sinks an engine handles (Debezium: everything except `exceptSinks`); `[engine.go:FlinkOnlySink]`.
- `[engine.go:Of]` — reads a stored engine, mapping `""` (rows from before engines existed) to Debezium.
- `[caps.go:Caps]` / `[caps.go:CapsFor]` — per-engine capabilities (edit position, re-copy one table, add tables, start from time, saved positions, event formats, schema changes, parallel copy, hash masks, resize) with `Why` for every unavailable action.
- `[flinkconf.go:FlinkSetting]` / `[flinkconf.go:FlinkSettings]` — the user-changeable Flink keys (type, default, bounds, reason); `[flinkconf.go:FlinkDefaults]` are those BeziSync sets when a pipeline does not.
- `[flinkconf.go:CheckFlinkSetting]` / `[flinkconf.go:CheckFlinkSettings]` — validate `spec.flink_cdc.flink_conf` values; `[flinkconf.go:ParseFlinkDuration]`, `[flinkconf.go:FlinkSettingOf]`, `[flinkconf.go:SortedKeys]` helpers.
- `[flinkconf.go:FlinkSize]` / `[flinkconf.go:FlinkSizes]` / `[flinkconf.go:FlinkSlots]` — `small`/`medium`/`large` presets (JobManager + N TaskManagers × 2 slots); `Parallelism`, `TotalMemoryMiB`.

## Invariants
- Flink CDC reads only `mysql`, `postgresql`, `sqlserver` and writes only `iceberg`, `starrocks`, `doris`, `fluss`; `starrocks`, `doris`, `fluss` are Flink-only (`[engine.go:flinkSources]`, `[engine.go:flinkSinks]`, `[engine.go:flinkOnlySinks]`).
- Debezium is the default wherever it can run, because it is first in `[engine.go:All]` (`[engine.go:Default]`).
- Capabilities depend on the engine (and, for Flink start-from-time, the source type), never on the destination type; every `false` capability has a `Why` entry (`[caps.go:CapsFor]`). Flink CDC can start from a time only for `mysql` and `sqlserver`.
- Only keys in `[flinkconf.go:FlinkSettings]` may be set by a pipeline; anything else is refused by `[flinkconf.go:CheckFlinkSetting]`. Keys BeziSync depends on (addresses, ports, TLS, state, memory, slots, parallelism) must never be added to the catalogue, and every key there must be safe to change on a deployed pipeline.
- Fractions reject exponent notation (`[flinkconf.go:CheckFlinkSetting]`); durations accept only `ms`, `s`, `min` with up to 7 digits (`[flinkconf.go:flinkDuration]`).
- Parallelism is TaskManagers × `FlinkSlots` and is fixed once deployed because saved positions hold per-reader state (`[flinkconf.go:Parallelism]`).

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/engine/"`
