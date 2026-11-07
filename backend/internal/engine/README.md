# engine

## Overview
Names the two change-data-capture engines (Debezium Server and Flink CDC), which
source/destination pairs each can run, and what each can do for a pipeline. It also owns the
**settings registry**: every engine, source, destination and pipeline option a pipeline may tune,
for both engines, typed and bounded, with what changing it does to a running pipeline. It holds
the Flink cluster size presets and packs custom Flink CDC clusters onto nodes. Pure tables and
functions: services, API, compiler, runtime drivers, UI, CLI and MCP all read their engine answers
from here so they offer the same actions.

## Interface
- `[engine.go:ID]` — stored engine name (`Debezium` = `"debezium"`, `FlinkCDC` = `"flink-cdc"`); `Valid`, `Title`; `[engine.go:All]` is offer order.
- `[engine.go:Supports]` / `[engine.go:Choices]` / `[engine.go:Default]` — which engines run a source → sink pair, in offer order; the default is the first choice.
- `[engine.go:Runs]` — sources/sinks an engine handles (Debezium: everything except `exceptSinks`); `[engine.go:FlinkOnlySink]`.
- `[engine.go:Of]` — reads a stored engine, mapping `""` (rows from before engines existed) to Debezium.
- `[caps.go:Caps]` / `[caps.go:CapsFor]` — per-engine capabilities (edit position, re-copy one table, add tables, start from time, saved positions, event formats, schema changes, parallel copy, hash masks, resize) with `Why` for every unavailable action.
- **Settings registry** (`settings.go`):
  - `[settings.go:Setting]` — one tunable option: engine, scope (`ScopeEngine`/`ScopeSource`/`ScopeSink`/`ScopePipeline`), wizard stage, type and bounds, default, why, the connector types it applies to, `Derived` (BeziSync sets a default from the size), Debezium release bounds (`Since`/`Until`/`Renamed`) and `Experimental`.
  - `[settings.go:Impact]` / `[settings.go:Impacts]` — live, restart, first_copy, next_copy, breaks_positions, lightest first; `Rank` orders them.
  - `[settings.go:Settings]` (all of `[settings.go:DebeziumSettings]`, `[flinkconf.go:FlinkSettings]`, `[settings.go:FlinkPipelineSettings]`), `[settings.go:SettingsFor]` (per engine and connector pair), `[settings.go:SettingOf]`, `[settings.go:TuningScopes]`.
  - `[settings.go:CheckSetting]` / `[settings.go:CanonicalSetting]` — validate a value; the one stored spelling of a valid value.
- `[flinkconf.go:FlinkSetting]` (an alias of `Setting`) / `[flinkconf.go:FlinkSettings]` — the engine-scoped Flink keys a pipeline sets in `spec.flink_cdc.flink_conf`; `[flinkconf.go:FlinkDefaults]` are those BeziSync sets when a pipeline does not.
- `[flinkconf.go:CheckFlinkSetting]` / `[flinkconf.go:CheckFlinkSettings]` — validate `flink_conf` values through `CheckSetting`; `[flinkconf.go:ParseFlinkDuration]`, `[flinkconf.go:FlinkSettingOf]`, `[flinkconf.go:SortedKeys]` helpers.
- `[flinkconf.go:FlinkSize]` / `[flinkconf.go:FlinkSizes]` / `[flinkconf.go:FlinkSlots]` — `small`/`medium`/`large` presets (JobManager + N TaskManagers × 2 slots); a custom cluster also carries `Slots` and `Readers`. `SlotsPerTaskManager`, `Parallelism`, `TotalMemoryMiB`.
- `[pack.go:Pack]` — packs a custom cluster (parallelism, CPU and memory per slot) into the fewest TaskManagers the largest node (`[pack.go:Node]`) holds, at most `[pack.go:MaxSlotsPerTaskManager]` slots each plus `[pack.go:TaskManagerOverheadMiB]`; returns `[pack.go:Packed]` with the reason in words and `Fits`.

## Invariants
- Flink CDC reads only `mysql`, `postgresql`, `sqlserver`, `oracle` (the sources with a Flink CDC pipeline source; MongoDB, Vitess and TiDB have none, ADR 0006) and writes only `iceberg`, `starrocks`, `doris`, `fluss`; `starrocks`, `doris`, `fluss` are Flink-only (`[engine.go:flinkSources]`, `[engine.go:flinkSinks]`, `[engine.go:flinkOnlySinks]`).
- Debezium is the default wherever it can run, because it is first in `[engine.go:All]` (`[engine.go:Default]`).
- Capabilities depend on the engine (and, on Flink CDC, the source type), never on the destination type; every `false` capability has a `Why` entry (`[caps.go:CapsFor]`). Flink CDC can start from a time only for `mysql` and `sqlserver`; on `oracle` it can't add tables to a pipeline that restores a saved position (`AddTables` false; preflight `flink_tables_added` enforces it).
- The registry is the only list of tunable options. `SettingsFor`, `SettingOf` and `CheckSetting` are the only way to read and check one; compiler, preflight, wizard, policies and MCP all read it. Raw keys outside it are judged by `compiler.ClassifyAdvanced`, never by a second list.
- Keys BeziSync sets itself (addresses, ports, TLS, state, slots, parallelism) are never in the registry unless they are `Derived`; the compiler test `TestRegistryKeysAreNotCompiledByBeziSync` holds that.
- `CheckSetting` must keep accepting every value it accepted before: the compiler re-checks deployed specs on every reconcile.
- Fractions reject exponent notation; durations accept only `ms`, `s`, `min` with up to 7 digits (`[flinkconf.go:flinkDuration]`).
- `Slots` and `Readers` are omitted from a `FlinkSize` when 0, so the presets' config hash does not change.
- `Pack` is pure. A shape is resolved once, at deploy, and stored by the services, so a running pipeline's config hash never follows the nodes. When one slot needs more than the node offers, `Fits` is false and the shape is one slot per TaskManager.

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/engine/"`
