# domain

## Overview
The pure product rules, with no database or network access: health derivation, the pipeline
spec (parsing, validation, canonical dump and hash), transform steps, project policies,
personal-data checks, transform previews, maintenance windows, schema diffs and the
`kind: Pipeline` manifest. Services, the controller, the compiler and the API call it; it
returns values and `checks.Check` results and never performs I/O.

## Interface
- Health: `[health.go:Derive]` / `[health.go:DeriveInput]` / `[health.go:Verdict]` / `[health.go:Reason]` — one lifecycle + health verdict from observations; `[health.go:ParseMetrics]` / `[health.go:Metrics]` read Debezium JMX values; `[health.go:Diagnose]` maps known log lines to reasons; `[health.go:DeployGrace]` (5 min). Lifecycle/health constants `LifecycleDraft` … `HealthUnknown`. Python-compatible text helpers: `FormatFixed`, `HumanBytes`, `Truncate`, `SplitLines`, `PyStrip`, `PyFloat`.
- Spec: `[spec.go:PipelineSpec]` and its parts (`SourceSpec`, `SinkSpec`, `SnapshotSpec`, `Transform`, `Policies`, `ColumnRules`); `[spec.go:ParseSpec]` validates submitted/stored JSON into a spec; `[spec.go:MustSpec]` for stored versions; `[spec.go:DefaultSpec]`; `[spec.go:PipelineSpec.Dump]` (ordered canonical form); `[spec.go:SpecHash]`; `[spec.go:SummarizeChange]`; `[spec.go:SnapshotModes]`.
- Flink CDC choices: `[flinkcdc.go:FlinkCDCSpec]`, `[flinkcdc.go:FlinkCDCSpec.Effective]` (fills defaults from `DefaultFlinkCDCSpec`).
- Transforms: `[transforms.go:TransformTypes]`, `[transforms.go:Transform.Scripted]`, `[transforms.go:ScriptedSteps]`, `[transforms.go:ScriptedClasses]`, `[transforms.go:Transform.Stage]`, `[transforms.go:BuiltInTransformTypes]`, `[transforms.go:StepLabel]`; findings for the captured tables: `[findings.go:TransformFindings]`, `[findings.go:TransformChecks]`.
- Policies: `[policies.go:ProjectPolicies]`, `[policies.go:DefaultPolicies]`, `[policies.go:ParsePolicies]`, `[policies.go:LoadPolicies]`, `[policies.go:SaveViolations]`, `[policies.go:EvaluatePolicies]` / `[policies.go:EvaluatePoliciesOn]`, `[policies.go:EngineViolation]`; `[policies.go:AskAdmin]` is the default fix text of a policy failure.
- Personal data: `[personal.go:PersonalDataChecks]` / `[personal.go:DataColumn]`.
- Preview: `[preview.go:Preview]` / `[preview.go:PreviewInput]` / `[preview.go:PreviewResult]` — runs the steps on a sample event of one table; `[preview.go:ColumnKind]`.
- Windows: `[windows.go:ParseWindow]`, `[windows.go:WindowContains]`, `[windows.go:WindowNextStart]` (`"HH:MM-HH:MM"`, UTC, may wrap midnight).
- Schema drift: `[schema.go:Schema]`, `[schema.go:DiffSchema]`, `[schema.go:Change]`.
- Manifest: `[manifest.go:ParseDocuments]`, `[manifest.go:FromDocument]` (→ `[manifest.go:ParsedPipeline]`, or a `[manifest.go:ManifestError]` listing field errors), `[manifest.go:ToDocument]`, `[manifest.go:ToYAML]`, `[manifest.go:SpecDiff]`, `[manifest.go:APIVersion]` (`bezisync.io/v1`).

## Invariants
- Health is evidence-based: nothing is healthy without fresh positive evidence; an undeployed pipeline, an unreachable runtime, missing metrics or a missing health answer are `unknown`, never `healthy` (`[health.go:derive]`). A paused pipeline is `unknown`, or `at_risk` when the source reports a risk. A fresh deploy (or a Flink CDC restart) gets `[health.go:DeployGrace]` before it counts as failed.
- A running pipeline is `lagging` when it is more than the latency target behind the source (default 60 s) or its queue has less than 10% left (`[health.go:derive]`).
- `[spec.go:SpecHash]` is sha256 of `pyjson.DumpsCompact` of the canonical dump; the dump shape and key order must not change, or stored hashes stop matching.
- Spec validation limits: at most 30 steps, 20 rules, 50 renames, 30 custom properties and 10 key columns (`[transforms.go:maxSteps]` …); tables must look like `schema.table` and be unique (`[spec.go:tableRe]`).
- Values that reach `application.properties` must not contain `${` or control characters, so Debezium Server cannot expand a secret into events (`[transforms.go:noExpression]`).
- Steps that run user-written scripts (filter conditions, `ScriptedClasses`, any `language` property) fail preflight unless the project policy `allow_scripted_transforms` is on (`[transforms.go:Transform.Scripted]`, `[policies.go:EvaluatePoliciesOn]`).
- Nil policy lists mean "any"; policy failures are blocking (`[policies.go:policyFail]`). Beyond runtimes, source/sink types, snapshot modes, the resource preset and pipelines per source, policies gate engines (`allowed_engines`), built-in step types (`allowed_transforms`) and `custom` steps (`allow_custom_transforms`, `[transforms.go:transformPolicyChecks]`).
- Stored policies that fail to parse fall back to `[policies.go:DefaultPolicies]`, which allows everything (`[policies.go:LoadPolicies]`) — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).
- Personal data: a column that cannot be masked but is set to mask is a blocking failure; unprotected PII columns and rules naming missing columns are warnings; unreadable tables give `pii_unknown` (`[personal.go:PersonalDataChecks]`).
- Manifests must have `apiVersion: bezisync.io/v1` and `kind: Pipeline` (`[manifest.go:FromDocument]`); YAML is read and written through `pyyaml` so exports stay byte-identical to the Python releases.

## Side Effects
- None (pure functions; times come in through inputs such as `DeriveInput.Now`).

## Verification
- Run: `make verify-quick GO_PKGS="./internal/domain/"`
