# Architecture Decision Records (ADR)

Significant BeziSync design decisions: the context that forced them, the options weighed, and the
consequences accepted. An ADR is the durable "why" a maintainer (or an AI agent) reads instead of
reverse-engineering intent from code. When and how to write one: [`.rules/adr.md`](../../.rules/adr.md).

These files are for contributors. The documentation site publishes `docs/README.md`, the check
reference and the threat model, but not this folder.

## Format

`NNNN-kebab-title.md`, zero-padded and sequential, in a lightweight
[MADR](https://adr.github.io/madr/)-style layout:

```markdown
# NNNN — Title

- **Status:** Proposed | Accepted | Superseded by NNNN | Deprecated
- **Date:** YYYY-MM-DD
- **Amends / Supersedes:** NNNN (when relevant)
- **Scope:** the packages, screens, endpoints, settings and docs this touches

## Context
The forces at play and what makes this hard.

## Decision
What we will do. For work about to be built, it ends with an *Implementation plan*
(target files, commits) and a *Verification plan* (tests, e2e specs, edge-case register).

## Consequences
What becomes easier or harder, and what we accept.

## Alternatives considered
The other options, and why they lost.
```

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-wizard-truth.md) | Wizard truth: Keep deleted rows, the Review autosave loop, and a badge that names the field | Accepted |
| [0002](0002-policies-fail-closed.md) | Project policies that can't be read fail closed | Accepted |
| [0003](0003-job-reclaim.md) | Reclaiming jobs a crashed controller left running | Accepted |
| [0004](0004-ssrf-on-every-dial.md) | The SSRF guard holds on every dial, not just the probe | Accepted |
| [0005](0005-iceberg-partitioning.md) | Iceberg partitioning that cannot lose deletes, the same on both engines | Accepted (Editor section superseded by 0007) |
| [0006](0006-flink-cdc-oracle-source.md) | Flink CDC reads Oracle; MongoDB, Vitess, TiDB and OceanBase stay on Debezium | Accepted |
| [0007](0007-partition-by-in-transform.md) | Partition by in the Transform step; no Iceberg change log on Flink CDC | Accepted |
| [0008](0008-flink-cdc-expressions.md) | Flink CDC expressions: more column functions, and custom expressions BeziSync parses | Accepted |
| [0009](0009-flink-cdc-destinations.md) | More Flink CDC destinations: Kafka, Elasticsearch, Apache Paimon and OceanBase | Accepted (D2's Elasticsearch test index amended by 0016) |
| [0010](0010-flink-cdc-keys-from-steps.md) | Column types in the Destination table, and Flink CDC keys from any written column | Accepted |
| [0011](0011-wizard-follows-source-and-engine.md) | The wizard follows the source and the engine | Accepted |
| [0012](0012-flink-cdc-column-layout.md) | Flink CDC writes the destination's columns in the destination's order, or doesn't start | Accepted (D2 replaced by 0013) |
| [0013](0013-flink-cdc-column-switch.md) | Flink CDC switches columns at the saved position | Accepted |
| [0014](0014-flink-cdc-restore-checkpoint-ids.md) | A restored Flink CDC job numbers its checkpoints past every one the pipeline reached | Accepted |
| [0015](0015-flink-cdc-columns-the-job-hasnt-read.md) | Flink CDC holds back source columns a restored job hasn't read yet | Accepted |
| [0016](0016-read-only-destination-checks.md) | Destination checks only read: no test tables or indexes on the destination | Proposed |
