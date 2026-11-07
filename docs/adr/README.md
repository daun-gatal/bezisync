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
| [0005](0005-iceberg-partitioning.md) | Iceberg partitioning that cannot lose deletes, the same on both engines | Accepted |
| [0006](0006-flink-cdc-oracle-source.md) | Flink CDC reads Oracle; MongoDB, Vitess, TiDB and OceanBase stay on Debezium | Accepted |
