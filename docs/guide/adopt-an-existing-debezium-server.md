# Adopt an existing Debezium Server

Bring a Debezium Server you already run under BeziSync: its `application.properties` becomes a
pipeline, with its own slot and offsets.

## Adopt a server

1. Stop the old deployment.
2. Go to **Pipelines → Adopt existing** and paste its `application.properties`.
3. Review what BeziSync mapped (see below) and fix anything it lists as needing a change.
4. Choose "Only new changes" to avoid copying rows again. BeziSync uses its own slot and offsets,
   not the old server's.
5. Afterwards, drop the old slot.

## What BeziSync maps

BeziSync maps tables, snapshot mode, naming, format, value formats, key columns and transforms.
Known SMTs (single message transforms) become steps. Any other SMT becomes a custom step, which
needs its policy.

Excluded and masked columns (`column.exclude.list`, `column.mask.with.N.chars`) become the Tables
step's choices. Patterns it can't map are named in a warning.

## How properties are sorted

Every property lands in one of five buckets:

| Bucket | What happens |
|---|---|
| Typed settings | Created as settings. One out of its bounds is listed as needing a change, never clamped |
| Wizard choices | A key a choice sets stays that choice, even when it is also a setting |
| Managed by BeziSync | Connection, offsets, slot, history |
| Kept as advanced | Kept as advanced properties |
| Refused | Not accepted |

---

[All documentation](../README.md)
