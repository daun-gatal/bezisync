# schemaval

## Overview
Validates data against a Pydantic model's JSON Schema with Pydantic semantics (lax coercion,
defaults filled, unknown keys ignored, identical errors) and returns the equivalent of
`model_dump(mode="json")`. It is the runtime half of the captured schemas: connector configs
(`connectors/descriptors.json`), sink options and secret-store/runtime/alert-channel models
(`schemas`) are both rendered as UI forms and validated here. Built on `val`; pure.

## Interface
- `[schemaval.go:Schema]` — a JSON Schema decoded with key order kept (`*om.Map`).
- `[schemaval.go:Model]` — validates an object against a model schema; returns every valid or missing-optional property (defaults or `nil`, `[]` for arrays without a default; a field that fails validation is left out, and a non-object input returns an empty map) and whether no new errors were added.
- `[schemaval.go:Value]` — validates one value against a field schema (`anyOf` with null, `enum`, string incl. `format: email`, integer, number, boolean, array with `items`/`minItems`/`maxItems`, object with `properties` or `additionalProperties`).
- `[schemaval.go:Properties]` — field names in validation order; `[schemaval.go:Field]` — one property's schema.

## Invariants
- Validation order follows Pydantic declaration order: `ssh_*` fields (the SSH mixin's base-class fields) come before all others (`[schemaval.go:Properties]`).
- Unknown keys are ignored, never rejected, and are not copied into the result (`[schemaval.go:Model]`).
- A missing required field records `missing` and is skipped; a missing optional field takes its schema `default` (`[schemaval.go:Model]`).
- `anyOf` with more than one non-null alternative validates only against the first (`[schemaval.go:Value]`); no model uses that shape today.
- `additionalProperties` maps are validated in sorted key order for stable errors (`[schemaval.go:sortedKeys]`).

## Side Effects
- None.

## Verification
- Run: No co-located tests; covered by `cd backend && go vet ./internal/schemaval/` and `make verify-quick GO_PKGS="./internal/services/ ./internal/api/"` (connection, runtime and secret-store validation).
