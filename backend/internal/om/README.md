# om

## Overview
An insertion-ordered JSON object for documents whose key order users see: YAML manifests and
responses that must keep Python's dict order. It also reproduces the key order PostgreSQL
gives JSONB objects. `pyjson` and `pyyaml` recognise `*om.Map` and keep its order.

## Interface
- `[om.go:Map]` / `[om.go:New]` — the ordered map; `Set` (new keys go last, existing keep their place), `SetFirst`, `Get`, `Delete`, `Pop`, `Keys`, `Len`.
- `[om.go:Map.MarshalJSON]` — writes keys in insertion order.
- `[om.go:Plain]` / `[om.go:Map.Plain]` — recursive conversion to plain `map[string]any`.
- `[om.go:JSONBKeys]` / `[om.go:FromJSONB]` — JSONB key order (shorter keys first, then bytewise) and an ordered map built in that order.
- `[om.go:DecodeOrdered]` — decodes JSON keeping object key order (objects become `*Map`, numbers `json.Number`).

## Invariants
- `[om.go:Map.Keys]` returns a copy; callers cannot reorder the map through it.
- `[om.go:JSONBKeys]` must match PostgreSQL's JSONB ordering, since that is the order Python saw for every dict loaded from the database.

## Side Effects
- None.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/om/` plus `make verify-quick GO_PKGS="./internal/pyyaml/ ./internal/domain/"`.
