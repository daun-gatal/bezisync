# pyyaml

## Overview
Reads and writes YAML exactly like PyYAML's `safe_load` / `safe_dump`, so exported manifests and
spec diffs are byte-identical to those of the Python implementation. The emitter is a port of
PyYAML's block-style emitter; the loader walks `gopkg.in/yaml.v3` nodes and re-resolves plain
scalars with YAML 1.1 rules. `internal/domain` uses it for manifests and spec diffs.

## Interface
- `[emit.go:Dump]` / `[emit.go:DumpAll]` — `yaml.safe_dump[_all](…, sort_keys=False[, allow_unicode])`; mappings are `*om.Map` (insertion order) or `map[string]any` (written in JSONB key order, `om.FromJSONB`), sequences `[]any` or `[]string`; other values must be nil, strings, bools, signed integers, floats, `pyjson.Float` or `json.Number`, else an error.
- `[load.go:LoadAll]` — `yaml.safe_load_all`: every document (nil for empty ones), mappings as `map[string]any`, numbers as `json.Number` except `.nan` and `.inf`/`-.inf`, which are `float64` (`[load.go:constructFloat]`).
- `[load.go:Resolve]` — the YAML 1.1 tag a plain scalar resolves to (`TagStr`, `TagBool`, `TagInt`, `TagFloat`, `TagNull`, `TagTimestamp`, …).
- `[load.go:Timestamp]` — a YAML 1.1 timestamp value (validates as neither string nor number). Only the date part is parsed; the time of day is dropped (`[load.go:scalar]`).

## Invariants
- Plain scalars resolve with PyYAML's YAML 1.1 rules, not yaml.v3's 1.2 ones (`yes`/`on`/`off` are booleans, `0`-prefixed ints are octal, `1:30` is sexagesimal) (`[load.go:scalar]`, `[load.go:resolvers]`); quoted scalars are strings unless explicitly tagged (`!!int "5"` is an int; `!!bool`, `!!float`, `!!null` and `!!timestamp` are honoured the same way).
- Mapping keys become strings the way Python would print them (`None`, `True`, …) (`[load.go:keyString]`); duplicate keys: last wins; explicit keys win over `<<` merges.
- Emitter layout is fixed to indent 2, width 80, block style (`[emit.go:bestIndent]`, `[emit.go:bestWidth]`); `[pyyaml_test.go:TestDumpMatchesPyYAML]` and `[pyyaml_test.go:TestLoadMatchesPyYAML]` check captured fixtures.
- Aliases are expanded by re-walking the anchored node (`[load.go:construct]`); there is no expansion limit in this package.

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/pyyaml/"`
