# pyjson

## Overview
Encodes values exactly like Python's `json.dumps`. Stored hashes (pipeline spec hashes,
connector config hashes) were computed by the Python implementation, so they only stay stable
if Go produces the same bytes: `ensure_ascii` escaping, Python's float repr, sorted keys and
the chosen separators. It also decodes JSON with numbers kept as `json.Number`.

## Interface
- `[pyjson.go:Encode]` / `[pyjson.go:Options]` — render with `SortKeys`, `Compact` and `EnsureASCII`.
- `[pyjson.go:Dumps]` — `json.dumps(v, sort_keys=True)`; `[pyjson.go:DumpsCompact]` — with `separators=(",", ":")` (the hashing form).
- `[indent.go:DumpsIndent]` — `json.dumps(v, indent=n, sort_keys=True)` (used for the OpenAPI document).
- `[pyjson.go:Float]` — a value Python held as a float (`10.0`, not `10`); `[pyjson.go:FloatRepr]` — Python's `repr(float)`.
- `[pyjson.go:OrderedMap]` — objects that keep key order (`om.Map`), written in order unless `SortKeys`.
- `[pyjson.go:DecodeGeneric]` / `[pyjson.go:ToGeneric]` — decode (or round-trip a struct) to maps/slices/`json.Number`.

## Invariants
- Output must stay byte-identical to Python: changing it changes stored hashes. `[pyjson_test.go:TestMatchesPythonJSONDumps]` and `[indent_test.go:TestDumpsIndentMatchesPython]` check captured fixtures.
- A `json.Number` without `.`, `e` or `E` is written verbatim as an integer; anything else goes through `[pyjson.go:FloatRepr]` (`[pyjson.go:encodeNumber]`).
- `[pyjson.go:FloatRepr]` uses fixed notation for decimal exponents in [-4, 16) and scientific otherwise, and writes `NaN`/`Infinity`/`-Infinity` like Python.
- Unsupported types (and non-string map keys) are an error, never a silent fallback (`[pyjson.go:Encode]`). Structs and pointers to `json.Marshaler`s are encoded through their `encoding/json` form (`[pyjson.go:ToGeneric]`), so float fields need `Float` to keep their `.0`.
- A nil Go map is written as `{}` and a nil slice as `[]`, not `null` (`[pyjson.go:encode]`).

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/pyjson/"`
