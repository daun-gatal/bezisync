# expr

## Overview
The expression language BeziSync accepts for Flink CDC transforms (ADR 0008): a whitelisted subset of Flink CDC's transform expressions (Calcite-parsed, Janino-compiled), parsed into a tree and rendered back by BeziSync, so no text a user typed reaches a pipeline definition as typed. Used by `domain` (decoding and checking computed-column and filter expressions), `compiler` (rendering them into Flink CDC transform rules) and `api` (the `expressions:check` endpoint). Pure: no I/O.

## Interface
- `[parse.go:Parse]` — text to a tree (`[parse.go:Node]`: `Col`, `Str`, `Num`, `Bool`, `Null`, `Call`, `Unary`, `Binary`, `Is`, `Distinct`, `In`, `Between`, `Like`, `Case`, `Cast`), or an `[lex.go:Error]` with a byte position and length; `[parse.go:Position]`.
- `[render.go:Canonical]` — what is stored and shown; `[render.go:Ident]` (metadata columns bare), `[render.go:Quote]`.
- `[flink.go:Flink]` / `[flink.go:FlinkChecked]` — what compiles into a Flink CDC transform (`[flink.go:MaxFlinkLength]`).
- `[check.go:JaninoName]` / `[check.go:JaninoNames]` — column names Flink CDC compiles as its time functions.
- `[check.go:Check]` — a tree's `[funcs.go:Kind]`, whether it is deterministic and the columns it reads (`[check.go:Result]`), for a `[check.go:Use]` (`UseColumn`, `UseFilter`) against an `[check.go:Env]`.
- `[funcs.go:Functions]` (`[funcs.go:Fn]`), `[funcs.go:Fits]`, `[funcs.go:SlotWords]`, `[funcs.go:Deterministic]`, `[funcs.go:DatePatterns]`, `[funcs.go:TimeUnits]`, `[parse.go:MetadataColumns]`, `[parse.go:CastTypes]`; limits `[lex.go:MaxLength]`, `[lex.go:MaxLiteral]`, `[lex.go:MaxDepth]`, `[lex.go:MaxNodes]`.

## Invariants
- Only names in `[funcs.go:Functions]` can be called: UDFs, Flink CDC's AI functions, regular expressions, struct and variant functions are refused (`TestNoAIFunctions`). Never add a function that runs user code, calls out, or can stall a TaskManager.
- The lexer refuses comments, `;`, `${`, `\`, double quotes and control characters anywhere, including inside literals and back-quoted names (`[lex.go:lex]`): Janino escapes only `\` and `"` in literals, and Flink CDC's `rewriteTryCast` misreads a `\` inside quotes.
- `Parse(Canonical(t))` is `t`, and `Canonical`/`Flink` never change their output for a tree (stored specs and `config_hash` depend on it): change them only with matrix rows proving no stored spec moves. `||` is always parenthesised against arithmetic, since its precedence differs between SQL dialects.
- `[flink.go:Flink]` follows the corpus run on Flink CDC 3.7.0 (ADR 0008): every strict sub-tree is NULL-guarded once (`CASE WHEN <inputs> IS NULL … THEN NULL ELSE … END`, a zero divisor included), `+ - * /` compute in DOUBLE and `%` in BIGINT, `-x` is `0 - x`, a comparison with a number compares both sides as DOUBLE, every cast is `TRY_CAST` with `STRING` as `VARCHAR`, `LIKE` always carries `ESCAPE '!'` (a `!` in the pattern doubled), and every operator node is parenthesised. Re-run the corpus before changing a rule.
- `[check.go:Check]` refuses: in a filter, `__data_event_type__`; comparing two number columns for equality without a CAST; mixing whole, decimal and floating-point values in `CASE`/`COALESCE`/`IF`; a timestamp with a time zone compared with another kind; a column named like a time function (`[check.go:JaninoName]`).
- `[check.go:Check]` refuses non-deterministic functions (`NOW`, `LOCALTIMESTAMP`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `UUID`) in filters, and comparisons of unlike kinds, which throw on every row.
- Never shrink the grammar: a spec stored with an expression must keep parsing.

## Side Effects
None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/expr/"` (and `go test ./internal/expr/ -run XXX -fuzz FuzzParse -fuzztime 60s` after grammar changes)
