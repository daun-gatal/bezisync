# val

## Overview
Request-body validation that reproduces Pydantic v2 lax-mode behaviour: the same error
locations, `type` codes and messages, so API clients see the same 422 responses the Python
implementation produced. Callers walk decoded JSON (`pyjson.DecodeGeneric` values) field by
field with a `V` collector; `schemaval` builds on it to validate against captured JSON Schemas.
Pure: no I/O.

## Interface
- `[val.go:V]` — error collector (`Fail`, `ValueError`, `OK`, `Count`, `Problem` → `problem.Validation` 422).
- `[val.go:Loc]` / `[val.go:Add]` — Pydantic error location (field names and list indexes as strings).
- `[val.go:Object]` — an object being validated, started with `V.Model` (nested, `model_type` error) or `V.Body` (request body, FastAPI wording); `Failed` when the input was not an object; `Get`/`Raw` (mark the key read), `Has` (does not), `Loc`, `V`, `Forbid` for `extra="forbid"` models.
- `[val.go:Field]` — one key with `Loc`, `Raw`, `Present`; `Required`, `IsNull`.
- Scalars (lax coercion): `[val.go:Str]`, `[val.go:Constrained]` with `[val.go:StrRules]`, `[val.go:Int]`, `[val.go:Float]`, `[val.go:Bool]`, `[val.go:IntIn]`/`[val.go:FloatIn]`/`[val.go:Bounds]` with `[val.go:Num]` (`[val.go:F]` pointer helper).
- Choices and containers: `[val.go:Literal]`, `[val.go:Enum]`, `[val.go:LiteralList]`, `[val.go:List]`, `[val.go:ListLen]`, `[val.go:StrList]`, `[val.go:Dict]`, `[val.go:StrDict]`.
- UUIDs: `[val.go:UUID]`, `[val.go:ParseUUID]` (Rust `uuid` crate wording).
- `[email.go:Email]` / `[email.go:ValidateEmail]` — `EmailStr` as email-validator 2.x without deliverability checks; returns the normalized address.
- `[datetime.go:Datetime]` / `[datetime.go:ParseDatetime]` — speedate parsing: RFC 3339-ish datetimes, bare dates (midnight UTC), Unix seconds or milliseconds; naive values are UTC.

## Invariants
- Messages and `type` codes are a compatibility contract with the Python API; change them only together with the clients/tests that compare them (`[val.go:Fail]`).
- `[val.go:Forbid]` must be called after every field has been read with `Get`/`Raw`; it reports unread keys as `extra_forbidden`, in sorted key order (`[val.go:sortedKeys]`), after field errors.
- `[val.go:Constrained]` checks min, then max, then pattern; first failure wins. Patterns use search semantics and are cached in a `sync.Map` (`[val.go:patternCache]`) because requests validate concurrently; an invalid pattern panics (`regexp.MustCompile`).
- String lengths count runes, not bytes (`[val.go:Constrained]`).
- `[val.go:Bounds]` reports at most one error per value.
- `[email.go:ValidateEmail]` rejects inputs longer than `maxEmailInput` (2048) before parsing.

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/val/"`
