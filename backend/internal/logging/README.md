# logging

## Overview
Configures the process's structured logs on top of `log/slog`. JSON is the default (one object
per line: `ts`, `level`, `logger`, `msg`, then extra fields), so log shippers need no parsing
rules; a text layout is available for local use. Every server role calls `Configure` at start-up.

## Interface
- `[logging.go:Configure]` — installs the default slog handler from `LOG_JSON` and `LOG_LEVEL` (`config.Get()`).
- `[logging.go:Logger]` — a named logger (`"bezisync.http"`, …) that resolves the default handler at log time.
- `[logging.go:NewHandler]` / `[logging.go:Handler]` — the JSON or `time LEVEL logger message k=v` handler.
- `[logging.go:ParseLevel]` — Python level names (`DEBUG`, `INFO`, `WARNING`/`WARN`, `ERROR`/`CRITICAL`) to slog levels; anything else is INFO.

## Invariants
- Package-level loggers created before `Configure` still follow the configured format and level, because `[logging.go:deferred]` forwards each record to the current default handler (`[logging_test.go:TestPackageLoggersFollowLaterConfiguration]`).
- JSON lines keep Python's key order and `ensure_ascii` encoding via `pyjson`; non-scalar values are stringified (`[logging.go:jsonValue]`); a record that cannot be encoded becomes an ERROR line instead of being dropped.
- Groups are flattened (`[logging.go:Handler.WithGroup]`); writes are serialised by a shared mutex.
- The handler does no redaction: callers must never pass secrets, tokens or passwords as attributes (`AGENTS.md` golden rule 1).

## Side Effects
- Writes log lines to stdout (`[logging.go:Configure]`); reads `BEZISYNC_LOG_JSON` / `BEZISYNC_LOG_LEVEL` through `config`.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/logging/"`
