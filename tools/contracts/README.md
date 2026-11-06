# contracts

## Overview
Checks the directory contracts: the `README.md` in every code directory and the root
`ARCHITECTURE.md` (format and rules: [`.rules/egm.md`](../../.rules/egm.md#directory-contracts)).
[cmd/contracts](../cmd/contracts/README.md) runs it as `make contracts-check`, inside `make verify`,
`make verify-quick` and CI's Tools job.

## Interface
- `[contracts.go:Check]` — runs every check against a repository root and returns the `Problem`s in directory order.
- `[contracts.go:Problem]` — one failure: the contract's repository-relative path and a message (`File: Message` as a string).
- `[contracts.go:ParseIgnore]` / `[contracts.go:Ignored]` — read `.egmignore` (one directory prefix per line, `#` comments) and match a directory against it.
- `[contracts.go:IsSource]` — non-test source files (`.go`, `.ts(x)`, `.js(x)`, `.sh`, `.sql`, …; not `*_test.go`, `*.test.*`, `*.spec.*`, `.d.ts`).
- `[contracts.go:Links]`, `[contracts.go:Citations]` (`[contracts.go:Citation]`), `[contracts.go:TestTargets]` (`[contracts.go:TestTarget]`) — parse a contract's relative links, `[path:Symbol]` citations and the test paths its verification commands name (the quoted `GO_PKGS` list of `make verify-quick`, resolved under `backend/`, and `cd <dir> && bun run test|npx vitest run|go test <paths…>`, up to the first flag).
- `[contracts.go:ContainsIdentifier]` — whole-identifier match for cited symbols.
- Constants `Contract` (`README.md`), `Architecture` (`ARCHITECTURE.md`), `IgnoreFile` (`.egmignore`).

## Invariants
- A contract is required for every directory that holds non-test source, or sits above one, as listed by `git ls-files --cached --others --exclude-standard` (files deleted in the working tree and files at the repository root do not count). Hidden folders, `node_modules`/`dist`/`build`/`coverage`, test folders (`__tests__`, `testdata`, `test`, `tests`, `fixtures`, `unittests`) and `.egmignore` prefixes are skipped (`[contracts.go:codeDirectories]`).
- A directory that itself holds source gets a component contract: Overview (or System Overview), Interface, Invariants, Side Effects and Verification. One that only sits above source gets a blueprint: Overview and Subdirectories (`[contracts.go:component]`, `[contracts.go:blueprint]`). `ARCHITECTURE.md` is checked for links and citations only.
- Links inside code fences and links with a scheme or an absolute path are not checked (`[contracts.go:Links]`).
- Only `[path]` / `[path:Symbol]` brackets whose path ends in a code, YAML or JSON extension count as citations, and not when they are a link's text (`[contracts.go:Citations]`). A citation resolves relative to the contract first, then to the repository root, and must name a file. Only the last `.`-separated part of the symbol must appear in the file, as a whole identifier (`[contracts.go:citations]`).
- A verification target passes when it has a test file directly, in a `__tests__` folder, anywhere below it (for recursive targets: a path ending in `...` or one not starting with `./`), or when it is itself a test file; symlinks and `node_modules`/`dist`/`build`/`coverage` are not searched (`[contracts.go:hasTests]`).
- Problems are reported once each (`[contracts.go:add]`).

## Side Effects
- Runs `git ls-files` in the repository and reads `.egmignore`, the contracts, `ARCHITECTURE.md` and cited files. It writes nothing.

## Verification
- Run: `cd tools && go test ./contracts/`
