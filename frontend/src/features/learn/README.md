# learn

## Overview
The Learn page: a searchable plain-language glossary of Debezium and CDC terms and why BeziSync cares about each. Pure static content inside the project shell.

## Interface
- `[LearnPage.tsx:GLOSSARY]` — the term list (`term`, optional Debezium name `aka`, `plain`, `why`).
- `[LearnPage.tsx:LearnPage]` — renders the glossary with a case-insensitive search over term, `aka` and the plain text.

## Invariants
- Search matches `term`, `aka` and `plain`, not `why` (`[LearnPage.tsx:LearnPage]`).

## Side Effects
- None.

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run typecheck` and `cd frontend && bun run test src/test` (source rules).
