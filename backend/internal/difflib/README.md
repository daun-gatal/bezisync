# difflib

## Overview
A port of Python's `difflib.SequenceMatcher` and `unified_diff` over lists of lines, so the
spec diffs BeziSync shows are identical to those of the former Python implementation.
`internal/domain` uses it for pipeline spec diffs.

## Interface
- `[difflib.go:UnifiedDiff]` — `unified_diff(a, b, fromfile, tofile, n)` as a list of lines without terminators.
- Internal: `[difflib.go:newMatcher]` and the opcode/grouping helpers port `SequenceMatcher`.

## Invariants
- Behaviour matches Python byte for byte, including autojunk (in a `b` of 200+ lines, a line occurring more than `len(b)/100+1` times is ignored as a match anchor, `[difflib.go:newMatcher]`); `[difflib_test.go:TestUnifiedDiffMatchesPython]` compares against captured fixtures.

## Side Effects
- None.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/difflib/"`
