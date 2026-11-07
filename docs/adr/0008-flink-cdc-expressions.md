# 0008 — Flink CDC expressions: more column functions, and custom expressions BeziSync parses

- **Status:** Accepted
- **Date:** 2026-11-08
- **Scope:** a new pure package `backend/internal/expr` (lexer, parser, canonical and Flink renderers, type
  checker, function list); `backend/internal/domain` (`computed.go`, `expression.go`, `transforms.go`,
  findings); `backend/internal/compiler/flinktransforms.go`; `backend/internal/services/transforms.go`
  (`NeedsColumns`); `backend/internal/api` (`expressions:check`) and `openapi.json`;
  `backend/internal/checks/catalog.json`; `backend/testdata/computed_functions.json`;
  `frontend/src/features/pipelines/transforms` (`ExpressionInput`, `ComputedEditor`, `FilterEditor`,
  `model.ts`, `PartitionBy.tsx`); e2e `32-flink-cdc`; the guide; the threat model (row 66); the contracts of
  every touched directory. Needs the whole-value secret fill of the Flink CDC helper (#15): literals may hold `$`.

## Context

A Flink CDC pipeline writes rows; its transform rule per table has a projection and a filter in Flink CDC's
expression language (Calcite parses it, Janino compiles it to Java on each TaskManager). BeziSync never lets
user text reach that language: a computed column is one of nine typed functions and a filter is built from
typed rules. The maintainer asked for (4) more of Flink CDC's functions, including custom input, and (5)
custom transforms for users, safely.

Two reviews and a **corpus run on the real engine** shaped this design. The corpus runs the Flink CDC 3.7.0
image's own CLI in a mini cluster against a PostgreSQL table with typed columns and NULLs, through the
`values` sink, one rendering per job; every form below was run, and every rendering the code produces for the
47-expression set in `internal/expr` gave the expected value for every row. What it showed:

| Form on Flink CDC 3.7.0 | Result |
|---|---|
| `UPPER(s)`, `LOWER`, `TRIM`, `CHAR_LENGTH`, `SUBSTR` on a NULL | NullPointerException: the job crash-loops |
| `i + 1`, `d * i`, `i / z` with a NULL | NullPointerException |
| `CASE WHEN x IS NULL THEN NULL ELSE f(x) END` | correct, NULL stays NULL |
| `CAST(x AS STRING)` | doesn't compile (the job never starts); `VARCHAR` does |
| `b = 5` (BIGINT), `d = 2.5` (DECIMAL 2.50) | always FALSE (`=` is Java `equals`) |
| `TRY_CAST(… AS DOUBLE)` on both sides of `=` | correct |
| DECIMAL `*` INT, DECIMAL in `CASE`/`COALESCE`/`IF` with INT | doesn't compile |
| DECIMAL `/` DECIMAL | the fraction is dropped (2.50 / 2 = 0) |
| DOUBLE arithmetic | correct |
| `-x` (unary minus) | doesn't compile; `-5` (a literal) does |
| `LIKE 'a%'` on a NULL | NullPointerException; with `ESCAPE` it gives NULL |
| `TO_TIMESTAMP('abc', …)` | ParseException: crash-loop; `TO_DATE` and `TRY_CAST(… AS TIMESTAMP)` give NULL |
| `TRY_CAST(… AS DATE)` | doesn't compile |
| `DECIMAL(38, 10)` | narrowed to `DECIMAL(19, 10)` |
| `UPPER(\`now\`)` (a column named `now`) | compiled as the `NOW` function: doesn't compile |
| `TIMESTAMPTZ` > `TIMESTAMP` | RuntimeException on every row |
| `CONCAT`, `\|\|` with a NULL | the text "null" |
| `AND`, `OR`, `NOT`, comparisons, `COALESCE`, `IF`, `LEFT`, `REPLACE`, `DATE_FORMAT`, `YEAR`… with a NULL | safe |
| a filter on an update that stops (starts) matching | becomes a delete (an insert): the table holds exactly the matching rows |

So **the shipped typed functions crash-loop on a NULL** (upper, lower, trim, length, part of text), **"Join"
with a column and "As text" never started** (`CAST … AS STRING`), and **typed filters' equality with a
number matched nothing on BIGINT and DECIMAL columns**. The e2e journeys had no NULLs and never read the rows
back.

Other facts from the pinned 3.7.0 sources: string literals reach Java with `\` and `"` escaped
(`JaninoCompiler.translateSqlSqlLiteral`); `TransformSqlSyntaxRewriter.rewriteTryCast` treats `\` inside
quotes as an escape, which SQL doesn't; the parser always registers AI model functions
(`AiFunctionSqlOperatorTable`); `JaninoCompiler.translateSqlIdentifier` turns an identifier named like a time
function into that function, back-quoted or not.

The invariant: every existing spec compiles to the same `config_hash`, **except** the pipelines this fixes
(computed columns, and typed filters comparing a number for equality), whose output was wrong or never ran.

## Decision

### E1. One language, owned by `internal/expr`

- **Lexer and parser** for a whitelisted grammar (E2), producing a tree; every refusal carries a position.
- **`Canonical`** is what is stored and shown: keywords and functions in capitals, columns back-quoted
  (Flink CDC's metadata columns bare), one space around operators, the fewest parentheses that keep the
  tree, `||` always parenthesised against arithmetic. `Parse(Canonical(t))` is `t` (round-trip and fuzz
  tests).
- **`Flink`** is what compiles (E4), fully parenthesised; `FlinkChecked` refuses a rendering over 20,000
  characters.
- **`Check`** gives a tree's kind and whether it is deterministic, against the table's columns when known.
- **Functions** not in the list can't be called: UDFs, AI functions, regular expressions, struct functions.

### E2. The grammar

- **Values:** columns (bare or back-quoted, case-sensitive), the metadata columns, text in single quotes
  (`''` for a quote, at most 200 characters), whole and decimal numbers written out, `TRUE`, `FALSE`, `NULL`.
- **Operators:** `+ - * / %`, `||`, comparisons, `AND OR NOT`, `IS [NOT] NULL|TRUE|FALSE`,
  `IS [NOT] DISTINCT FROM`, `[NOT] IN`, `[NOT] BETWEEN`, `[NOT] LIKE 'pattern'`.
- **Forms:** `CASE`; `CAST`/`TRY_CAST` to `STRING`, `BOOLEAN`, `INT`, `BIGINT`, `DOUBLE`, `DECIMAL(p, s)`,
  `TIMESTAMP`.
- **Functions:** text `UPPER LOWER TRIM LTRIM RTRIM CHAR_LENGTH CONCAT CONCAT_WS SUBSTR LEFT RIGHT REPLACE
  LPAD RPAD LOCATE STARTSWITH ENDSWITH`; numbers `ABS CEIL FLOOR ROUND`; dates `DATE_FORMAT YEAR QUARTER
  MONTH WEEK DAYOFMONTH DAYOFWEEK DAYOFYEAR HOUR MINUTE SECOND TO_DATE DATE_ADD TIMESTAMPDIFF`, and the
  non-deterministic `NOW LOCALTIMESTAMP CURRENT_TIMESTAMP CURRENT_DATE UUID`; empty values
  `COALESCE IFNULL NULLIF IF`. Date patterns come from a fixed list.
- **Refused anywhere** (literals included): comments, `;`, `${`, `\`, double quotes, control characters,
  over 1,000 characters, nesting over 32, more than 200 parts; `CAST … AS DATE` (use `TO_DATE`),
  `TO_TIMESTAMP` (use `TRY_CAST(… AS TIMESTAMP)`), `SIMILAR TO`, a chosen `ESCAPE`.
- **Columns:** the table's source columns before renames (as computed columns read them); masked or left-out
  ones are refused; a column named like one of Flink CDC's time functions (`NOW`, `DATE_FORMAT`, …) can't be
  read inside an expression; a computed column can't read another.

### E3. Where it is used

- **Computed column:** `expression` instead of `function` and `args`. **Filter** (Flink CDC only):
  `expression` instead of operations and rules; typed, stage 0, may be limited to tables. Both dumped only
  when set; stored as the canonical text (decoding parses and re-renders).
- **Debezium** refuses both (`transform_unsupported`).
- **Policy:** no new key. The editors offer *An expression* in Expert mode (or when a step already has one);
  `allowed_transforms` limits computed and filter steps as before. Nothing in an expression can run code,
  call out, or fail a row (E4, E5), unlike Debezium's custom classes and scripts.

### E4. The Flink rendering is null-safe and total (the corpus)

- A sub-tree of strict functions and operators gets one guard:
  `CASE WHEN <its inputs> IS NULL [OR <divisor> = 0] THEN NULL ELSE … END`.
- `+ - * /` compute in `DOUBLE` (`TRY_CAST(… AS DOUBLE)`); `%` in `BIGINT`; division and remainder by zero
  give NULL; `-x` is `0 - x`.
- A comparison involving a number literal or a computed number compares both sides as `DOUBLE`.
- `CAST` renders as `TRY_CAST`, and `STRING` as `VARCHAR`; `LIKE` always carries `ESCAPE '!'`.
- **The typed functions compile through this rendering too** (`domain.ComputedTree`), and typed filters'
  equality and lists with numbers compare as `DOUBLE`.

### E5. Type and determinism rules (`Check`)

- Non-deterministic functions can't be in a filter; `__data_event_type__` can't be in a filter expression
  (an update's old row is `-U`, its new row `+U`).
- Two number columns compared for equality need `CAST(… AS DOUBLE)` on both sides; `CASE`, `COALESCE` and `IF`
  can't mix whole, decimal and floating-point values; `%` takes whole numbers; a timestamp with a time zone
  compares only with another.
- Arithmetic results are floating point. A computed column of unknown kind (an expression) can't be a
  partition source.

### E5b. More typed functions

Replace text, first / last characters, pad on the left / right, join with a separator, first that isn't
empty, absolute value, round up / down, round (digits 0–18), add / subtract / multiply / divide (a column or
a number), year, month, day of the month, hour, date from text (`TO_DATE` with a pattern), days between, as a
decimal number (scale 0–18), as a floating-point number. New argument fields `integer` and `decimal`
(dumped only when set). `KindFits(ArgTime)` takes `timestamptz` (verified). The Go list and the frontend's
`COMPUTED_FUNCTIONS` are held equal by `backend/testdata/computed_functions.json`.

### E6. The check endpoint

`POST /api/v1/projects/{project_id}/pipelines/{pipeline_id}/expressions:check`, `pipeline.view` (404 when
the caller can't see the pipeline), pure: `{use, table, expression, columns?}` →
`{canonical, kind, deterministic, columns, error}` with the error's offset and length in UTF-16 code units.
The editor checks 250 ms after typing stops and adopts the canonical text when the field is left.

### E7. Preflight

`transform_expression` (blocking): a column the table lacks (with a case hint), a kind a function doesn't
take, mixed kinds, a column named like a time function. `NeedsColumns` now includes computed steps and
expression filters, so preflight and the drift alert run it (a gap: computed steps were never checked there).

### Implementation plan (as built)

1. Pin today's Flink CDC transforms in the compile matrix.
2. `internal/expr`, its contract and tests (renderings, refusals, checker, fuzz, the corpus generator).
3. Domain and compiler: `expression`, the new functions and argument fields, `ComputedTree`, the null-safe
   typed functions and numeric filters (the bug fixes), `transform_expression`, `NeedsColumns`, vectors.
4. API `expressions:check`, OpenAPI, tests.
5. Frontend: the grouped function picker, *An expression* for computed columns and filters, `ExpressionInput`.
6. e2e and docs.

### Verification plan

- The corpus (above) on the Flink CDC 3.7.0 image, for every rendering rule.
- Go: `internal/expr` table, refusal, checker, round-trip and fuzz tests; domain decode/dump/findings;
  compiler goldens and matrix (only the `transforms` rows change); the endpoint's 404, validation, offsets.
- Vitest: the vectors, the new arguments, expression steps.
- e2e `32-flink-cdc`: a pipeline whose typed functions read NULLs, joins a number into text, divides by zero,
  filters a BIGINT id for equality, and has an expression column and filter typed in the wizard; the rows are
  read back from Iceberg.

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| X-1 | Existing spec without computed columns or numeric equality filters | Same `config_hash` | matrix |
| X-1b | Existing spec with a computed column or a numeric equality filter | New hash: restarts once (announced); the output now runs and is correct | matrix, e2e |
| X-2 | `'US$5'` in a literal | Reaches Flink CDC as written | unit (needs #15) |
| X-3 | `'it''s'`, Unicode in a literal | Round-trips | unit, corpus |
| X-4 | A literal with `\`, `${`, a line break, over 200 characters | Refused with the position | unit |
| X-5 | `;`, comments, `SELECT`, a UDF, `AI_*`, `REGEXP_*` | Refused | unit |
| X-6 | A NULL into any function or operator | NULL, never an exception | corpus, e2e |
| X-7 | Division and remainder by zero | NULL | corpus, e2e |
| X-8 | `NOW()` or `__data_event_type__` in a filter | Refused at save | unit |
| X-9 | A column named `now` read by a step | Refused (save for expressions, compile for typed steps) | unit |
| X-10 | Column name differing by case | Error with the hint | unit |
| X-11 | Column named like a keyword (`order`) | Back-quoted | unit |
| X-12 | 1,001 characters, depth 33, an exponential rendering | Refused | unit |
| X-13 | Text function on a number column; INT = BIGINT columns; DECIMAL with INT in `COALESCE`; `TIMESTAMPTZ` vs `TIMESTAMP` | Refused at preflight (or as typed when columns are known) | unit |
| X-14 | `LIKE 'a!%'` | `ESCAPE '!'` with `!!` | unit |
| X-15 | Debezium pipeline with an expression filter | 422 `transform_unsupported` | unit |
| X-16 | Expression filter limited to two tables | Preflight per table | unit |
| X-17 | Check endpoint, another project | 404 | api test |
| X-18 | Check endpoint, over 4,000 characters | 422 | api test |
| X-19 | Typed text not canonical | Editor adopts the canonical text on leaving the field | e2e |
| X-20 | Older BeziSync reading a spec with `expression` or a new function | Refuses to decode (rollback note) | changelog |

## Consequences

- Users write Flink CDC expressions; BeziSync owns the language, so nothing outside it reaches a job, and no
  value can stop the job.
- **Pipelines with computed columns or numeric equality filters restart once** on upgrade, and from then on
  run (many never started or would crash on the first NULL) and filter correctly.
- Arithmetic is floating point: exact decimals keep their column as it is, or round with *Round*.
- Times are read in the JobManager's time zone (UTC in the reference image).
- **Rollback:** an older BeziSync refuses a spec with `expression` or a new function; remove those steps first.
- **Not done:** regular expressions (backtracking can stall a TaskManager, and RE2 can't check Java syntax);
  UDFs and AI functions; struct and variant functions; expressions on Debezium; a computed column reading
  another; a sample value in the preview.

## Alternatives considered

- **Pass the user's text through, checked by a regular expression.** Rejected: only a parser can say what
  reaches Calcite.
- **Validate with Flink CDC itself on each keystroke.** Rejected: BeziSync has no JVM; the corpus does this
  once per rendering rule instead.
- **Type-dependent rendering (cast by the column's real type at compile).** Rejected: the compiler doesn't
  have column types, and a type change would silently change the compiled job; syntactic rules plus the
  checker (which has the types) are enough, as the corpus shows.
- **Exact DECIMAL arithmetic.** Rejected for 3.7.0: mixed DECIMAL and INT doesn't compile, and DECIMAL
  division drops the fraction.
- **A new `allow_expressions` policy, off by default.** Rejected: an expression can't run code, call out,
  fail a row or break deletes; `allowed_transforms` limits the steps, and the editor keeps it to Experts.
