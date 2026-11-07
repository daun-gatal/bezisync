type: minor

### Added
- **More functions for Flink CDC computed columns** - replace text, first and last characters, pad, join with a separator, first that isn't empty, absolute value, round (up, down, to digits), add, subtract, multiply, divide, year, month, day of the month, hour, date from text, days between, and as a decimal or floating-point number, picked by group (ADR 0008).
- **Flink CDC expressions** - in Expert mode a computed column, or a filter's condition, can be written as an expression in Flink CDC's functions (`CASE WHEN total > 100 THEN 'big' ELSE 'small' END`), checked as you type (`POST …/expressions:check`). BeziSync parses it and writes its own rendering into the job: only documented functions (no user-defined, AI or regular-expression functions), no comments or `;`, and every function NULL-safe so no value can stop the job. Preflight's `transform_expression` checks the columns and their kinds.

### Fixed
- **Flink CDC computed columns no longer stop on an empty value** - upper/lower case, trim, length and part of text threw on a NULL, which stopped the job on the first such row; they now give an empty value.
- **Flink CDC "Join" with a column and "As text" run** - they compiled to `CAST(… AS STRING)`, which Flink CDC 3.7.0 doesn't accept, so the pipeline never started.
- **Flink CDC filters compare numbers by value** - "is", "is not", "is one of" with a number matched nothing on a BIGINT or DECIMAL column (Flink CDC compares Java objects). They now compare as numbers.
- **Preflight and the schema-drift alert check computed columns** - a computed column whose input left the table was only caught when the job failed.

### Changed
- **Pipelines with Flink CDC computed columns, or a filter comparing a number for equality, restart once on upgrade** to run the fixed rendering. Rolling back: an older BeziSync refuses a spec with an expression or one of the new functions; remove those steps first.
