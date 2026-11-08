type: minor

### Added
- **Flink CDC switches columns at the saved position** - a version that changes the columns a Flink CDC pipeline writes (a step that adds, computes or removes a column, keeping deleted rows, a rollback) no longer needs a Start over: once the running job has stopped with a savepoint, BeziSync adds the new columns to the destination tables and the job resumes from exactly that position writing them, with nothing copied again and no change lost (ADR 0013). Positions saved before the switch restore with the new columns too. Flink CDC pipelines keep their configuration until their next deploy.
