type: patch

### Fixed
- **A source column added while a Flink CDC pipeline was stopped no longer fails it** - a deploy named the column before the resumed job had read the change that added it, and the job failed with a misleading "transform can't run" (hold mode, the default, and tables written column by column). BeziSync now holds such a column back and resumes; the next deploy writes it (ADR 0015).
