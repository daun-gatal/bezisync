type: minor

### Added
- **A project policy for advanced Debezium properties** - Admins decide whether pipelines may pass raw properties (Settings → Policies, *Allow advanced properties*); without it their deploy is blocked. Upgrading turns it on for every project that already has a pipeline using them, with an audit entry.

### Changed
- **Unknown checks** - preflight checks BeziSync can't verify are now *Unknown*: grey, never shown as passed. Advanced properties that are allowed show as one, listing each key.

### Fixed
- **Advanced Debezium properties can't override BeziSync** - a raw property may no longer change a key BeziSync sets for the pipeline (where events go, tables, columns and personal-data choices, the error policy, connection and TLS details, heartbeats and signals). Credentials, values containing `${…}` or line breaks, and unsafe options (SQL on the source, dropping the slot, skipping changes, weaker TLS, loading classes, `quarkus.*`) are refused too. Saving refuses new ones with where to set them instead; ones an older pipeline already has still save, and its deploy is blocked by the new *Advanced properties* check (a forced deploy can't skip it). Adopting a Debezium Server keeps only allowed properties.
- **JDBC driver options that can read files or load code are refused** - advanced `driver.*` and `database.*` properties that read local files, deserialize or load classes, call out, log to files, or change TLS and sign-in (for example MySQL's `allowLoadLocalInfile`) are no longer passed to the source's driver.
