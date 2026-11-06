type: minor

### Fixed
- **Advanced Debezium properties can't override BeziSync** - a raw property may no longer change a key BeziSync sets for the pipeline (where events go, tables, columns and personal-data choices, the error policy, connection and TLS details, heartbeats and signals). Credentials, values containing `${…}` or line breaks, and unsafe options (SQL on the source, dropping the slot, skipping changes, weaker TLS, loading classes, `quarkus.*`) are refused too. Saving refuses new ones with where to set them instead; ones an older pipeline already has still save, and its deploy is blocked by the new *Advanced properties* check (a forced deploy can't skip it). Adopting a Debezium Server keeps only allowed properties.
