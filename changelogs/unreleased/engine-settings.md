type: minor

### Added
- **Typed engine settings** - a pipeline spec takes `tuning`: settings from BeziSync's registry, each checked against the pipeline's engine, source and destination and its bounds, and carried by YAML, the API and MCP. The first ones: snapshot threads and the poll interval (Debezium), the connection pool (Flink CDC). `GET /engines` lists every setting with what changing it does to a running pipeline, and preflight and the version diff list the settings a deploy changes with that impact (`settings_change`).
- **A project policy for advanced Debezium properties** - Admins decide whether pipelines may pass raw properties (Settings → Policies, *Allow advanced properties*); without it their deploy is blocked. Upgrading turns it on for every project that already has a pipeline using them, with an audit entry.

### Changed
- **Unknown checks** - preflight checks BeziSync can't verify are now *Unknown*: grey, never shown as passed. Advanced properties that are allowed show as one, listing each key.

### Fixed
- **Advanced Debezium properties can't override BeziSync** - a raw property may no longer change a key BeziSync sets for the pipeline (where events go, tables, columns and personal-data choices, the error policy, connection and TLS details, heartbeats and signals). Credentials, values containing `${…}` or line breaks, and unsafe options (SQL on the source, dropping the slot, skipping changes, weaker TLS, loading classes, `quarkus.*`) are refused too. Saving refuses new ones with where to set them instead; ones an older pipeline already has still save, and its deploy is blocked by the new *Advanced properties* check (a forced deploy can't skip it). Adopting a Debezium Server keeps only allowed properties.
- **JDBC driver options that can read files or load code are refused** - advanced `driver.*` and `database.*` properties that read local files, deserialize or load classes, call out, log to files, or change TLS and sign-in (for example MySQL's `allowLoadLocalInfile`) are no longer passed to the source's driver.
- **Flink CDC on PostgreSQL checks replication slots for the copy** - during a first copy each reader opens its own temporary replication slot and WAL sender, so it needs 1 + readers of each; preflight only checked for one free slot, so a large pipeline could pass it and then stop part-way. The new check blocks such a deploy, with the SQL to fix it.
- **Credential-looking advanced properties are hidden** - a pipeline saved before credentials were refused as advanced properties may still hold one; preflight, the Config view and change summaries now show its value as `(hidden)` instead of in plain text (the pipeline keeps working).
