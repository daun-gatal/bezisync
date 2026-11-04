type: minor

### Added
- **Flink CDC engine** - pipelines into Iceberg, StarRocks, Doris and Fluss can run on Flink CDC (a JobManager and TaskManagers per pipeline on Docker or Kubernetes), with parallel first copies, schema changes applied to the destination tables and saved positions kept by BeziSync. Debezium stays the engine for every other destination; each pipeline shows its engine.
- **StarRocks, Apache Doris and Apache Fluss destinations** - on Flink CDC: one primary-key table per source table, created and kept in step with the source. StarRocks and Doris are checked, read back in the Events tab and dropped on delete, and get no source column defaults (they refuse them); Fluss is checked for reachability and its tables are kept on delete. None of the three loads over TLS: keep them on a private network.
- **Iceberg on Flink CDC** - Iceberg pipelines from MySQL, MariaDB, PostgreSQL or SQL Server can choose Flink CDC in the wizard (Debezium stays the default) over the same REST and Glue catalog connections.
- **Engine versions** - Settings → Debezium versions is now Settings → Engine versions, with a Flink CDC tab. Flink CDC stays off until an Admin registers an image they build (BeziSync publishes none; the guide lists what it holds). MySQL/MariaDB sources need MySQL Connector/J added to it.
- **Saved positions** - a Flink CDC pipeline's Positions tab lists its saved positions (encrypted in the state database) and, while paused, restores one or starts over: copy every table again, read from now, or read since a time (MySQL, SQL Server).
- **Flink CDC from scripts and agents** - manifests take `spec.engine`, `flink_cdc` settings and `flinkCdcImage`; `bezisync remote pipelines positions|restore|start-over` and the ENGINE column in `remote get`; MCP tools `list_engines`, `list_saved_positions`, `restore_saved_position`, and `from_time` on `reset_pipeline_offsets`.

### Changed
- **Helm charts 0.2.0** - the control-plane and agent charts' Roles allow Services, which a Flink CDC pipeline's TaskManagers use to reach its JobManager on Kubernetes.
