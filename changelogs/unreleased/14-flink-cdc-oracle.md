type: minor

### Added
- **Flink CDC reads Oracle** - Oracle tables can be synced into StarRocks, Doris and Fluss, and into Iceberg on Flink CDC (Debezium stays Iceberg's default), with parallel first copies, added columns applied and restorable positions. It needs a Flink CDC image with the `oracle` connector (`/opt/flink-cdc/connectors/oracle.jar`; the reference image adds it). Flink CDC's Oracle reader is narrower than Debezium's, and preflight says so before a deploy: Enterprise, Standard or Express Edition up to 23ai only (`flink_oracle_version`; not Oracle Database Free or 26ai), `ANALYZE ANY` for the first copy, which replaces the tables' optimizer statistics (`flink_oracle_analyze`), and, on a multitenant database, a `LOG_MINING_FLUSH` table in the CDB root that BeziSync creates (`flink_oracle_flush_table`). Tables must have upper-case names; renaming or dropping a column stops the pipeline (Start over), and adding tables needs a Start over first (`flink_tables_added`). MongoDB, Vitess, TiDB and OceanBase stay on Debezium: Flink CDC has no pipeline source for them (ADR 0006).

### Changed
- **Raw Debezium 1.9 options that load code or open connections are refused on Flink CDC** - Flink CDC embeds Debezium 1.9, whose `source.debezium.database.history*`, `source.debezium.rac.nodes` and `source.debezium.database.connection.adapter` could load a class, send the schema history elsewhere or hand the source's credentials to other hosts. A pipeline that set one fails preflight until it is removed.

### Fixed
- **Flink CDC pipelines no longer run the delivery canary** - Flink CDC captures only the pipeline's tables, so a canary row never arrived; the canary is now off for them and preflight says why.
