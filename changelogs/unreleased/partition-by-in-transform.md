type: minor

### Changed
- **Partition by is chosen in the Transform step** - Iceberg's Partition by now has its own row in Transform, after the key, so the key, renames and computed columns chosen on the same screen are offered as they change; the Destination step says where it went and warns when a write-mode or engine change made a saved field invalid, and so does Review. Nothing is stored differently, and no pipeline restarts (ADR 0007).
- **Flink CDC shows why Iceberg has no change log** - "Every change appended" is shown unavailable on Flink CDC with the reason (its Iceberg writer always writes rows by the table's key), naming Debezium when it is a real choice. A pipeline switched to Flink CDC with that choice must pick another before continuing.

### Fixed
- **Wizard steps no longer save an unchanged pipeline** - with several Iceberg tables, the Destination and Review steps saved the recorded table keys in another order on every visit, and Transform and Tables saved when the spec carried value formats or keys next to Flink CDC settings.
- **Unticking a partitioned table** - its Partition by and recorded key stayed in the spec, and the server refused the save.
- **Preflight fixes for Iceberg partitioning** point at the Transform step, and RustFS's upsert check no longer tells Flink CDC pipelines to choose a change log.
