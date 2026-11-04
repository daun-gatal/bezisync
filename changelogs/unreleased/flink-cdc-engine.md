type: minor

### Added
- **Flink CDC engine** - pipelines into Iceberg, StarRocks, Doris and Fluss can run on Flink CDC (a JobManager and TaskManagers per pipeline on Docker or Kubernetes), with parallel first copies, schema changes applied to the destination tables and saved positions kept by BeziSync. Debezium stays the engine for every other destination; each pipeline shows its engine.
