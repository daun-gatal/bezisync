type: minor

### Added
- **More Flink CDC destinations: Elasticsearch, Apache Paimon and OceanBase, and Kafka on Flink CDC** - Elasticsearch (6, 7 or 8; one index per table, a document per row keyed by the primary key), Apache Paimon (primary-key tables in a filesystem catalog on S3 or an S3-compatible store) and OceanBase (MySQL tenants, through OceanBase Connector/J) run on Flink CDC, with connection checks that write and remove a test index, object or table. Kafka can now also run on Flink CDC (JSON without the schema, keyed by the primary key; Debezium stays its default), so a pipeline that needs Flink CDC can write Kafka. They need a Flink CDC image with the `kafka`, `elasticsearch`, `paimon` + `paimon-s3` and `oceanbase` + `oceanbase-client` JARs (the guide lists them; the reference image adds them). Paimon has no read-back: Events stay empty and health is Unknown. Hudi and MaxCompute aren't offered (ADR 0009).

### Fixed
- **Kafka SASL passwords with a backslash** - the JAAS line escaped only `"`; a `\` in a password broke sign-in.
