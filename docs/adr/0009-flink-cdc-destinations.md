# 0009 — More Flink CDC destinations: Kafka, Elasticsearch, Apache Paimon and OceanBase

- **Status:** Accepted (D2's Elasticsearch test index amended by [0016](0016-read-only-destination-checks.md))
- **Date:** 2026-11-08
- **Scope:** `backend/internal/connectors` (`kafka_flink.go`, `elasticsearch.go`, `paimon.go`, `oceanbase.go`,
  `olap.go`, `kafka.go`, `descriptors.json`), `backend/internal/engine` (which engine writes what),
  `backend/internal/checks/catalog.json` and `docs/reference/checks.md`, compile matrix rows, e2e fixtures
  (`49-flink-sinks.yaml`) and `34-flink-sinks.spec.ts`, the guide, the threat model, the connectors and engine
  contracts; the Flink CDC image (`daun-gatal/image-repo`, `flink-cdc/Dockerfile`).

## Context

Flink CDC 3.7.0 ships pipeline sinks for Doris, Elasticsearch, Fluss, Hudi, Iceberg, Kafka, MaxCompute,
OceanBase, Paimon and StarRocks. BeziSync's Flink CDC engine wrote Iceberg, StarRocks, Doris and Fluss.
The maintainer asked for the others.

Each candidate was run first with the Flink CDC 3.7.0 image's own CLI (mini cluster, the class loading
BeziSync's helper uses: connectors on Flink's classpath, `parent-first`) from PostgreSQL into a real server:

| Sink | Result |
|---|---|
| Kafka (`apache/kafka`) | Works. The topic is the route's sink table, created if missing; key `{"TableId":…, <key>}` JSON; value `debezium-json` (before/after/op, a short `source`, no schema, no `ts_ms`); `partition.strategy` defaults to `all-to-zero`. |
| Elasticsearch 8 (security on, HTTP) | Works with `parent-first` loading (child-first fails: the connector's Jackson clashes). Index = sink table, created by its first document; `_id` = the key; NULL fields omitted; decimals as text. Basic auth only, no CA option. |
| Paimon 2.0.0 on S3 (RustFS) | Works with `paimon-s3` 2.0.0 and Hadoop's client classes on the classpath (`ClassNotFoundException: org.apache.hadoop.conf.Configuration` without them). Snapshots commit on checkpoints. |
| OceanBase CE 4.3.5 | Works with OceanBase Connector/J 2.4.14 (LGPL-2.1; the connector bundles no driver, its default is GPL MySQL Connector/J). Timestamps shift by the tenant's `+08:00` unless the URL sets `serverTimezone=UTC`. |
| Hudi | Not run: its only documented way to pass S3 credentials is `table.properties.*` (persisted in table metadata) or AWS keys in the TaskManagers' environment, which BeziSync's threat model forbids; it would also add a 310 MB AWS SDK v1 bundle. |
| MaxCompute | Not run: an Alibaba Cloud service with no emulator; no check could be shown to work. |

## Decision

### D1. Kafka on Flink CDC (both engines write Kafka; Debezium stays the default)
- The same Kafka connection; `partition.strategy: hash-by-key` (never all to partition 0), `key.format: json`,
  `value.format: debezium-json`; producer security from `connectors.KafkaClientProps` as whole-value
  placeholders. The topic is the routed name, created at deploy by `PrepareSink` with the connection's
  partitions and replication factor, as on Debezium.
- Only JSON without the schema (Flink CDC has no other format): anything else is refused at compile.
- The record differs from Debezium's (no schema, no `ts_ms`, the key carries `TableId`): the guide says so.
  The Transform step's event shape (Row after change, delete handling, added fields) is hidden on Flink CDC
  for every destination; for Kafka that means records are always the envelope, which the guide says too.
- `KafkaClientProps` now escapes `\` as well as `"` in the JAAS line (a backslash in a password broke it).
  The JAAS line is part of the hashed environment, so the one exception to S-1: a running Debezium Kafka
  sink or Spanner source whose Kafka user or password holds a `\` gets a new `config_hash` and restarts
  once (it couldn't sign in with the old line).

### D2. Elasticsearch, Apache Paimon and OceanBase (Flink CDC only)
- **Elasticsearch:** hosts, version (6/7/8), optional user. The index is the routed name with dots as `_`,
  lower-cased (the naming rule already checks names that way; it must start with a letter or digit). Checks:
  reachability, sign-in and the server's major version (must match), and a test document in a new index
  (automatic index creation and the privileges; a test index that can't be deleted warns, since "drop
  destinations" would fail the same way). Events read `_search`; "drop destinations" deletes the
  index. https only with a publicly trusted certificate (the sink takes no CA): said in the form and checks.
- **Apache Paimon:** a filesystem catalog on S3 or an S3-compatible store: warehouse `s3://bucket/path`,
  database, endpoint, path-style, keys (or none: the runtime's role, and the check uses the control plane's
  own AWS credentials, as Iceberg's checks do). Credentials travel as catalog properties of the job (placeholders the
  helper fills), as for Iceberg on Flink CDC. The check writes and deletes a test object under the warehouse,
  through the SSRF guard. BeziSync has no Paimon reader: Events stay empty and health is Unknown; "drop
  destinations" retains the tables and says how to drop them (as for Fluss). Tables are unpartitioned. A
  destination that already exists without a primary key is an append table, which can't take updates and
  deletes: preflight reads the table's current schema file from the warehouse and blocks it
  (`paimon_keyless_table`, as `iceberg_flink_keyless_table` does for Iceberg).
- **OceanBase (MySQL tenants):** host, port, database, user (`user@tenant`), password; the JDBC URL is built
  from these (the host must be a host name or IP address, so it can't carry `/`, `?` or driver options; the
  same rule now holds for StarRocks and Doris, whose URLs are built the same way) and BeziSync's own parameters (`characterEncoding=UTF-8&serverTimezone=UTC`), never user JDBC
  options. Checks, Events and "drop destinations" reuse the MySQL-protocol code of StarRocks and Doris
  (`olapSink`, with no HTTP port and MySQL's `DROP TABLE`).

### D3. The image
The Flink CDC image adds `kafka.jar`, `elasticsearch.jar`, `paimon.jar` + `paimon-s3.jar`, `oceanbase.jar` +
`oceanbase-client.jar`, pinned by SHA-256 (image-repo #19). A pipeline lists its sink's extra JARs
(`FlinkBlock.Extra`), and the helper refuses to start on an image that lacks one, naming it.

### Implementation plan (as built)
1. The connectors, descriptors, engine lists, catalog entries and unit tests; compile matrix rows (new only).
2. e2e fixtures (Elasticsearch 8, OceanBase CE) and `34-flink-sinks`: for each sink, the connection's checks,
   a copy, inserts, updates and deletes read back (Paimon: a saved position), and the delete.
3. Docs, threat model, contracts, changelog; the image PR.

### Verification plan
- Unit: each sink's Flink block (options, placeholders, extras), configuration refusals, destinations,
  cleanup outcome, JAAS escaping, engine choices.
- `TestCompileMatrixUnchanged`: only new rows.
- e2e `34-flink-sinks` with the image built from the image-repo change, and `32-flink-cdc` (unchanged
  destinations still run on it).

**Edge-case register**

| ID | Case | Expected | Test |
|---|---|---|---|
| S-1 | Existing pipelines (any engine) | Same `config_hash` | matrix |
| S-2 | Kafka on Flink CDC with Avro or the schema | 422 `engine_settings` | unit |
| S-3 | Kafka password with `"` or `\` | JAAS escapes both | unit |
| S-4 | Elasticsearch host without a scheme | Refused at save | unit |
| S-5 | Elasticsearch version 7 set, server 8 | `elasticsearch_version` blocks | check |
| S-6 | Elasticsearch user without a password | Refused | unit |
| S-7 | Two tables whose names differ only by case (Elasticsearch, lower-cased) | `destination_collision` at compile | compiler (existing rule) |
| S-8 | Paimon warehouse not on S3 | Refused at save | unit |
| S-9 | OceanBase database name with punctuation | Refused at save | unit |
| S-10 | OceanBase timestamps | Written in UTC (no +08:00 shift) | spike, e2e |
| S-11 | An image without the new JARs | The helper refuses to start, naming the connector | helper (existing rule) |
| S-12 | Debezium asked for Elasticsearch, Paimon or OceanBase | 422 `engine_unsupported` | unit, API |
| S-13 | Paimon "drop destinations" | Tables retained, the statement shown | unit, e2e |
| S-14 | A host like `h/x?allowLoadLocalInfile=true&a=` (OceanBase, StarRocks, Doris) | Refused at save and compile | unit |
| S-15 | A Paimon destination that exists without a primary key | `paimon_keyless_table` blocks | unit |
| S-16 | Paimon without an access key | The runtime's credentials; no placeholders | unit |
| S-17 | Kafka password with `\` on a running Debezium pipeline | New `config_hash`, one restart (S-1 exception) | unit |
| S-18 | Elasticsearch routed name with dots | Index with `_` (matches the naming rule) | unit |
| S-19 | "Drop destinations" on each new sink | Removed (Kafka, Elasticsearch, OceanBase), retained (Paimon) | e2e |

## Consequences
- Four more destinations on Flink CDC; Kafka can run on either engine, Debezium remaining its default.
- Paimon pipelines have no read-back evidence (Unknown health), like Fluss.
- The Flink CDC image grows by about 160 MB.
- **Not done:** Hudi and MaxCompute (above); Paimon partitioning and REST/Hive catalogs; Elasticsearch API
  keys, a CA, index mappings; Kafka's canal-json, a single topic, or Debezium's envelope on Flink CDC.

## Alternatives considered
- **Kafka on Flink CDC as Flink-only, or not at all.** Rejected: a Flink CDC pipeline (Oracle through Flink
  CDC, expressions) may need Kafka; Debezium stays the default for Kafka, so nothing changes for existing users.
- **OceanBase through Debezium's JDBC sink (MySQL preset).** Not chosen here: the maintainer asked for Flink
  CDC's sinks; Debezium's path is unchanged and can be tried separately.
- **MySQL Connector/J for OceanBase.** Rejected: GPL; OceanBase's own driver is LGPL and works.
- **Hudi with credentials in table properties or the TaskManagers' environment.** Rejected: secrets would be
  persisted in table metadata or exposed to every TaskManager process.
