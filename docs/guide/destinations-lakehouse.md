# Lakehouse tables

These destinations keep one lakehouse table per source table: Apache Iceberg (Debezium or Flink
CDC), Apache Paimon and Apache Fluss (Flink CDC). See [Destinations](destinations.md) for what
every destination shares.

Each section below opens with the same rows, so you can compare destinations. "Reading it back"
means the pipeline's **Events** tab and the [delivery canary](operate.md#delivery-canary). "Drop
destinations" is the choice made when you [delete a pipeline](operate.md#delete-a-pipeline).

## Apache Iceberg

Changes land in Iceberg tables, one per source table, through an Iceberg REST catalog or AWS Glue.
Snowflake, BigQuery (BigLake), Databricks, Trino, StarRocks and ClickHouse read the tables through
the same catalogs.

| | Apache Iceberg |
|---|---|
| Engine | Debezium (default) or Flink CDC (see [On Flink CDC](#on-flink-cdc) below) |
| What you get | One table per source table, created by its first change, upserted by key or appended as a change log |
| Connection | A catalog preset (see [Catalogs](#catalogs)), the namespace, and storage credentials: the connection's object-storage keys, the runtime's IAM role, or credentials the catalog vends |
| Pipeline settings | Destination step: how rows are written and how often to commit. Transform step: key columns and **Partition by** |
| Preflight checks | Signs in to the catalog and checks the namespace, then checks keys and tables that already exist. It creates no table |
| Reading it back | The Events tab and the delivery canary read the newest rows of each table |
| Deleting a pipeline | "Drop destinations" drops its tables, purging files where the catalog allows (RustFS keeps them) |
| Limits | New source columns are added; other schema changes stop the pipeline. Outbox steps and tombstone deletes are not available |

### Catalogs

| Preset | Notes |
|---|---|
| **Iceberg REST catalog** | Polaris, Lakekeeper, Gravitino, Nessie |
| **Amazon S3 Tables** | SigV4, service `s3tables`; warehouse = table bucket ARN |
| **AWS Glue Data Catalog** | Warehouse = an `s3://` location |
| **Snowflake Open Catalog** | OAuth2, scope `PRINCIPAL_ROLE:ALL`, vended credentials |
| **Databricks Unity Catalog** | Token, vended credentials |
| **RustFS S3 Tables** | Preview: SigV4 service `s3`, warehouse = the table bucket, path-style S3. Change logs only, as it refuses the first upsert commit of a new table |

Data files are written with the object-storage keys of the connection, the runtime's IAM role, or
credentials the catalog vends. With the connection's keys or the role, BeziSync asks the catalog
for no credentials. So a catalog that can't vend them (Polaris over storage without STS, for
example) works too.

### Tables and rows

Tables are created by their first change in the connection's namespace. BeziSync creates the
namespace on deploy. Each table gets:

- the columns from the source;
- the primary key, or the key columns chosen in the Transform step, as row identity;
- lower-case names with dots as `_` (naming `{schema}_{table}`).

Rows (Destination step):

| Choice | What the table holds |
|---|---|
| *latest row, deletes kept with `__deleted = true`* (default) | The latest row; deleted rows stay, marked |
| *latest row, deletes removed* | The latest row; deleted rows are removed |
| *every change appended* | A change log |

Tables without a key are always appended; a preflight warning says which.

### Commits

Commits happen at least every minute by default. Choose 5 or 15 minutes for fewer, larger files,
or "as soon as possible" to commit every batch. Health counts that interval as expected delay, not
lag.

### Partitioning

**Partition by**, in the Transform step, works on both engines. It is its own row, after the key,
so the key, renames and computed columns chosen on the same screen are offered as they change. It
gives a table up to four fields:

| Field | Meaning |
|---|---|
| A column | Each value |
| Year, month, day or hour | Of a date or timestamp (one per column) |
| `bucket[N]` | Buckets of a hash, N from 1 to 1024 |
| `truncate[N]` | The leading part, N up to 1,000,000 |

The columns offered are the ones the table writes (after renames, added and computed columns).
The transforms offered are the ones Iceberg allows for the type the engine writes: no buckets of
a true/false or floating-point value, no truncation of a date.

Partitioning is applied when the table is created. A table that already exists keeps its
partitioning, so preflight blocks asking it for other partitioning (`iceberg_partition_existing`).
Keep its own, or drop the table with another engine on the catalog and Start over so the pipeline
creates it.

**Upserted tables are partitioned only by key columns** (`iceberg_partition_key`):

- Rows are upserted by writing deletes on the key, and Iceberg applies a delete only to rows in
  its own partition. Partitioned by a column outside the key (an `updated_at` day, a `status`), an
  update that changes it leaves the old row behind. The editor shows such columns disabled.
- For time-like layout on an upserted table, partition by the leading part of an increasing key
  (`truncate[100000](id)`).
- Change logs (*every change appended*) take any column, including Debezium's *time of the
  change* (`__source_ts_ms`).
- A table that already exists is checked against its own identifier fields. One partitioned off
  them may already hold duplicate rows
  (`SELECT <key>, count(*) … GROUP BY <key> HAVING count(*) > 1`); the fix is Start over.

**On Debezium**, a table that Partition by doesn't list keeps the automatic partitioning:

- Upserted tables are bucketed by their first key column (`bucket(id, 16)`; 4 or 64 buckets in
  the Destination step). Change logs are partitioned by the day of the change.
- The wizard records each table's key column in `sink.options.partitions` (set it there through
  YAML or the API). Expert mode can turn the automatic partitioning off; Partition by still
  applies.
- Tables routed to one destination share its partitioning: a listed table's fields (tables that
  list fields must list the same), else the automatic one when they agree, else none.
- A field the table doesn't have stops the pipeline when the table is created, so preflight
  checks every field.
- MongoDB collections can't be listed.

**Key columns on Debezium** stay under their own names: a key column renamed or left out drops
out of the table's identity (`iceberg_key_columns`).

### Event shape

Iceberg needs the row after change with `__op` and `__source_ts_ms`. BeziSync adds that step (or
completes yours) and sends timestamps as ISO-8601 text (Kafka Connect types for SQL Server).
Outbox steps and tombstone deletes are not available.

### What preflight can't check

Preflight creates no table, so whether the credentials may create tables and commit is "not
checked": the catalog answers that only by trying. The first commit shows it.

### Image

Debezium pipelines run `ghcr.io/memiiso/debezium-server-iceberg` (see
[Engine versions](engine-versions.md)), amd64 only. It serves health on port 9000, which BeziSync
probes. Agent runtimes need an agent of this BeziSync version or newer; preflight says so.

### On Flink CDC

Flink CDC is chosen in the wizard when a Flink CDC image is registered; Debezium stays the
default. It uses the same connections and tables, written by Flink CDC's Iceberg sink.

- Rows are upserted by key. Deleted rows are removed, or kept and marked `__op = '-D'`.
- *every change appended* is shown unavailable with the reason: Flink CDC's Iceberg writer always
  writes rows by the table's key.
- An Iceberg table created beforehand without a key is refused (`iceberg_flink_keyless_table`),
  because Flink CDC 3.7.0 would write deletes into it as rows.
- New columns are added.
- Tables are unpartitioned unless **Partition by** (see [Partitioning](#partitioning)) lists them,
  compiled into Flink CDC's `partition.key`. There is no automatic partitioning and no time of the
  change (`__source_ts_ms`).
- The namespace must be one level of letters, digits and underscores.
- A Glue catalog signs in with the runtime's AWS role (no access keys).
- Catalog and storage credentials travel to the cluster as catalog properties, filled in by
  BeziSync's helper inside the pipeline's containers.

## Apache Paimon

One primary-key Paimon table per source table, in a filesystem catalog on S3 or an S3-compatible
store (MinIO, RustFS, Ceph).

| | Apache Paimon |
|---|---|
| Engine | Flink CDC |
| What you get | One primary-key table per source table. Rows are written on each Flink checkpoint |
| Connection | The warehouse (`s3://bucket/path`), the database, the endpoint and path-style access for S3-compatible stores, and an access key (or the runtime's own credentials) |
| Preflight checks | Writes and deletes a test object under the warehouse. Blocks a destination that already exists without a primary key (`paimon_keyless_table`: an append table can't take updates and deletes) |
| Reading it back | Nothing: BeziSync has no Paimon reader, so the Events tab has nothing to show and health stays Unknown. Read the tables with Flink SQL, Spark or Trino |
| Deleting a pipeline | Leaves its tables in place; the report says how to drop them |
| Limits | Tables are not partitioned |

## Apache Fluss

One primary-key table per source table in a Fluss database. Fluss can tier those tables into a
lakehouse.

| | Apache Fluss |
|---|---|
| Engine | Flink CDC |
| What you get | One primary-key table per source table, created and evolved by Flink CDC |
| Connection | The coordinators' `host:port` list and sign-in (none, or SASL PLAIN) |
| Preflight checks | That a coordinator can be reached (BeziSync has no Fluss client) |
| Reading it back | Nothing: the Events tab has nothing to show. Read the tables with Flink SQL or your own client |
| Deleting a pipeline | Leaves its tables in place; the report lists them as kept |
| Limits | Fluss documents no TLS for Flink CDC: keep it in a network you trust |

---

[All documentation](../README.md)
