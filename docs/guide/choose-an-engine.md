# Choose an engine

Each pipeline runs on one of two engines, Debezium or Flink CDC. You pick it in the wizard's
Destination step, and it is fixed at the pipeline's first deploy.

## The two engines

| Engine | How it runs | What it reads and writes | Availability |
|---|---|---|---|
| **Debezium** | Each pipeline is its own Debezium Server | Every source; every destination except the six that only Flink CDC writes | The default, always available |
| **Flink CDC** | Each pipeline is its own small Flink cluster (see below) | MySQL/MariaDB, PostgreSQL, SQL Server and Oracle, kept in sync in lakehouse, analytics and search stores and in Kafka | Once an Admin registers a Flink CDC image (see [Flink CDC engine](flink-cdc-engine.md)) |

A Flink CDC cluster has a *JobManager* that coordinates, and *TaskManagers* whose *readers* copy
and stream the tables. Until an Admin registers a Flink CDC image, the wizard shows its
destinations as unavailable, with the reason.

**Policies → Allowed engines** can limit the engines a project's pipelines use.

## Which engine writes each destination

| To… | Destinations | Engine |
|---|---|---|
| Stream changes to services as events | Kafka | Debezium (default) or Flink CDC |
| | Pub/Sub, Kinesis, Amazon SQS and SNS, Azure Event Hubs, Apache Pulsar, Redis Streams, NATS JetStream, HTTP | Debezium |
| Keep a copy of tables in another database | PostgreSQL, MySQL, MariaDB, SQL Server (JDBC) | Debezium |
| | OceanBase | Flink CDC |
| Keep lakehouse tables | Apache Iceberg | Debezium (default) or Flink CDC |
| | Apache Paimon, Apache Fluss | Flink CDC |
| Query fresh rows in an analytics database | StarRocks, Apache Doris | Flink CDC |
| Search the rows | Elasticsearch | Flink CDC |

MongoDB, CockroachDB, Vitess and Google Cloud Spanner are read by Debezium only, so their
pipelines write the Debezium destinations. See [Sources](sources.md) for what each source does on
each engine.

## When the destination takes both (Iceberg, Kafka)

Choose **Flink CDC** for:

- large tables whose first copy should run in parallel;
- source schema changes applied to the destination tables;
- restoring an earlier saved position.

Choose **Debezium** for:

- an Iceberg change log (*every change appended*);
- Iceberg's automatic partitioning and the time of the change (`__source_ts_ms`);
- Kafka records in Debezium's formats and event shapes. On Flink CDC they are always JSON without
  the schema, in its own change envelope: see [Kafka on Flink CDC](destinations-events.md#kafka-on-flink-cdc);
- masked columns;
- copying one table again while the others stream.

## Compare the engines

| | Debezium | Flink CDC |
|---|---|---|
| Runs as | One Debezium Server container | A JobManager and one or more TaskManagers |
| Smallest size | Small: 0.5 CPU, 768 MiB | Small: one TaskManager (1 CPU, 2 GiB, 2 readers) and a JobManager (1 to 1.5 GiB) |
| First copy | Debezium's snapshot | Split into chunks, copied by every reader at once (2, 4 or 8 by size, or a custom cluster) |
| Copy one table again while streaming | Yes, on the Snapshots tab (most sources) | No: Start over copies every table |
| Source schema changes | New columns added (Iceberg, JDBC); others depend on the destination | Applied to the destination tables, ignored, or stop the pipeline, as you choose |
| Where it resumes from | Offsets kept in BeziSync's PostgreSQL, which you can view and edit | Saved positions (Positions tab): restore one, or start over from scratch, from now or (MySQL, SQL Server) from a time |
| After the first deploy | Tables can be added; size can change | Tables can be added (Oracle: only after Start over); memory and TaskManagers can change, the number of readers only with Start over |
| Personal data | Mask, remove or keep each column | Remove or keep (no masking) |
| Transforms | Every step but computed columns, including value formats, outbox, flattening, script filters and custom steps | Filters, renames, added fields, routes, keep deleted rows, cut text and computed columns (expressions in Expert mode) |
| Delivery canary | Where the source and destination allow it | Off |
| What the source needs | What the checks list for Debezium | The same, plus: PostgreSQL tables with `REPLICA IDENTITY FULL` and 1 + readers free replication slots during the first copy; MySQL Connector/J in the image; for Oracle, the list under *Oracle on Flink CDC* |
| Image | Debezium Server images listed under [Engine versions](engine-versions.md) (Iceberg runs its own build) | One an Admin builds, scans and registers (BeziSync publishes none) |

## Switch engines

The engine is fixed at the first deploy. Until then the Destination step can switch it.

After the first deploy, create a new pipeline on the other engine:

1. Create the new pipeline. It makes its own first copy and keeps its own position.
2. Wait until the new pipeline is Healthy.
3. Delete the old pipeline.

Consumers of a Kafka pipeline get a different record shape after such a switch.

## How the wizard offers engines

- The Destination step lists only the destinations that an engine the project allows can write
  from the pipeline's source.
- The other destinations are named under the list with the reason: no engine runs this source to
  them, or the project doesn't allow the engine that does.
- A destination whose engine isn't set up yet is shown disabled, with what an Admin does about it.
- **Add a destination** offers the same types.
- When a destination takes both engines and the project allows only one, that one is chosen, in
  the wizard and through the API, CLI and MCP.
- The Source step disables a source no allowed engine reads (MongoDB in a project that allows only
  Flink CDC), and a template whose destination the source can't go to.

---

[All documentation](../README.md)
