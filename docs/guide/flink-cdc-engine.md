# Flink CDC engine

<a id="flink-cdc-engine-optional"></a>

Flink CDC is the second engine a pipeline can run on, next to Debezium. Use it to keep tables in
sync in lakehouse and analytics stores. [Choose an engine](choose-an-engine.md) compares the two.

It reads MySQL/MariaDB, PostgreSQL, SQL Server and Oracle, and keeps their tables in sync in
Iceberg, StarRocks, Doris, Fluss, Elasticsearch, Apache Paimon, OceanBase and Kafka. It copies
large tables in parallel chunks, applies schema changes to the destination tables and keeps saved
positions you can restore.

Each pipeline runs its own small Flink cluster: a JobManager and TaskManagers. So it needs more
CPU and memory than a Debezium pipeline.

## Set it up (Admin)

Flink CDC is off until an Admin registers a Flink CDC image. BeziSync publishes no Flink CDC
image, so you build one.

1. Build an image on the official Flink image (`flink:2.2.1-java17`) with Flink CDC 3.7.0, in the
   [layout below](#image-layout).
2. Push it to a registry your runtimes can pull from.
3. Register it under **Settings → Engine versions → Flink CDC**, or set
   `BEZISYNC_FLINK_CDC_DEFAULT_IMAGE`.

Until an image is registered, the wizard shows the Flink CDC-only destinations as unavailable,
with the reason, and Iceberg and Kafka pipelines run on Debezium.

Projects can limit which engines their pipelines use (**Policies → Allowed engines**).

### Image layout

| Path | What |
|---|---|
| `/opt/flink` | Flink 2.2.1 (the base image); add `flink-shaded-guava-31.1-jre-17.0.jar` to `/opt/flink/lib` (Flink CDC on Flink 2.2 needs it) |
| `/opt/flink-cdc/lib/flink-cdc-dist-3.7.0-2.2.jar` | Flink CDC's runtime, from the Flink CDC 3.7.0 release |
| `/opt/flink-cdc/connectors/<name>.jar` | Flink CDC's pipeline connectors (`flink-cdc-pipeline-connector-*-3.7.0-2.2.jar`), renamed (see the next table) |
| `/opt/flink-cdc/drivers/` | JDBC drivers the connectors leave out (see [MySQL driver](#mysql-driver)) |

Connector file names under `/opt/flink-cdc/connectors/`:

| For | `<name>` |
|---|---|
| Each pipeline connector, renamed to | `mysql`, `postgresql`, `sqlserver`, `oracle`, `iceberg`, `starrocks`, `doris`, `fluss`, `kafka`, `elasticsearch`, `paimon`, `oceanbase` |
| Paimon, also | `paimon-s3` (Paimon 2.0.0) and the Hadoop JARs below |
| OceanBase, also | `oceanbase-client` (OceanBase Connector/J 2.4.14, LGPL) |
| Iceberg, also | `iceberg-aws` (`iceberg-aws-bundle` 1.10.1), `hadoop-client-api` and `hadoop-client-runtime` (3.3.6) and `commons-logging` (1.3.5) |

- Only the connectors your pipelines use have to be there. A pipeline whose connector is missing
  fails at start and says which file it expected.
- End the image with `USER flink` (uid 9999, the base image's user). Kubernetes runtimes run it as
  that uid, Docker runtimes as the image's user, both on a read-only root filesystem, so these
  files must be readable by it.

### MySQL driver

MySQL/MariaDB sources need MySQL Connector/J. Flink CDC leaves it out of its MySQL connector for
licence reasons (it is GPL-licensed). Copy `mysql-connector-j-<version>.jar` into
`/opt/flink-cdc/drivers`; its licence is then yours to accept.

Every other source and destination works without it. Flink CDC's StarRocks connector carries its
own copy of the driver (Connector/J 5.1.49).

### Pulling and scanning the image

- A Docker runtime pulls images anonymously: for a private registry, pull the image on the Docker
  host first. Kubernetes runtimes use the runtime's image pull secrets.
- The image holds Flink and Flink CDC only. BeziSync copies its own helper into each pipeline at
  start, so an image keeps working across BeziSync upgrades.
- Scan the image you run with your own tools. Flink CDC's connector JARs bundle libraries
  (Jackson, Netty, protobuf and others) with published vulnerabilities that only a Flink CDC
  release can update.

## Create a pipeline on Flink CDC

The wizard's destination step shows the engine:

| Destination | Engine |
|---|---|
| StarRocks, Doris, Fluss, Elasticsearch, Paimon, OceanBase | Flink CDC |
| Iceberg, Kafka | Both; Debezium is the default |

The engine can change until the pipeline's first deploy. After that, create a new pipeline.

### Tables

- Tables need a primary key, or key columns picked in the Transform step, which can include added
  and computed columns (`flink_primary_key`).
- PostgreSQL tables need `REPLICA IDENTITY FULL` (`flink_replica_identity`, with the SQL to run).
- MySQL readers get a block of server ids no other pipeline on that source uses
  (`flink_server_ids`).
- Oracle has its own list (see [Oracle on Flink CDC](#oracle-on-flink-cdc)).

### Columns and transforms

- Columns can be removed or kept, not masked: Flink CDC has no hash function.
- Filters built from rules, renames and added fields work.
- Script filters, outbox, flattening and custom steps are Debezium only, and so are value formats
  (decimals, timestamps, binary): each column keeps its own type.

### Settings

The review step offers:

| Setting | Choices |
|---|---|
| What a change of a source table's shape does | *apply* (new and renamed columns are added, nothing is dropped), *ignore*, *stop* |
| New columns | Held back until the next deploy (default) or sent at once |
| How often the position is saved (and, for Iceberg, data committed) | 30 s, 1 min or 5 min |
| How long a load batch may wait (StarRocks and Doris) | 2, 5 or 30 s |

Expert mode adds Flink settings from a fixed list (checkpoint timeouts, restart strategy, memory
fractions, timeouts), each with its range; BeziSync keeps the rest.

**Engine settings** also offer the connectors' options:

- the sources' connect timeout and attempts (MySQL, PostgreSQL);
- PostgreSQL's WAL release delay;
- copying the open-ended chunk first (experimental: a pill, and a preflight warning);
- StarRocks' load batch bytes and threads;
- Iceberg's target file size and the schema change timeout.

**Advanced Flink CDC options** pass through only the connectors' own prefixes:

| Prefix | Rule |
|---|---|
| `source.jdbc.properties.*` | Unsafe driver options refused, as for Debezium |
| `source.debezium.*` | Debezium's rules |
| `sink.properties.*`, `table.create.properties.*`, `table.properties.*` (the destination's) | Not the Iceberg keys that would move or rewrite a table |

Catalog and Hadoop options, bare Flink keys and anything BeziSync sets are refused. Values are
written by the YAML encoder.

### Size

| Size | TaskManagers | Readers |
|---|---|---|
| Small | 1 | 2 |
| Medium | 2 | 4 |
| Large | 4 | 8 |

Each TaskManager asks for 1 CPU and 2 GiB, the JobManager 1 to 1.5 GiB. Every reader copies in
parallel, but streaming reads the binlog or WAL with one.

**Custom cluster.** In Review → Size → Custom (Expert mode) you name how many readers run (1 to
128) and what each reader's slot needs (CPU 0.25 to 4, memory 512 MiB to 8 GiB). In YAML:
`policies: {resources: custom}` with `flink_cdc: {cluster: {parallelism, slot: {cpu, memory},
job_manager: {cpu, memory}}}`.

- The form starts from the size the pipeline had and draws how the cluster would be packed now,
  with why it splits and its total against the largest node.
- When it deploys, BeziSync packs the slots into the fewest TaskManagers the runtime's largest
  node holds (at most 8 slots each, plus 512 MiB per TaskManager). It keeps that shape until the
  next deploy, so nodes coming and going never restart it.
- `shape: manual` with `task_managers: {count, slots, cpu, memory}` sets the TaskManagers
  yourself.

**Shrink after the first copy** (`streaming_slot: {cpu, memory}`, smaller than `slot`; automatic
shape only). Streaming reads the log with one reader. So once every table is copied and a position
was saved after streaming began, the controller repacks the cluster for the streaming size.

- The repack is a restart from that position, audited as `controller`, and shown on the
  Overview's cluster card.
- It grows back to the copy size before copying again: a Start over that copies every table,
  restoring a position saved during the copy, or a deploy that adds tables.
- If the streaming shape fails before it saves a position, it goes back to the copy size. After
  three such failures BeziSync stops shrinking it (alert *Cluster stopped shrinking*).
- With the project policy `shrink_after_copy: suggest`, the Overview offers **Shrink now**
  instead.

### Deploy checks

| Check | What it does |
|---|---|
| `flink_cluster_fit` | Blocks a cluster whose slot or TaskManager is bigger than any node, even when forced; *unknown* when the runtime doesn't report its nodes |
| `runtime_capacity` | Checks the runtime has room. Preflight and project limits count the copy size, the peak |
| `flink_replication_slots` | On PostgreSQL each reader opens its own temporary replication slot while it copies, so a first copy needs 1 + readers free slots and WAL senders. Blocks a deploy that would run out part-way |
| `flink_kubernetes_services` | Kubernetes runtimes need the Role to allow Services; both charts grant it from 0.2.0 |
| `flink_agent_version` | Agent runtimes need an agent of this BeziSync version |

## Operate it

The **Overview** shows the cluster: the job, its JobManager and TaskManagers, the last saved
position and where the pipeline started from. Health comes from the job itself and is never green
before it runs and reads changes. **Logs** choose the JobManager or a TaskManager.

Pause, resume, restart, new versions and rollback work as for Debezium. Each stop saves the
position first, and the next start resumes from it.

### Change the cluster of a deployed pipeline

A deployed pipeline can change its TaskManagers, slots, CPU and memory: the deploy saves the
position, recreates the cluster and restores it.

Its number of readers stays while a saved position is left to restore, because the position is
kept per reader. `flink_readers_changed` blocks that deploy, even when forced, until you pause it
and use **Positions → Start over**.

### Saved positions

The **Positions** tab (instead of Snapshots) lists the saved positions. They are encrypted in the
state database. Positions set aside by a restore or start over, and all but the newest 10
savepoints, are removed after 7 days.

While the pipeline is paused, you can:

- **Restore** an earlier position. The job reads the changes since that position again: keyed
  tables are upserted, tables without a key get those rows twice. One from before a source column
  change the destination already has fails with `schema_change_replayed`, a Flink CDC 3.7 limit.
- **Start over**: copy every table again, read from now, or read since a time (MySQL and SQL
  Server).

Re-copying selected tables is not available: start over instead.

Deleting the pipeline removes its saved positions and, for PostgreSQL, every replication slot it
created.

### Schema and column changes

Iceberg and Paimon write a row's values into a table by position, and Flink CDC 3.7 never changes
a table that already exists. So a job writing other columns than its table has would put values
into the wrong columns. BeziSync keeps that from happening:

**While the pipeline runs**, columns the source adds, renames, widens or drops reach the
destination as Flink CDC applies them (with *Apply them*):

| Source change | In the destination |
|---|---|
| Added column | Added at the end |
| Renamed column | A new column, with the old one kept empty |
| Dropped column | Kept empty |

**A version that changes the columns the pipeline writes** switches columns at the saved position.
That is a step that adds, computes or removes a column, keeping deleted rows, leaving a column out,
or rolling back to a version with other columns.

1. The running job stops with a savepoint.
2. BeziSync adds the new columns to the destination tables (planned as for a start without a
   saved position, below).
3. The job resumes from exactly there, writing the new columns.

Nothing is copied again and no change the source made is lost. Rows written before keep the
columns they had; a new column is empty there. Preflight says so (`flink_columns_changed`). To
fill the new columns for the old rows too, Start over and copy the tables again.

A source column added after the last saved position (while the pipeline was stopped) isn't
written at a switch on a table whose order BeziSync fixed: it waits for the next deploy.

**A column added while the pipeline was stopped.** Flink CDC learns a table's new column only when
it reads the change that added it, so a deploy that names it before the resumed job got there
would fail the job. When that happens BeziSync holds the column back (`column_not_read`, audited
`pipeline.columns_held`) and resumes. The next deploy writes it, switching columns at the saved
position.

**A start without a saved position** (the first one, or one after Start over) is planned:

- BeziSync reads every destination table that exists and writes its columns in the table's order.
  A column the pipeline no longer writes stays, empty for new rows.
- It adds the columns a table lacks at its end before the start (Iceberg, StarRocks, Doris and
  OceanBase). Paimon tables you change yourself: preflight gives the statement.
- It refuses a start that would empty a key, partition or NOT NULL column, needs a type it can't
  name, or whose table it can't read (`flink_destination_columns`).
- Where it had to fix the order, new source columns wait for the next deploy.
- BeziSync can't read Fluss tables: preflight warns that an existing one drops the columns it
  doesn't have.

**The helper never starts fresh on its own.** With nothing to restore and no planned start (a
crash before the first saved position, state too large to save), the pipeline stops with
`no_position` instead: Restore a saved position or Start over.

**Older positions.** A saved position from before the last Start over can't be restored
(`position_before_layout`): its job wrote other columns. One from before a column switch can: it
resumes with the new columns too.

**StarRocks** reserves `__op`, so *Keep deleted rows* writes the change type into `__bz_op` there.

### Scripts and agents

- Manifests carry `spec.engine: flink-cdc`, the `flink_cdc` settings and an optional
  `flinkCdcImage`. Exports write `engine` only for Flink CDC pipelines.
- `bezisync remote get` has an ENGINE column.
- `bezisync remote pipelines positions NAME`, `… restore NAME POSITION_ID --yes` and
  `… start-over NAME --from-scratch|--from-now|--since TIME --yes` operate positions.
- MCP adds `list_engines`, `list_saved_positions` and `restore_saved_position`,
  `reset_pipeline_offsets` takes `from_time`, and pipeline logs take a component.

```yaml
spec:
  engine: flink-cdc
  runtime: prod-k8s
  source: {connection: orders-db, tables: {include: [shop.orders]}}
  sink: {connection: warehouse-starrocks, naming: "{schema}_{table}"}
  flink_cdc: {schema_changes: apply, new_columns: hold, commit_seconds: 60, freshness_seconds: 5}
  policies: {resources: medium}
```

## Details and limits

### Oracle on Flink CDC

Flink CDC 3.7 reads Oracle through its own copy of Debezium (1.9). So it asks more of the database
than Debezium Server does, and reads less.

**Releases.** Enterprise, Standard or Express Edition, multitenant or not, up to 23ai.

- Oracle Database Free (every release) and Oracle AI Database 26ai can't be read: preflight blocks
  them (`flink_oracle_version`, even when forced). Run those pipelines on Debezium.
- A database that moves to 26ai later stops its Flink pipeline at the next restart, and health
  says why.
- Measured end to end on Express Edition 21c; Enterprise Edition passes the same version rule.

**Grants.** What Debezium needs (the checks give the script), plus `ANALYZE ANY` in the PDB.

- Each first copy runs `ANALYZE TABLE … COMPUTE STATISTICS` on every table before it splits the
  copy into chunks. That reads the table in full and replaces its optimizer statistics, so query
  plans may change.
- Preflight warns before every deploy that copies.
- It blocks when the right is missing (`flink_oracle_analyze`, with the `GRANT` to run in the
  PDB), and blocks tables whose statistics are locked (ORA-38029).

**Multitenant.** BeziSync keeps `LOG_MINING_FLUSH` (one row) in the connector user's schema of the
CDB root, where LogMiner runs. Debezium Server's Oracle connector also creates this table there.

- The user needs `CREATE TABLE` and a quota in the root, or a DBA creates it
  (`flink_oracle_flush_table` gives the SQL).
- It is kept when pipelines are deleted, because every pipeline mining as that user shares it.
  Drop it with `DROP TABLE <user>.LOG_MINING_FLUSH` once none remain.
- Debezium 1.9 also leaves an empty copy in the PDB.
- Managed services that allow no objects in the root can't run Oracle on Flink CDC.

**Tables.** Names must be upper case (Oracle's unquoted names); quoted lower- or mixed-case names
are refused (`flink_table_name`). There is no signal or canary table on Flink CDC.

**Schema changes.** Added columns reach the destination. Renaming or dropping a column stops the
pipeline: LogMiner can no longer rebuild the changes made before it ("Oracle failed to
re-construct redo SQL"), and health says so (`oracle_schema_change`). Pause it and use
**Positions → Start over**. Choose Debezium for tables whose columns get renamed.

**Types.**

| Oracle type | Written as |
|---|---|
| Bare `NUMBER` | `DECIMAL(38,19)`: more than 19 digits before the decimal point don't fit; more after it are rounded |
| `NUMBER(p,0)` up to 18 digits | `BIGINT` |
| `DATE` | A timestamp |

**Positions.** Start over copies every table again or reads from now; there is no start from a
time.

- Adding tables to a deployed pipeline needs a Start over first. The reader never copies a table
  added to a pipeline that restores a saved position, so preflight blocks that deploy
  (`flink_tables_added`, even when forced).
- A position saved before tables were added can no longer be restored.
- Capture new tables in a second pipeline to avoid the full copy.

**Source protection.** The Overview shows how long the archive keeps redo, but not how far behind
a Flink pipeline is: its position is inside its encrypted saved state.

**Options.** `source.jdbc.properties.*` are refused (the reader ignores them); `source.debezium.*`
take Debezium 1.9's names.

### Other sources

Flink CDC 3.7 has pipeline sources only for MySQL, PostgreSQL, SQL Server and Oracle. These stay
on Debezium:

- MongoDB, Vitess and TiDB have none (only Flink SQL readers, which can't apply schema changes or
  routes).
- Its OceanBase connector writes rather than reads.

TiDB and OceanBase have no BeziSync connection type.

---

[All documentation](../README.md)
