# BeziSync guide

BeziSync is a self-service control plane for change data capture: platform teams connect runtimes
and set guardrails once; anyone can then stream changes from a database to a destination safely.
Each pipeline runs on one of two engines, [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
or [Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) (see [Choose an
engine](#choose-an-engine)). This guide covers installing, using and operating it.

- [Install](#install) · [Concepts](#concepts) · [Your first pipeline](#your-first-pipeline) · [Choose an engine](#choose-an-engine) · [Sources](#sources)
- [Destinations](#destinations) · [Databases behind a bastion (SSH)](#databases-behind-a-bastion-ssh) · [Operate](#operate) · [Manage runtimes](#manage-runtimes) · [Remote runtimes (agents)](#remote-runtimes-agents) · [Google GKE and Amazon EKS](#google-gke-and-amazon-eks) · [Engine versions](#engine-versions) · [Flink CDC engine](#flink-cdc-engine)
- [Guardrails: policies, approvals, templates](#guardrails-policies-approvals-templates)
- [Secrets and secret stores](#secrets-and-secret-stores) · [Single sign-on](#single-sign-on)
- [Config-as-code and the CLI](#config-as-code-and-the-cli) · [Adopt an existing Debezium Server](#adopt-an-existing-debezium-server)
- [Administration](#administration) · [Configuration reference](#configuration-reference)
- Reference: [preflight checks](reference/checks.md) · [threat model](security/threat-model.md)

## Install

**Try it (Docker, two minutes):** `docker compose -f deploy/compose/docker-compose.yml up -d --build`,
open http://localhost:8080, sign in as `admin@bezisync.local` / `bezisync-demo`. The **Sandbox**
project is wired to a sample shop database with live orders, Kafka and a local Docker runtime.

**Kubernetes (production):** the charts are published as OCI artifacts:

```bash
helm install bezisync oci://ghcr.io/daun-gatal/charts/bezisync --version <version> \
  -n bezisync --create-namespace -f my-values.yaml
```

Use an external, highly available PostgreSQL (`secrets.databaseUrl`, or `existingSecret`), a
master key (`bezisync generate-master-key`), an `https` `publicUrl` and `cookieSecure: true`. The
controller can run connectors in the namespaces listed in `kubernetesRuntime.namespaces` (direct
mode). Production refuses to start with development keys, insecure cookies or a non-https public
URL — unless `allowPlainHttp: true` says the network itself encrypts traffic (e.g. a Service
exposed only on a Tailscale tailnet). Useful values: `imagePullSecrets`, `service.annotations` /
`service.loadBalancerClass` (e.g. the Tailscale operator), `postgres.storageClass`, `ingress.*`.
`appArmorProfile` (e.g. `{ type: Localhost, localhostProfile: bezisync-default }`) gives every pod of
the chart an AppArmor profile loaded on each node;
[`deploy/apparmor/`](../deploy/apparmor/kustomization.yaml) (`kubectl apply -k deploy/apparmor`) loads one at every boot for kernels whose
default containerd profile blocks signals inside containers.
The charts install the `latest` release images by default (`image.tag` / `webImage.tag`, pulled
with `pullPolicy: Always`); pin `image.tag` and `webImage.tag` to a release (e.g. `0.1.0`) for
reproducible upgrades. The images (`X.Y.Z`, for linux/amd64 and linux/arm64) and the CLI share the
release version; each chart has its own version (`helm show chart
oci://ghcr.io/daun-gatal/charts/bezisync` shows it) and installs the release it was published with. Every
release lists its chart versions; verify what you pull with `gh attestation verify
oci://ghcr.io/daun-gatal/bezisync:X.Y.Z --owner daun-gatal`.

**Private registry.** To install from a mirror or a private registry, push the images there and
create an image pull secret for the cluster:
`kubectl create secret docker-registry ghcr-pull-secret --docker-server=<registry> --docker-username=<user> --docker-password=<token>`,
listed in `imagePullSecrets` of both charts (and in a runtime's *image pull secrets* when
connectors use a private image), and point `image.repository` and `webImage.repository` at it.

PostgreSQL holds BeziSync's metadata **and** each connector's offsets and schema history
(`debezium_state` schema, or `BEZISYNC_STATE_DATABASE_URL`). It is on the data path: run it HA and
back it up.

## Concepts

| Term | Meaning |
|---|---|
| Project | Isolation boundary (e.g. `staging`, `prod`) with members, runtimes, connections, pipelines and policies |
| Runtime | Where connectors run: a Kubernetes namespace or a Docker host, directly or through an agent |
| Connection | A reusable source (PostgreSQL, MySQL/MariaDB, SQL Server, MongoDB, Oracle, CockroachDB, Vitess, Google Cloud Spanner) or destination (Kafka, HTTP, Pub/Sub, Kinesis, Redis Streams, NATS JetStream, Apache Iceberg, PostgreSQL / MySQL / MariaDB / SQL Server through JDBC, Amazon SQS, Amazon SNS, Azure Event Hubs, Apache Pulsar, StarRocks, Apache Doris, Apache Fluss, Elasticsearch, Apache Paimon, OceanBase) |
| Pipeline | One source → one destination, the tables, format, transforms and settings. Every saved change is a **version**; deploying runs one version |
| Engine | What runs a pipeline: **Debezium** (one Debezium Server) or **Flink CDC** (a small Flink cluster). Chosen per pipeline in the Destination step and fixed at its first deploy; see [Choose an engine](#choose-an-engine) |
| Health | Healthy · Lagging · At risk (source impact) · Down · Unknown. Never green without fresh evidence |

**Admin** is account-wide: Admins open every project with every permission and are the only ones who
see **Settings** (people, projects, runtimes, engine versions, policies). Everyone else gets a role
per project: Viewer, Operator (pause/resume, preview data) or Editor (create, deploy and upgrade
pipelines and connections); "Can approve" is a separate per-project right. Everyone has **Account**
(user menu) for their name, theme (System, Light or Dark, also in the user menu), Guided/Expert mode, notifications and personal API tokens. Long lists show 20 rows a page; the rows-per-page choice under each list (10 to 100) applies to every list in that browser. **Help & tour**
(bottom of the sidebar) opens the **Tour** (what BeziSync does across a pipeline's life, what to try
in the project, and what Admins get; its closing "Would your team use BeziSync?" answer reaches the
Admins as feedback), **Getting started**, **Learn** (Debezium terms in plain words) and search. New
members see a dismissible "New here?" card until they open the Tour.

## Your first pipeline

1. **Settings → Runtimes:** add a Kubernetes namespace or Docker host (or install an agent).
2. **Connections:** add the source. BeziSync runs readiness checks (logical replication, privileges,
   retention, keys…) and shows each failure with the exact fix for your provider (self-hosted, RDS,
   Cloud SQL, Azure…) and a "send to my DBA" message. Add the destination the same way.
3. **New pipeline:** pick the source (or start from a template), select tables and choose what
   happens to personal data (see [Personal data](#personal-data)), pick the destination, its naming and, where it offers both, the engine (see [Choose an engine](#choose-an-engine)), shape the events (optional,
   see below), review the generated `application.properties`, and deploy. A step opens once the
   ones before it are done, and leaving a step by any route saves it. On **Review**, settings save
   as you change them and the final checks re-run on their own; Deploy says why while it is
   unavailable. Final checks must pass; "Deploy anyway" (if the project allows it) needs a recorded
   reason and never skips configuration, runtime or policy failures. Closing a fresh draft asks
   whether to keep or delete it.

## Choose an engine

Each pipeline runs on one of two engines, picked in the wizard's Destination step:

- **Debezium** runs each pipeline as its own Debezium Server. It reads every source and writes
  every destination except the six that only Flink CDC writes. It is the default and always available.
- **Flink CDC** runs each pipeline as its own small Flink cluster: a *JobManager* that coordinates
  and *TaskManagers* whose *readers* copy and stream the tables. It reads MySQL/MariaDB, PostgreSQL,
  SQL Server and Oracle, and keeps their tables in sync in lakehouse, analytics and search stores
  and in Kafka. It is available once an Admin registers a Flink CDC image (see [Flink CDC
  engine](#flink-cdc-engine)); until then the wizard shows its destinations as unavailable, with
  the reason. **Policies → Allowed engines** can limit the engines a project's pipelines use.

The Destination step lists only the destinations an engine the project allows can write from the
pipeline's source, and names the others under the list with the reason (no engine runs this source to
them, or the project doesn't allow the engine that does); a destination whose engine isn't set up yet
is shown disabled, with what an Admin does about it. **Add a destination** offers the same types.
When a destination takes both engines and the project allows only one, that one is chosen, in the
wizard and through the API, CLI and MCP. The Source step disables a source no allowed engine reads
(MongoDB in a project that allows only Flink CDC) and a template whose destination the source can't
go to.

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

MongoDB, CockroachDB, Vitess and Google Cloud Spanner are read by Debezium only, so their pipelines
write the Debezium destinations (see [Sources](#sources) for what each source does on each engine).

**When the destination takes both (Iceberg, Kafka).** Choose **Flink CDC** for large tables whose
first copy should run in parallel, for source schema changes applied to the destination tables, or
to restore an earlier saved position. Choose **Debezium** for an Iceberg change log (*every change
appended*), Iceberg's automatic partitioning and the time of the change (`__source_ts_ms`), Kafka
records in Debezium's formats and event shapes (on Flink CDC they are always JSON without the
schema, in its own change envelope: see [Kafka on Flink CDC](#kafka-on-flink-cdc)), masked columns,
or copying one table again while the others stream.

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
| Image | Debezium Server images listed under [Engine versions](#engine-versions) (Iceberg runs its own build) | One an Admin builds, scans and registers (BeziSync publishes none) |

**The engine is fixed at the first deploy.** Until then the Destination step can switch it. After
that, create a new pipeline on the other engine: it makes its own first copy and keeps its own
position. Delete the old pipeline once the new one is Healthy. Consumers of a Kafka pipeline get a
different record shape after such a switch.

## Sources

Every source runs the readiness checks of its own database, each failure with the exact fix for the
provider, and shows **source protection** in its own terms (see [Operate](#operate)). Sources Debezium
marks *incubating* carry a **Preview** badge when you add them. What a source can do decides what the
wizard offers: the snapshot modes on Review, re-copies on the Snapshots tab, masking, message keys
and the delivery canary. The first four columns describe Debezium; the last says whether
[Flink CDC](#flink-cdc-engine) reads the source too. On Flink CDC every source copies its tables in
parallel chunks, copies them again only all together (Start over), can't mask and runs no canary
(see [Choose an engine](#choose-an-engine)).

| Source | Copies existing rows | Re-copies while streaming | Mask columns | Delivery canary | Flink CDC |
|---|---|---|---|---|---|
| PostgreSQL, MySQL / MariaDB, SQL Server | every mode | yes | yes | PostgreSQL, MySQL | yes (MySQL needs Connector/J in the image) |
| MongoDB | every mode | yes | no | no | no |
| Oracle | every mode | yes | yes | yes | yes, narrower: see *Oracle on Flink CDC* |
| CockroachDB | every mode | yes | yes | yes | no |
| Vitess | "Copy existing rows" or "Only new changes" | no: reset the position from scratch | yes | no | no |
| Google Cloud Spanner | no: it streams from its first start | no | no (columns can be removed) | no | no |

### Oracle

Oracle 19c and newer through **LogMiner**, self-hosted, on Amazon RDS for Oracle or OCI Base
Database (Autonomous Database has no LogMiner). The database must run in **ARCHIVELOG** mode with
minimal **supplemental logging**, and every captured table logs all its columns
(`ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`); the checks give each statement, and the
GRANT script for whatever the user lacks (`rdsadmin` procedures on RDS).

- **Database** is the service Debezium signs in to: the database itself or, for a multitenant
  database, its CDB (for example `ORCLCDB`); **Pluggable database** names the PDB with the tables
  and then needs a common user (`C##…`). If the PDB's service name differs from its name, set it
  under Expert.
- BeziSync keeps its signal table `BZ_SIGNAL_<id>` (and `BZ_CANARY_<id>`) in the connector user's
  own schema, so the user needs a quota on its default tablespace; they are removed with the pipeline.
- Unquoted Oracle names are upper case, so are table names in events and destinations
  (`cdc.SHOP.ORDERS`). Tables in schemas whose names hold `#` or `$` can't be routed and are not offered.
- LOB columns (`CLOB`, `NCLOB`, `BLOB`, `XMLTYPE`) arrive empty unless the connection's *Capture
  LOB columns* (Expert) is on; `LONG`, `LONG RAW`, `BFILE` and object types are never captured.
- Checks warn when the archived logs reach back less than a day (on RDS: `archivelog retention
  hours`) and when the first copy may outlast `UNDO_RETENTION` (ORA-01555).
- The connection is plain TCP; use Oracle Native Network Encryption on the server to encrypt it.
- Oracle can also run on Flink CDC (Iceberg, Kafka, StarRocks, Doris, Fluss, Elasticsearch, Paimon, OceanBase), with narrower support and more grants: see [Oracle on Flink CDC](#flink-cdc-engine).

### CockroachDB

CockroachDB v22.2 and newer, self-hosted or Cloud. Debezium reads a **sinkless changefeed** over its
own SQL connection, so no Kafka sits in between (Debezium 3.7 or newer: deploy refuses an older
official image, and warns for a custom one), and resumes from its last resolved timestamp, which
the cluster keeps only for the tables' **GC TTL** (`gc.ttlseconds`, 4 hours by default): a pipeline
stopped for longer must copy its tables again. Needs `kv.rangefeed.enabled`, `CHANGEFEED` and
`SELECT` on the tables, and the right to create the `bezisync` schema (signal and canary tables).
For CockroachDB Cloud choose *verify-full* and paste the cluster's CA certificate. Decimals always
arrive as text. A table without a primary key is keyed by CockroachDB's hidden `rowid`, which also
appears in its rows. Debezium 3.7 sends the `TIMESTAMP` and `TIMESTAMPTZ` columns of rows copied
again on the Snapshots tab as empty; live changes and the first copy carry them.

### Vitess

One keyspace, across all its shards, through **VTGate**: Debezium streams from its gRPC port (15991)
and BeziSync reads tables through its MySQL port (15306). Choose whether to read from primary,
replica or read-only tablets. The position is one GTID set per shard. How long the tablets keep their
binlogs is not visible through VTGate, so source protection stays *Unknown*: keep at least a day of
binlogs on every tablet and watch it with your Vitess monitoring. Vitess can't copy tables again
while streaming; to copy everything again, pause, reset the position from scratch and resume. Rows of
the first copy arrive as inserts. Decimals always arrive as text; spatial columns are not captured.
The gRPC connection is not encrypted, so keep VTGate on a private network (an SSH bastion works too).

### Google Cloud Spanner

GoogleSQL databases (not the PostgreSQL dialect) through a **change stream**. Debezium's Spanner
connector keeps its partition state in two Kafka topics of its own even in Debezium Server, so the
connection names a **coordination Kafka**: pick one of the project's Kafka connections (it can't be
deleted while a Spanner connection uses it). BeziSync checks it like a Kafka destination and removes
the connector's `_sync_topic_spanner_connector_bz_<id>` and `_rebalancing_topic_…` topics when the
pipeline is deleted or its position reset.

- By default BeziSync creates one change stream per pipeline, `bz_<id>`, watching the pipeline's
  tables with full rows (`NEW_ROW_AND_OLD_VALUES`, kept 7 days), updates it when tables change and
  drops it with the pipeline. That needs schema changes (`roles/spanner.databaseAdmin`); otherwise
  create a stream yourself and choose *Use an existing change stream*.
- Spanner can't copy the rows a table already has: a pipeline streams the changes made from its
  first start. It can't mask columns, but removed columns are left out of BeziSync's change stream,
  so they never leave the database (with your own stream, keep them or let BeziSync create it).
- Without a service-account key, BeziSync and the connector use the default credentials where they
  run (Workload Identity on GKE). The emulator (Expert) is for tests.
- Source protection compares how far the pipeline is behind with the stream's `retention_period`.

## Shape events (transforms)

The wizard's **Transform** step decides what each event looks like. Debezium Server chains single
message transforms (SMTs); BeziSync offers them by goal and puts each step where it works:

| Stage | Steps | Notes |
|---|---|---|
| Value formats | Decimals as exact bytes (default), a number or exact text; timestamps as epoch numbers (default) or ISO-8601 text; binary as base64 or hex | Connector options. ISO text exists for PostgreSQL and MySQL only; CockroachDB and Vitess send decimals as text only; MongoDB and Spanner have none |
| Keep events | Keep only some operations (insert, update, delete, snapshot) and/or rows matching rules on columns (`is`, `is one of`, `>`, `is empty`, …), for all or some tables | Runs on the full change event, so deletes are judged on the row before. PostgreSQL sends only the key of a deleted row unless the table has `REPLICA IDENTITY FULL`. Dropped events are gone for that pipeline; re-snapshot to bring rows back. MongoDB can filter on operations only. On Flink CDC (*Keep only some rows*) the table holds exactly the matching rows (a row an update makes match is added, one it stops matching is deleted), equality with a number matches whatever the column's number type, and in Expert mode the condition can be an expression, as for computed columns (no time-of-reading functions such as `NOW()`, and no `__data_event_type__`: choose operations instead) |
| Event shape | The row after the change (with deletes kept as `__deleted = true`, as tombstones, or dropped, and optional metadata fields such as `__op`) or the full change envelope; **Keep deleted rows** (soft delete) keeps every deleted row, marked — `__deleted = true` on Debezium (the row shape is added when missing, and no tombstone follows), `__op = -D` on Flink CDC, where Iceberg and StarRocks keep the row. It shows under Event shape on Debezium (the row shape's Deletes choice is then locked to it) and under Keep rows on Flink CDC, can be added once, and a destination whose own Deletes option it overrides says so | A table that already has a column of that name is refused (field name duplication). Adding it changes what consumers get |
| Field changes | Rename fields, add a field (fixed text or the pipeline's name), flatten nested fields, **cut long text** (text columns to at most a length: Debezium on PostgreSQL and MySQL, Flink CDC on any source; a masked or left-out column can't be cut; another type is left as it is, and blocks on Flink CDC), custom transforms (Experts, policy) | Order matters within this stage; they need the row shape. The message key keeps column names |
| Destinations | **Send tables elsewhere** (routes): tables a pattern matches (`schema.table`, where `*` matches any run of characters, e.g. `shop.orders_*`) go to one topic or table you name, instead of each to its own; a table two routes match is refused, and the step lists where every captured table goes | Debezium routes after its own naming; Flink CDC writes the matched tables into the named table. Tables sent to one destination must have the same key and columns (preflight blocks otherwise), and a route that matches no captured table is a warning. Adding or changing one moves events, so consumers of the old destination stop receiving them |
| Computed column | **Flink CDC only**: a new column of one table, from a function picked in groups — **Text** (upper/lower case, trim, length, join, join with a separator, part of a text, replace, first/last characters, pad), **Numbers** (absolute value, round, round up/down, add, subtract, multiply, divide), **Dates** (date as text, year, month, day of the month, hour, date from text, days between), **Conversions** (as text, whole, decimal or floating-point number), **Empty values** (first that isn't empty) — with its arguments picked from the table's columns of the kind the function takes. In Expert mode it can be **an expression** in Flink CDC's functions (`CASE WHEN total > 100 THEN 'big' ELSE 'small' END`, `UPPER(city) \|\| ' ' \|\| country`), checked as you type. Debezium shows the step with why it can't run it | BeziSync parses every expression and writes its own rendering into the job (ADR 0008), so no text you type reaches it as typed: only Flink CDC's documented functions (no user-defined, AI or regular-expression functions), no comments or `;`. Every function is NULL-safe (an empty input gives an empty column, never a stopped job), dividing by zero gives an empty value, arithmetic is floating point, and a number compares equal whatever its column type. A masked or left-out column can't be an input. Preflight blocks an input the table no longer has or of a kind its function doesn't take, a name the table already has, and a column Flink CDC reads as one of its time functions (`now`, `date_format`, …) |
| Message key / Primary key | Key columns per table, e.g. for tables without a primary key, each shown with its type | **Debezium** (*Message key*): the source's columns, on PostgreSQL, MySQL, Oracle, CockroachDB and Vitess. Debezium builds the key at the source, before any step runs, so a field a step adds is shown but can't be picked, and the key keeps the source's column names; a key column the table lacks is ignored by Debezium. **Flink CDC** (*Primary key*, the destination table's key): any column the table writes — a source column (under its new name when renamed), an added field, or a computed column. A computed key column must be text or a whole number (an expression whose kind follows its columns needs `CAST(… AS STRING)`), read only columns that are NOT NULL or in the primary key, use a function that always gives a value (not *As a whole number*, *Date from text* or a division; a fallback for an empty value isn't enough, since those rows would share a key) and be the same each time a row is written (no `NOW()` or `UUID()`). On a table with a primary key, a key with an added or computed column keeps the whole primary key. A key column can't be cut, and one that can be empty in the source is a warning when the key doesn't also hold the whole primary key (`transform_key_column_nullable`): Flink CDC writes it as an empty text, so those rows would share a key. A computed date can't be a key column; to partition an Iceberg table by date, put the timestamp column itself in the key (with the primary key) and partition it by its **day** under Partition by — Iceberg derives the date from it. Preflight blocks a key column the table doesn't write (`transform_key_column_unknown`, e.g. a computed column since removed) and one that can't key rows (`transform_key_column_unfit`). A key made of a column an update can change moves the row: the old one stays behind |

Fields are picked from the discovered columns; only new names and rule values are typed, and
anything with `${…}` is refused (Debezium Server would read it as a variable, for example a
secret). The **preview** builds a sample row from the table's columns (no data is read) and shows
the event after every step for an insert, update, delete or snapshot, with its key and
destination. Custom classes and filter scripts cannot be previewed and show as Unknown.
On Flink CDC, which writes rows into tables, the **Destination table** panel lists instead each
column a table gets, its type (the source's type for a source column; for a computed or added
column the kind it writes, such as text or a whole number) and where it comes from (a source
column, renamed or cut, an added field or a computed column), with its key columns marked.

At deploy, preflight checks every step against the tables: a field added or renamed onto one the
event already has **blocks the deploy** (Debezium would stop with "field name duplication"); steps
on missing, excluded or masked columns, rules that can never match a column's type, missing key
columns and custom classes Debezium 3.7 does not ship are warnings. Review (and the approval
request) lists **what changes for consumers**: Breaking, Key change or Additive. After a deploy,
the Events tab says whether real events match the preview, and when a schema change breaks a step
the pipeline's Overview shows it with a **Fix** link and the *Transform needs attention* alert
fires. Every step is in the spec (`transforms`, `values`, `keys`), so YAML, templates, promote and
rollback carry it; `debezium.transforms*` cannot be set as advanced properties.

**Size** (Review): a Debezium pipeline runs at Small (0.5 CPU · 768 MiB), Medium (1 CPU · 1.5 GiB),
Large (2 CPU · 3 GiB) or, in Expert mode, a Custom size: CPU from 0.25 to 8 in quarters and memory
from 512 MiB to 16 GiB in 256 MiB steps (`policies: {resources: custom, custom_resources: {cpu:
"1.5", memory: 2Gi}}`). BeziSync works out the Java heap (70% of memory, leaving the JVM at least
256 MiB) and takes the batch and queue sizes of the largest preset the memory reaches. A project's
largest size limits a custom one by its CPU and memory, and a destination's smallest size too.
Preflight measures the runtime when it runs (`runtime_capacity`): it warns when the CPU or memory
free right now is short (the pods may wait for room), blocks — even forced — when the Kubernetes
namespace's ResourceQuota leaves less than the pipeline asks for (Kubernetes would refuse its
pods), and is *unknown*, never passed, when the runtime doesn't report its capacity. A redeploy
counts only what it adds to what the pipeline runs now, and pipelines deployed to the runtime that
have no pods yet count too (named in the message). Two deploys checked at the very same moment can
still each fit alone; Kubernetes then refuses the pods of the second, and its status gives
Kubernetes' reason ("exceeded quota: …").

Review & deploy saves as you change it. When something can't be saved, the badge above the settings
says which field (**Not saved: fix Memory**, or **Memory and 2 more**; **Fix … before deploying** when
a saved value is the problem); click it to go there, and the error sits under that field. In Guided mode, an engine setting or advanced property that blocks the
save is listed under **Settings that block saving**, with Remove. When the server refuses a save,
Review shows why and waits until you change something, click Try again (for a network or server
error) or leave the step.

**Engine settings** (Review, Expert mode) lists the typed settings BeziSync offers for the pipeline's
engine, source and destination, grouped by when they matter (snapshot, streaming, delivery,
engine): each has its bounds, default and what changing it does to a running pipeline. Add one from
the searchable list; removing it restores the default. Debezium pipelines offer snapshot threads, the
poll interval, the queue and batch sizes (BeziSync sets them from the size; the batch stays below the
queue), a queue memory limit, the wait before reconnecting, skipping unchanged updates, the position
save interval and the log level, and each connector's own: PostgreSQL's slot feedback interval and
slot retries, MySQL's keep-alive, connect timeout and binlog look-ahead, Oracle LogMiner's batch and
pause, MongoDB's check for new collections; Kafka's producer linger, batch bytes and compression,
Pub/Sub's batch delay and size, Redis's batch size and retry delay. A connector's setting is offered
only on that connector, a destination key a connection field sets is never a setting, and sources
without JDBC (MongoDB, Spanner) refuse `debezium.source.driver.*` properties. Preflight checks them
against this run's evidence: each tuned
setting exists in the image's Debezium release (`setting_version`; unknown for an image BeziSync
can't place), snapshot threads fit the source's free connections (`setting_threads`; read from
PostgreSQL and MySQL, unknown otherwise), and a full queue and a copy fetch fit the heap
(`setting_queue_memory`, `setting_fetch_memory`, estimated from the tables' sizes). Review — in
both modes — suggests settings from the same evidence with one click to use them (snapshot threads
for large copies with free connections; skipping unchanged updates when columns are left out) and
hides a suggestion once its evidence is ten minutes old.

**Advanced Debezium properties** (Review, Expert mode, one row per key and value) pass raw `debezium.source.*` and
`debezium.sink.*` keys to Debezium Server for options BeziSync doesn't offer. They may only add what
BeziSync doesn't set: a key BeziSync compiles for the pipeline, or one a wizard choice sets (tables,
columns and personal data, keys, transforms, the event format, where events go, connection and TLS
details, heartbeats and signals, the error policy), is refused with where to set it instead.
Credentials (any key naming a password, secret, token, key store or SASL) belong on the connection.
Unsafe options are never passed: SQL on the source (`database.initial.statements`,
`heartbeat.action.query`, `replica.identity.autoset.values`), dropping or skipping changes
(`slot.drop.on.stop`, `offset.mismatch.strategy`, `lsn.flush.*`), weaker TLS
(`database.sslfactory`), classes loaded into the connector (`converters`, `*.custom.name`,
`post.processors`, `topic.naming.strategy`), JDBC driver options (`driver.*`, `database.*`) that read
local files, load classes, call out, log to files or change TLS and sign-in (such as MySQL's
`allowLoadLocalInfile` and `autoDeserialize`, or a `socketFactory`), `snapshot.select.statement.overrides`, logging
changes that could reveal secrets, and every `quarkus.*` key. Values can't hold `${…}` (Debezium
Server would fill it in from the pod's environment) or line breaks. Saving refuses a key it adds or
changes, and each row says, as you type, whether BeziSync will pass it on or why not (a key that is
also a typed setting offers to become one); one an older pipeline already has still saves, and its deploy is blocked by the
*Advanced properties* check until it is removed (a forced deploy can't skip it). They need the
project policy *Allow advanced properties*; the ones that pass show in preflight as **Unknown**
(grey, "not verified by BeziSync"), never as passed.

## Destinations

Each destination states what it needs from a pipeline, and the wizard only offers that: the names
it accepts (with a live preview), its event formats, the event shape it requires, its own
pipeline settings and the smallest connector size it runs in. Preflight checks the rest; every
destination's checks and their fixes are in the [preflight check reference](reference/checks.md).

Each destination below opens with the engine that writes it: Kafka and Iceberg take both (Debezium
by default), StarRocks, Doris, Fluss, Elasticsearch, Paimon and OceanBase only Flink CDC, and the
rest only Debezium. They are grouped by what they are for: event streams and queues (Kafka to HTTP),
databases (JDBC, OceanBase), lakehouse tables (Iceberg, Paimon, Fluss), analytics (StarRocks, Doris)
and search (Elasticsearch). [Choose an engine](#choose-an-engine) compares the engines.

### Kafka

**Engine:** Debezium (default) or Flink CDC (see [Kafka on Flink CDC](#kafka-on-flink-cdc)).

One topic per table (naming `cdc.{schema}.{table}` by default), keyed by the row key so each row
keeps its order. Presets: **Self-hosted Kafka**, **Confluent Cloud**, **Amazon MSK** (SASL/SCRAM) and
**Redpanda**. The connection takes the bootstrap servers, the security protocol (`PLAINTEXT`, `SSL`,
`SASL_PLAINTEXT`, `SASL_SSL`), the SASL mechanism, user and password, and in Expert mode a private CA.

- **Topics** are created at deploy with the connection's partitions and replication factor (1 and 1
  unless set in Expert mode), unless *Create topics automatically* is off (then they must exist).
- **Formats** (Destination step): JSON (default), Avro or Protobuf with a schema registry URL
  (Apicurio or Confluent), or CloudEvents. The Transform step chooses the event shape.
- **Preflight** reads the broker list, reaches every broker, fetches metadata and signs in, checks
  that the cluster has as many brokers as the replication factor, and that the user may create
  topics (the fix gives the `kafka-acls` command).
- The **Events** tab and the delivery canary read the newest records of each topic. Deleting a
  pipeline with "drop destinations" deletes its topics.

### Kafka on Flink CDC

**Engine:** Flink CDC; Debezium writes Kafka by default (see [Kafka](#kafka)).

On Flink CDC each table goes to the topic its naming
pattern gives, created at deploy with the connection's partitions and replication factor; records are keyed
by the primary key (`{"TableId": …, "id": …}`) and spread by it, and the value is Flink CDC's
Debezium-style JSON: `before`, `after` and `op`, with a short `source` and no schema or `ts_ms`. Consumers
written for Debezium's records need to accept that shape; switching a pipeline's engine changes it.
Only JSON without the schema is available on Flink CDC (choosing Flink CDC sets it; saving another format
is refused), and the Transform step's event shape (Row after change, delete handling, added fields)
doesn't apply there: it is hidden on Flink CDC, and records are always the change envelope above. The
Transform step speaks events on this pair: *Keep events*, *Fields*, the *Message key* (the key columns
the record key carries, which also spread events over partitions), and an **Events** panel with the topic, the key and the value. *Keep deleted rows* sends a
delete as an insert event whose `after` carries `__op = -D`, with no delete event.

### Google Pub/Sub

**Engine:** Debezium.

One topic per table in the connection's Google Cloud project, created at deploy unless turned off.
With *Keep per-row order* (default) each message carries the row key as its ordering key; your
subscriptions need message ordering turned on to use it. Sign in with a service-account key, or
leave it empty to use the runtime's own identity (Workload Identity on GKE); the account needs
`roles/pubsub.editor` (preflight signs in and lists the project's topics, and gives the `gcloud`
command when it can't). Formats as for Kafka. BeziSync can't read Pub/Sub without taking messages
from your subscriptions, so the Events tab has nothing to show and there is no delivery canary.
"Drop destinations" deletes the topics. An emulator host (Expert) is for tests.

### AWS Kinesis

**Engine:** Debezium.

One Kinesis data stream per table in the connection's region, created at deploy (one shard,
more in Expert mode) unless turned off, records keyed by the row key. Sign in with an
access key or the runtime's IAM role; the role needs `kinesis:ListStreams`,
`kinesis:DescribeStreamSummary`, `kinesis:CreateStream`, `kinesis:PutRecord` and `kinesis:PutRecords` (preflight reaches
the endpoint, signs in and says what is missing). A custom endpoint (Expert) points at a VPC
endpoint or an emulator. Formats as for Kafka. The Events tab and the delivery canary read the newest
records; "drop destinations" deletes the streams.

### Amazon SQS and Amazon SNS

**Engine:** Debezium.

- **SQS**: every table of the pipeline goes to the connection's queue, one message per change, so it
  suits low-volume tables; consumers tell tables apart by `source.table` (or `__table` with the
  "row after change" shape). A FIFO queue needs content-based deduplication (Debezium sends no
  deduplication ID). The Events tab *peeks* at messages still waiting (they stay available to your
  consumers, but each look counts as a receive, so it is off when the queue's dead-letter redrive
  allows fewer than 10 receives); there is no delivery canary, as consumers may take it first.
  BeziSync keeps the connector from logging message bodies.
- **SNS**: one topic per table (`cdc_{schema}_{table}`; names ending in `.fifo` make FIFO topics,
  created with content-based deduplication and ordered per row key). Topics are created on deploy
  unless turned off. SNS keeps nothing to read back, so BeziSync subscribes a small queue of its own
  to each topic (`bz-preview-<topic>`, 15-minute retention) for the Events tab and the delivery
  canary; deleting the pipeline removes them (and the topics with "drop destinations").
- Both: keys or the runtime's IAM role; JSON or CloudEvents (SQS and SNS take text); deletes as
  `__deleted` rows or dropped (they refuse empty "tombstone" messages, so BeziSync turns those off);
  messages over 256 KiB (SNS, and SQS by default) stop the pipeline.

### Azure Event Hubs

**Engine:** Debezium.

Every table of the pipeline goes to the connection's event hub (it must exist), partitioned by the
row key so each row keeps its order; consumers tell tables apart by `source.table` (or `__table`).
Sign in with a namespace connection string (shared access policy with **Send**), without
`EntityPath`. The Events tab and the delivery canary read the last events of each partition, which
needs **Listen**: add a second connection string for reading when the first cannot listen
(preflight warns). Events over the tier's limit (1 MB on Standard) stop the pipeline. Entra ID
sign-in is not available: Debezium's sink supports connection strings only.

### Apache Pulsar

**Engine:** Debezium.

One persistent topic per table in the connection's tenant and namespace, keyed by the row key. The
connector produces to the **service URL** (`pulsar://…:6650`, or `pulsar+ssl://…:6651` with an
optional private CA); BeziSync uses the **admin URL** (`http(s)://…:8080`) to check the namespace,
create missing topics on deploy, and read the newest messages for the Events tab and the delivery
canary (`examinemessage`: no subscription, your consumers are untouched). Sign in with a token (JWT,
or a StreamNative API key). A send that does not complete within 30 seconds fails the batch, which
Debezium retries, rather than being lost.

### Redis Streams

**Engine:** Debezium.

Every change is appended (`XADD`) to one stream per table, the row key as the entry's first field and
the event as its value. Presets: **Redis** and **AWS ElastiCache**. The connection takes the host,
port, an ACL user (Redis 6+), password and TLS. Streams grow with every change: set *Trim streams to
N entries* (Expert, an approximate `MAXLEN`) to cap them. Preflight reaches Redis, signs in, checks
the user may write streams, and warns when Redis uses more than 80% of `maxmemory`. The Events tab and
the delivery canary read the newest entries; "drop destinations" deletes the streams.

### NATS JetStream

**Engine:** Debezium.

One subject per table in a persistent JetStream stream (`CDC` by default). BeziSync creates the stream
at deploy with the pipeline's subjects, or adds missing subjects to a stream that exists; it never
changes the stream's limits or retention, so set those on the server. The connection takes the server
URL and an optional user and password. Preflight reaches the server and checks that JetStream is
enabled. The Events tab and the delivery canary read the newest messages.
Deleting a pipeline leaves the stream in place.

### HTTP

**Engine:** Debezium.

Every change event is sent as a `POST` to the connection's URL, one request per event (JSON by
default; formats as for Kafka). A request that fails or takes longer than the timeout (10 seconds) is
retried, 5 times 1 second apart; Expert mode changes all three. Preflight reaches the endpoint and
checks that it answers HTTP; any status other than 2xx is a warning, as deliveries may fail. BeziSync keeps nothing to read back: the Events tab is empty and
there is no delivery canary.

### Database (JDBC)

**Engine:** Debezium.

Changes land in PostgreSQL, MySQL, MariaDB or SQL Server tables, one per source table, written by
Debezium's JDBC sink connector (the official images ship the drivers).

- **Tables** are created in the connection's schema (PostgreSQL), the login's default schema (SQL
  Server, `dbo` unless set otherwise) or the database (MySQL, MariaDB) with the source's key as primary key, and get new columns as the source changes
  (naming `{schema}_{table}`; dots become `_`). "Use existing tables only" turns that off.
- **Rows** (Destination step): *upsert by key* (default: the table mirrors the source) or *insert
  only* (every change is a new row; needs no key). **Deletes** delete the row, or are ignored. With
  the "row after change" shape, BeziSync turns deletes into what this setting needs.
- **Keys**: upserts need a key on every table; preflight blocks tables without one until you pick
  key columns (Transform step) or insert only.
- **Preflight** reaches the database, signs in, asks whether the user may create tables in the
  schema (PostgreSQL: `USAGE` and `CREATE`; SQL Server: `CREATE TABLE` and `ALTER` on the schema;
  MySQL and MariaDB: the `SHOW GRANTS` text, "not known" when a role may grant it) without creating
  one, and checks keys and
  name lengths (63 characters in PostgreSQL, 64 in MySQL, 128 in SQL Server). The **Events** tab
  and the delivery canary read rows of the tables.
- Deleting a pipeline with "drop destinations" drops its tables.
- Across databases, Debezium copies column defaults into the tables it creates: a PostgreSQL
  `timestamptz DEFAULT now()` column fails to create in MySQL or MariaDB ("Invalid default value").
  Exclude the column (Tables step) or create the table yourself and choose "Use existing tables only".

### OceanBase

**Engine:** Flink CDC.

One table per source table in a database of a MySQL tenant, created by Flink CDC over
JDBC with OceanBase Connector/J (in the reference image; no MySQL driver needed). The connection takes the
host and port (2881, or 2883 through OBProxy), the database (created on deploy if missing), the user as
`user@tenant` (`#cluster` through OBProxy) and its password. The connection is not encrypted (no TLS
options): keep OceanBase and the runtime on a private network. Times are written in UTC. Only new and renamed
columns are applied as schema changes; others stop the pipeline. Preflight signs in and reads the user's
`SHOW GRANTS` (it creates nothing); the Events tab reads rows; "drop destinations" drops the tables.

### Apache Iceberg

**Engine:** Debezium (default) or Flink CDC (see *On Flink CDC* below).

Changes land in Iceberg tables, one per source table, through an Iceberg REST catalog or AWS Glue.
Presets: **Iceberg REST catalog** (Polaris, Lakekeeper, Gravitino, Nessie), **Amazon S3 Tables**
(SigV4, service `s3tables`, warehouse = table bucket ARN), **AWS Glue Data Catalog** (warehouse =
an `s3://` location), **Snowflake Open Catalog** (OAuth2, scope `PRINCIPAL_ROLE:ALL`, vended
credentials), **Databricks Unity Catalog** (token, vended credentials) and **RustFS S3 Tables**
(preview: SigV4 service `s3`, warehouse = the table bucket, path-style S3; change logs only, as it
refuses the first upsert commit of a new table). Data files are written
with the object-storage keys of the connection, the runtime's IAM role, or credentials the catalog
vends. With the connection's keys or the role, BeziSync asks the catalog for no credentials, so a
catalog that can't vend them (Polaris over storage without STS, for example) works too. Snowflake, BigQuery (BigLake), Databricks, Trino, StarRocks and ClickHouse read the tables
through the same catalogs.

- **Tables** are created by their first change in the connection's namespace (BeziSync creates the
  namespace on deploy): columns from the source, the primary key (or the key columns chosen in the
  Transform step) as row identity, lower-case names with dots as `_` (naming `{schema}_{table}`).
  New source columns are added; other schema changes stop the pipeline.
- **Rows** (Destination step): *latest row, deletes kept with `__deleted = true`* (default),
  *latest row, deletes removed*, or *every change appended* (a change log). Tables without a key are
  always appended (a preflight warning says which).
- **Commits** at least every minute by default (5 or 15 minutes for fewer, larger files; "as soon as
  possible" commits every batch). Health counts that interval as expected delay, not lag.
- **Partitioning** (both engines): **Partition by**
  in the Transform step (its own row, after the key, so the key, renames and computed columns chosen on
  the same screen are offered as they change) gives a table up to four fields: a column and each value, the year,
  month, day or hour of a date or timestamp (one per column), buckets of a hash (`bucket[N]`, N
  from 1 to 1024) or the leading part (`truncate[N]`, N up to 1,000,000). The columns offered are
  the ones the table writes (after renames, added and computed columns) and the transforms the
  ones Iceberg allows for the type the engine writes: no buckets of a true/false or
  floating-point value, no truncation of a date. It is applied when the table is created; a table
  that already exists keeps its partitioning, so preflight blocks asking it for other partitioning
  (`iceberg_partition_existing`: keep its own, or drop the table with another engine on the
  catalog and Start over so the pipeline creates it).
  - **Upserted tables are partitioned only by key columns** (`iceberg_partition_key`). Rows are
    upserted by writing deletes on the key, and Iceberg applies a delete only to rows in its own
    partition: partitioned by a column outside the key (an `updated_at` day, a `status`), an update
    that changes it leaves the old row behind. The editor shows such columns disabled. For
    time-like layout on an upserted table, partition by the leading part of an increasing key
    (`truncate[100000](id)`). Change logs (*every change appended*) take any column, including
    Debezium's *time of the change* (`__source_ts_ms`). A table that already exists is checked
    against its own identifier fields: one partitioned off them may already hold duplicate rows
    (`SELECT <key>, count(*) … GROUP BY <key> HAVING count(*) > 1`), and the fix is Start over.
  - **On Debezium** a table Partition by doesn't list keeps the automatic partitioning: upserted
    tables bucketed by their first key column (`bucket(id, 16)`; 4 or 64 buckets in the Destination
    step), change logs by the day of the change. The wizard records each table's key column in
    `sink.options.partitions` (set it there through YAML or the API); Expert mode can turn the
    automatic partitioning off (Partition by still applies). Tables routed to one
    destination share its partitioning: a listed table's fields (tables that list fields must list
    the same), else the automatic one when they agree, else none. A field the table doesn't have stops the pipeline when
    the table is created, so preflight checks every field. MongoDB collections can't be listed.
  - **Key columns on Debezium** stay under their own names: a key column renamed or left out drops
    out of the table's identity (`iceberg_key_columns`).
- **Event shape**: Iceberg needs the row after change with `__op` and `__source_ts_ms`; BeziSync adds
  that step (or completes yours) and sends timestamps as ISO-8601 text (Kafka Connect types for SQL
  Server). Outbox steps and tombstone deletes are not available.
- **Preflight** signs in to the catalog and checks the namespace, then checks keys and tables that
  already exist. It creates no table: whether the credentials may create tables and commit is
  "not checked" (the catalog answers that only by trying), and the first commit shows it. The **Events** tab and the
  delivery canary read the newest rows of each table.
- **Image**: pipelines run `ghcr.io/memiiso/debezium-server-iceberg` (see [Engine
  versions](#engine-versions)), amd64 only. It serves health on port 9000, which BeziSync probes;
  agent runtimes need an agent of this BeziSync version or newer (preflight says so). Deleting a pipeline with "drop destinations" drops its
  tables (purging files where the catalog allows; RustFS keeps them).
- **On Flink CDC** (chosen in the wizard when a Flink CDC image is registered; Debezium stays the
  default): the same connections and tables, written by Flink CDC's Iceberg sink. Rows are upserted
  by key (deleted rows are removed, or kept and marked `__op = '-D'`; *every change appended* is
  shown unavailable with the reason: Flink CDC's Iceberg writer always writes rows by the table's key, and
  an Iceberg table created beforehand without a key is refused, `iceberg_flink_keyless_table`, because
  Flink CDC 3.7.0 would write deletes into it as rows) and new columns are added. Tables are unpartitioned unless **Partition by** (see
  *Partitioning*) lists them, compiled into Flink CDC's `partition.key`; there is no automatic
  partitioning and no time of the change (`__source_ts_ms`).
  The namespace must be one level of letters, digits and underscores, and a Glue catalog signs in
  with the runtime's AWS role (no access keys). Catalog and storage credentials travel to the
  cluster as catalog properties, filled in by BeziSync's helper inside the pipeline's containers.

### Apache Paimon

**Engine:** Flink CDC.

One primary-key Paimon table per source table in a filesystem catalog on S3 or an
S3-compatible store (MinIO, RustFS, Ceph): the warehouse (`s3://bucket/path`), the database, the endpoint
and path-style access for S3-compatible stores, and an access key (or the runtime's own credentials).
Rows are written on each Flink checkpoint. Preflight writes and deletes a test object under the warehouse,
and blocks a destination that already exists without a primary key (`paimon_keyless_table`: an append
table can't take updates and deletes).
BeziSync has no Paimon reader: the Events tab has nothing to show and health stays Unknown (read the
tables with Flink SQL, Spark or Trino), and deleting a pipeline leaves its tables in place (the report says
how to drop them). Tables are not partitioned.

### Apache Fluss

**Engine:** Flink CDC.

Changes land in one primary-key table per source table in a Fluss database, created and evolved by
Flink CDC; Fluss can tier those tables into a lakehouse. The connection takes the coordinators'
`host:port` list and sign-in (none, or SASL PLAIN). BeziSync has no Fluss client: preflight checks
that a coordinator can be reached, the Events tab has nothing to show (read the tables with Flink
SQL or your own client), and deleting a pipeline leaves its tables in place (the report lists them as
kept). Fluss documents no TLS for Flink CDC: keep it in a network you trust.

### StarRocks

**Engine:** Flink CDC.

Changes land in one primary-key table per source table in a StarRocks database (`cdc` by default,
created on deploy if missing), written by Flink CDC with Stream Load; schema changes are applied to the
tables. The connection takes the FE host, its query port (9030,
MySQL protocol) and HTTP port (8030, Stream Load), a user and password, and the replicas of each
table (1 on a single-node cluster). Flink CDC's StarRocks connector carries its own MySQL JDBC driver
(Connector/J 5.1.49), so StarRocks needs no driver added to the image.

- **Column defaults** are not copied: Flink CDC passes them on as the source writes them
  (`nextval(...)`, `now()`, `'NEW'::character varying`), which StarRocks refuses. BeziSync writes
  every source column that has a default without it; the values the source filled in still arrive.
  Such a column arrives as `STRING` if it is text, and a timestamp one with millisecond precision.
  Because of that, new source columns are held back until the next deploy ("Send" is not offered).
- **Timestamps** arrive to the second: Flink CDC's StarRocks connector writes no fractions.
- **Preflight** signs in over the MySQL protocol and reads the user's `SHOW GRANTS` (it creates
  nothing): it passes when they show `CREATE TABLE` on the database, and otherwise says "not known"
  (the right may come from a role) and shows the `GRANT` statements the user needs. Every table needs a key
  (`flink_primary_key`). The **Events** tab reads rows back (Flink CDC pipelines run no delivery canary).
- **No TLS**: Stream Load and the MySQL protocol connection are plain text upstream. Keep StarRocks
  and the runtime in a network you trust.
- Deleting a pipeline with "drop destinations" drops its tables.

### Apache Doris

**Engine:** Flink CDC.

The same as StarRocks for Doris: one unique-key table per source table, written with Stream Load
(no TLS), read back and dropped over the MySQL protocol, and column defaults left out the same way
(new columns are held back until the next deploy). Doris needs no MySQL Connector/J. When the
BE addresses the FE hands out can't be reached from the runtime (Doris in Docker or behind NAT),
list the BEs under **BE nodes** (`host:HTTP port`) and Flink CDC loads into them directly.

### Elasticsearch

**Engine:** Flink CDC.

One index per source table (the naming pattern, dots as `_`, lower-cased), one document per row
whose id is the primary key: an update replaces it and a delete removes it. Indexes are created by
their first document with dynamic mappings (decimals arrive as text, NULL fields are left out); create
an index template beforehand for other mappings. The connection takes the hosts (`http(s)://host:port`),
the version (6, 7 or 8, matching the server) and an optional user and password. Preflight signs in and
compares the version; the pipeline check then asks Elasticsearch (`_has_privileges`, 6.4 and later)
whether the user may write and delete documents in the pipeline's indexes (`index`, `delete`) and
create the missing ones (`create_index` or `auto_configure`), and warns without `delete_index`. It
creates no index, and says "not known" when security is off or the question is refused. The sink takes no CA: use `https` only
with a certificate from a public CA, otherwise keep the cluster in a network you trust. The Events tab
reads documents, and deleting a pipeline with "drop destinations" deletes its indexes.

## Databases behind a bastion (SSH)

PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, CockroachDB and Vitess connections can reach a database through an SSH bastion
(jump host). Open **Show expert settings → SSH tunnel** on the connection and set the bastion host,
port and user, the private key (OpenSSH or PEM, no passphrase; stored encrypted and write-only,
or a secret-store reference) and — recommended — the bastion's **host key** from
`ssh-keyscan -t ed25519 <bastion>`. Host and port stay the database's address *as seen from the
bastion*.

- Checks, discovery and probes open a short-lived tunnel; the readiness checks report whether the
  bastion is reachable (`*_ssh`) and whether the tunnel works (`*_ssh_tunnel`, a warning while the
  host key is not pinned).
- Each connector gets a `bezisync tunnel` sidecar (Kubernetes: a native sidecar container; Docker:
  a companion container sharing its network) that keeps the forward open and reconnects; Debezium
  connects to `127.0.0.1:15432`. The bastion must allow TCP forwarding (`AllowTcpForwarding yes`).
- TLS still works through the tunnel, but the server name is then `127.0.0.1`: use `require` or
  `verify-ca`, not `verify-full`.
- MongoDB is not supported (replica-set members announce their own host names); use a network
  route or an agent runtime inside the network instead. Spanner is a Google API, not a host.
- Vitess tunnels both VTGate ports: gRPC for the connector, MySQL for the checks.

## Operate

The pipeline page opens on **Overview**: a flow strip (source → connector → destination) with the
log kept on the source for this pipeline (for MongoDB, how far back the shared oplog reaches), the
connector's state, lag, throughput, Debezium version and runtime, and what is written at the
destination; then a full-width throughput and latency chart (the latest values, or the values at
the point you hover, and the peak over the range), counters since the connector started, and
**source protection** in the source's own terms beside the deployed **transforms** and end-to-end
delivery:

- **Source protection** — PostgreSQL: the replication slot, its WAL status and the room left before
  `max_slot_wal_keep_size` invalidates it ("No cap" when that is -1); MySQL: the binlog file the
  pipeline reads and whether the next purge would remove it; MongoDB: the oplog window and how full
  the oplog is; SQL Server: how long CDC keeps changes (the cleanup job's retention), whether the
  pipeline's position is still inside that window, and the transaction log's fill; Oracle: how far
  back the archived logs reach; CockroachDB: the tables' GC TTL; Spanner: the change stream's
  retention — each against how far the stored position is behind, with a bar of the window used;
  Vitess: *Unknown* (binlog retention is not visible through VTGate). For PostgreSQL and MySQL a bar shows the log kept for this pipeline against the
  pipeline's *source retained limit* policy (10 GB by default). Anything the probe could not
  measure says *Unknown* or *Not measured yet*, never fine.
- **Transforms** — what consumers receive (the row after the change or the full envelope, how
  deletes arrive, the extra fields with their names, value formats and custom message keys), then
  the steps that change events on the way (filters, renames, added fields, …). Side by side, the
  card is as tall as the cards beside it and scrolls when the steps need more room.

The other tabs are **Tables &
schema**, **Events** (read back from the destination, masked), **Logs**, **Snapshots** (re-copy
rows and the stored position) and **History** (versions, the generated configuration, Export YAML,
Promote, Save as template, and the activity). The header's one primary button follows the state
(Continue setup, Review and deploy, Resume, Restart); **Operate**, **Change** and **More actions**
hold the rest.

- **Pause / Resume / Restart** (Operate) — pausing keeps the source retaining changes; watch source protection.
- **Change** — edit one part (tables, destination, transforms, settings); it becomes one new version (every save while you
  edit updates that version, until it is deployed or sent for approval) and Review shows the diff and what deploying will do (added tables are back-filled with an incremental
  snapshot; some changes restart the connector).
- **Rollback** — deploy any earlier version from History; the confirmation lists the restart, the
  changes and whether approval is needed.
- **Discard** — a change you saved but don't want to deploy: the "vN is saved but not deployed"
  notice has **Discard vN**, and closing the wizard asks whether to keep the change or discard it.
  Settings go back to the deployed version; the connector keeps running and nothing restarts, and
  no approval is needed. The discarded version stays in History (deploy it from there to bring it
  back), and the next change gets a new number. Description, owner, labels and the runtime aren't
  part of a version, so they stay as they are. Editing the settings back to exactly what is deployed
  does the same. Not offered while a change waits for approval: cancel or decide the request
  first. Scripts and agents use `bezisync remote pipelines discard NAME` or the MCP tool
  `discard_pipeline_changes`.
- **Snapshots** — re-copy selected tables without stopping streaming (sources that can't, Vitess
  and Spanner, say so instead): search and page through the
  captured tables, see their rows and size and a rough duration; large copies (over 10 tables or
  5 million rows) warn and ask to confirm. With a **maintenance
  window** (e.g. `22:00-06:00` UTC, set on the review step), backfills and re-syncs wait for it to
  spare the source; "Run now" overrides it. The first copy on deploy is not delayed.
- **Delivery canary** (opt-in on the review step) — every few minutes BeziSync writes a tiny row to
  `bezisync.canary_<id>` on the source and looks for it at the destination, showing "verified N
  minutes ago, X s end to end" and firing "Delivery not verified" when a canary does not arrive in
  time (`BEZISYNC_CANARY_INTERVAL_SECONDS`, `BEZISYNC_CANARY_TIMEOUT_SECONDS`). Needs a PostgreSQL,
  MySQL, Oracle (`BZ_CANARY_<id>` in the connector user's schema) or CockroachDB source and a destination BeziSync can read back (every one but HTTP and SQS); it adds
  one small canary topic, stream or table named like the others. Flink CDC pipelines have none:
  they capture only the pipeline's tables, so a canary row would never arrive (preflight says so);
  their health comes from the job and its saved positions.
- **Schema** — BeziSync compares the columns of captured tables every few minutes
  (`BEZISYNC_SCHEMA_CHECK_SECONDS`) and lists each change. Dropped columns, type changes and
  missing tables are *breaking* and fire the "Breaking schema change" alert until someone marks
  them reviewed; added columns and nullability changes are compatible. (MongoDB has no fixed schema.)
- **Stored position** (Snapshots tab) — see the exact position; reset it (re-snapshot or skip to
  now) while paused, with typed confirmation.
- **Alerts** — rules (down, lagging, source at risk, offset regression, breaking schema change,
  delivery not verified…) notify Slack, webhooks, PagerDuty, email and the in-app inbox. Each person
  chooses which events reach their inbox (account menu → Notification settings).
- **Find anything** — press ⌘K / Ctrl+K to jump to a pipeline, connection, page or setting.
- **Delete** (More actions) — removes the connector and everything created on the source (slot,
  publication, BeziSync tables, offsets) with a per-item report. Keeping the destination topics or
  streams is the default; deleting them is an explicit choice.

## Manage runtimes

**Settings → Runtimes** (Admins; pick the project at the top: it changes which project the section shows, not the project you are in) adds Kubernetes namespaces and Docker hosts with **Add runtime**. Each runtime's
**More actions** menu has:

- **Edit** — name, settings, credentials (write-only: leave a secret empty to keep it) and the
  default Debezium image. Changing where a runtime points needs its running pipelines paused first.
  Pipeline manifests refer to runtimes by name, so update them after a rename.
- **Placement profiles** (Kubernetes, in Edit) — named places pipelines may run: a node selector,
  tolerations, a priority class and pod labels, taken as they are (no `${…}` or `{{…}}`; Kubernetes
  label names; BeziSync's own labels can't be set). A pipeline picks one in Review (`spec.placement`)
  and keeps the profile it deployed with until its next deploy, so editing a profile restarts
  nothing; one in use can't be removed (the error names its pipelines). Preflight's `placement`
  check blocks a profile no schedulable node takes, or an agent too old to apply it, and a custom
  Flink CDC cluster is packed for the largest node the profile can use. Docker runtimes have none.
- **Disable / Enable** — disabling *cordons* the runtime: pipelines already on it keep running and
  can still be paused, resumed, restarted and snapshotted, but nobody can create, move or deploy
  pipelines onto it (a blocking `runtime_disabled` check). Both are audited.
- **Delete** — once no pipeline uses it (move or delete them first) and deleted pipelines finished
  cleaning up; their history stays. Deleting an agent runtime revokes its token.

On a Docker runtime, BeziSync downloads a Debezium image the host does not have yet in the
background: the pipeline shows **Downloading the connector image…** (or, when it replaces a
running connector, keeps streaming on the old version with a note) and starts once the download
finishes. **Image download timeout** (10 minutes to 2 hours, default 30 minutes) bounds one
download; a Docker agent takes it from `BEZISYNC_AGENT_DOCKER_IMAGE_PULL_TIMEOUT` (the install
command sets it). On Kubernetes the node downloads images; a failed attempt only fails the
pipeline once Kubernetes gives up retrying (`ImagePullBackOff`).

The image a pipeline runs is fixed when it is deployed: changing a runtime's (or the install's)
default image never restarts running pipelines. They pick it up at their next deploy, which their
pipeline page shows as "next deploy".

## Remote runtimes (agents)

Add a runtime with **Through an agent**: BeziSync shows a one-time install command (closing it needs
"I've copied it", since the token is never shown again; Helm chart
`deploy/helm/bezisync-agent`, or `docker run` of the `bezisync` image). The agent runs next to the runtime and only makes
outbound HTTPS requests to BeziSync — no inbound firewall rules and no runtime credentials in
BeziSync. If the agent disconnects for 30 seconds, its pipelines show **Unknown** (never Healthy)
with the reason. "New agent token" (More actions) rotates the token; the old one stops working immediately.

## Google GKE and Amazon EKS

**Use an agent.** Install the agent chart into the cluster (the runtime's install command); it uses
its own in-cluster ServiceAccount and only dials out, so it works with private GKE and EKS clusters
and needs no cloud IAM sign-in or kubeconfig in BeziSync.

**Direct mode** (BeziSync talks to the cluster API itself, which must be reachable): untick
"Use BeziSync's in-cluster service account" and choose how to sign in.

- **GKE** — API server endpoint and cluster CA certificate (both in the cluster's details), and a
  Google service-account key with *Kubernetes Engine Developer*, or nothing when BeziSync itself runs
  with Workload Identity. BeziSync mints and refreshes short-lived access tokens.
- **EKS** — endpoint, CA certificate, cluster name and region, and AWS keys, or nothing when BeziSync
  runs with IRSA / an instance role. BeziSync creates the same `k8s-aws-v1.` tokens as
  `aws eks get-token` (15 minutes, refreshed). Map that IAM identity to a Kubernetes user or group
  with an access entry (or `aws-auth`) bound to the Role below.
- **kubeconfig** — any kubeconfig with a static ServiceAccount token (exec plugins such as
  `gke-gcloud-auth-plugin` are not available in the image).

The identity needs the Role from the agent chart in the connector namespace: Deployments,
ConfigMaps, Secrets, Services (Flink CDC pipelines only), pods, logs, `pods/proxy` and reading
ResourceQuotas (so preflight can tell a deploy the namespace's quota would refuse; both charts grant
it from 0.2.1).

Around the cluster:

- **Databases:** choose the AWS RDS/Aurora or GCP Cloud SQL/AlloyDB preset on the source; checks
  and fixes then use the provider's settings (`rds.logical_replication`, `cloudsql.logical_decoding`, …).
- **Secrets:** AWS Secrets Manager and Google Secret Manager stores work with keys, or with the
  control plane's pod identity (IRSA / Workload Identity) when the credentials are left empty.
- **Destinations:** Kinesis, SQS, SNS, Iceberg (S3, Glue, S3 Tables) and Pub/Sub use keys, or the connector pod's own identity: set the
  runtime's *connector service account* to a ServiceAccount bound to IRSA
  (`eks.amazonaws.com/role-arn`) or Workload Identity (`iam.gke.io/gcp-service-account`), and leave
  the keys empty.
- **Images:** connector pods pull `quay.io/debezium/server`; for clusters without that egress,
  mirror it to Artifact Registry / ECR and choose it as the runtime's default image (or under
  Settings → Engine versions).

**Checks run where connectors run.** For pipelines on an agent runtime, the final checks before a
deploy run on the agent, inside the cluster's VPC, so a database reachable only from there passes
(the sections say "checked by the agent"). If the agent cannot answer, BeziSync checks from its own
network and says so. Checks on the Connections page always run from BeziSync.

## Engine versions

**Settings → Engine versions** (Admins) lists the images each engine runs, on two tabs: **Debezium**
and **Flink CDC**. On the Flink CDC tab Admins add, label, deprecate and remove images and choose
the default, which pipelines that pin no image (`flinkCdcImage` in YAML) run from their next deploy;
running pipelines are not restarted. Flink CDC is off until the tab lists an image (see [Flink CDC
engine](#flink-cdc-engine)). Version counts, the version column and bulk moves are for
Debezium pipelines.

The Debezium tab is the one list of Debezium Server images every project picks from, with how many pipelines run each one per project (each count links to that project's
Pipelines list filtered to that version). The install default (`BEZISYNC_DEBEZIUM_DEFAULT_IMAGE`) is always listed;
add releases or your own builds (extra converters, SMTs or drivers) with a label and notes, make one
the **default** (it overrides the install default; "Use install default" goes back), deprecate or
remove images. The catalog is advice, not enforcement: pipelines on a deprecated or unlisted image
keep running and get a non-blocking `debezium_image_deprecated` / `debezium_image_unlisted` check.
A new default never restarts running pipelines: they show it as their next deploy.
Destinations the official images lack run their own Debezium Server build, pinned by digest:
Apache Iceberg pipelines run `ghcr.io/memiiso/debezium-server-iceberg` (built on Debezium 3.6,
amd64 only), whatever the runtime or catalog default; the Pipelines list shows it as "Iceberg sink
<version>". A pipeline may pin another tag of that build, never an official image (`422
image_family`). Point `BEZISYNC_ICEBERG_IMAGE` at a mirror for private registries. Preflight blocks
a runtime whose nodes are all another architecture and warns when only some are
(`sink_image_platform`).

Making an image the default (or going back to the install default) asks first and says how many
live pipelines run the current default.

Upgrading is pipeline work, done by editors where pipelines live. The **Pipelines** list has a
*Debezium* column and version filter; select pipelines and **Change Debezium version…** to move them
to an offered version, any other image, or back to the default (pipelines that already follow the
default are redeployed when it changed). A pipeline's flow strip shows the version it runs and why
(chosen for this pipeline, the runtime's default, or the default) with a **Change** button. Change
one first, watch it reach Healthy, then the rest. Each change is a new version, so rollback is
deploying the previous one. Before you confirm, the dialog says what the release does to each
pipeline's settings: a tuned setting it doesn't have refuses that pipeline's change until you remove
it, a renamed one moves to its new key, and raw properties are listed as not checked.

## Flink CDC engine

<a id="flink-cdc-engine-optional"></a>

Flink CDC is the second engine a pipeline can run on, next to Debezium: it reads MySQL/MariaDB,
PostgreSQL, SQL Server and Oracle and keeps their tables in sync in Iceberg, StarRocks, Doris, Fluss,
Elasticsearch, Apache Paimon, OceanBase and Kafka. It copies large tables in parallel chunks, applies
schema changes to the destination tables and keeps saved positions you can restore. Each pipeline
runs its own small Flink cluster (a JobManager and TaskManagers), so it needs more CPU and memory
than a Debezium pipeline. [Choose an engine](#choose-an-engine) compares the two.

**Set it up: register an image.** An Admin makes Flink CDC available by registering a Flink CDC
image under **Settings → Engine versions → Flink CDC** (or `BEZISYNC_FLINK_CDC_DEFAULT_IMAGE`).
BeziSync publishes no Flink CDC image: build one on the official Flink image (`flink:2.2.1-java17`)
with Flink CDC 3.7.0 in this layout, and push it to a registry your runtimes can pull from. Until an
image is registered the wizard shows the Flink CDC-only destinations as unavailable with the reason,
and Iceberg and Kafka pipelines run on Debezium.

| Path | What |
|---|---|
| `/opt/flink` | Flink 2.2.1 (the base image); add `flink-shaded-guava-31.1-jre-17.0.jar` to `/opt/flink/lib` (Flink CDC on Flink 2.2 needs it) |
| `/opt/flink-cdc/lib/flink-cdc-dist-3.7.0-2.2.jar` | Flink CDC's runtime, from the Flink CDC 3.7.0 release |
| `/opt/flink-cdc/connectors/<name>.jar` | Flink CDC's pipeline connectors (`flink-cdc-pipeline-connector-*-3.7.0-2.2.jar`), renamed: `mysql`, `postgresql`, `sqlserver`, `oracle`, `iceberg`, `starrocks`, `doris`, `fluss`, `kafka`, `elasticsearch`, `paimon`, `oceanbase`; for Paimon also `paimon-s3` (Paimon 2.0.0) and the Hadoop JARs below; for OceanBase also `oceanbase-client` (OceanBase Connector/J 2.4.14, LGPL); for Iceberg also `iceberg-aws` (`iceberg-aws-bundle` 1.10.1), `hadoop-client-api` and `hadoop-client-runtime` (3.3.6) and `commons-logging` (1.3.5) |
| `/opt/flink-cdc/drivers/` | JDBC drivers the connectors leave out (see below) |

Only the connectors your pipelines use have to be there: a pipeline whose connector is missing
fails at start and says which file it expected. End the image with `USER flink` (uid 9999, the base
image's user): Kubernetes runtimes run it as that uid, Docker runtimes as the image's user, both on a
read-only root filesystem, so these files must be readable by it.

MySQL/MariaDB sources need MySQL Connector/J, which Flink CDC leaves out of its MySQL connector for
licence reasons (it is GPL-licensed): copy `mysql-connector-j-<version>.jar` into
`/opt/flink-cdc/drivers` (its licence is then yours to accept). Every other source and destination
works without it; Flink CDC's StarRocks connector carries its own copy of the driver (Connector/J
5.1.49).

A Docker runtime pulls images anonymously: for a private registry, pull the image on the Docker host
first. Kubernetes runtimes use the runtime's image pull secrets.

The image holds Flink and Flink CDC only; BeziSync copies its own helper into each pipeline at
start, so an image keeps working across BeziSync upgrades. Scan the image you run with your own
tools: Flink CDC's connector JARs bundle libraries (Jackson, Netty, protobuf and others) with
published vulnerabilities that only a Flink CDC release can update. Projects can limit which
engines their pipelines use (**Policies → Allowed engines**).

**Choosing it.** The wizard's destination step shows the engine: StarRocks, Doris, Fluss, Elasticsearch,
Paimon and OceanBase run on Flink CDC; Iceberg and Kafka offer both (Debezium is the default). The engine can change until the pipeline's
first deploy; after that, create a new pipeline. On Flink CDC:

- **Tables** need a primary key, or key columns picked in the Transform step, which can include added and computed columns (`flink_primary_key`);
  PostgreSQL tables need `REPLICA IDENTITY FULL` (`flink_replica_identity`, with the SQL to run).
  MySQL readers get a block of server ids no other pipeline on that source uses
  (`flink_server_ids`). Oracle has its own list (see *Oracle on Flink CDC* below).
- **Columns** can be removed or kept, not masked (Flink CDC has no hash function). Filters built
  from rules, renames and added fields work; script filters, outbox, flattening and custom steps are
  Debezium only, and so are value formats (decimals, timestamps, binary): each column keeps its own
  type.
- **Settings** (review step): what a change of a source table's shape does (*apply*: new and renamed
  columns are added, nothing is dropped; *ignore*; *stop*), whether new columns are held back until
  the next deploy (default) or sent at once, how often the position is saved (and, for Iceberg, data
  committed: 30 s, 1 min or 5 min) and, for StarRocks and Doris, how long a load batch may wait (2, 5
  or 30 s). Expert mode adds Flink settings from a fixed list (checkpoint timeouts, restart
  strategy, memory fractions, timeouts), each with its range; BeziSync keeps the rest. Engine
  settings also offer the connectors' options: the sources' connect timeout and attempts (MySQL,
  PostgreSQL), PostgreSQL's WAL release delay, copying the open-ended chunk first (experimental: a
  pill, and a preflight warning), StarRocks' load batch bytes and threads, Iceberg's target file
  size and the schema change timeout. **Advanced Flink CDC options** pass through only the
  connectors' own prefixes — `source.jdbc.properties.*` (unsafe driver options refused, as for
  Debezium), `source.debezium.*` (Debezium's rules), and the destination's `sink.properties.*`,
  `table.create.properties.*` and `table.properties.*` (not the Iceberg keys that would move or
  rewrite a table); catalog and Hadoop options, bare Flink keys and anything BeziSync sets are
  refused, and values are written by the YAML encoder.
- **Size**: small runs one TaskManager (2 readers), medium two (4), large four (8); each
  TaskManager asks for 1 CPU and 2 GiB, the JobManager 1 to 1.5 GiB. A **custom cluster**
  (`policies: {resources: custom}` with `flink_cdc: {cluster: {parallelism, slot: {cpu, memory},
  job_manager: {cpu, memory}}}`) names how many readers run (1 to 128) and what each reader's slot
  needs (CPU 0.25 to 4, memory 512 MiB to 8 GiB) — in Review → Size → Custom (Expert mode), which
  starts from the size the pipeline had and draws how the cluster would be packed now, with why it
  splits and its total against the largest node; when it deploys, BeziSync packs the slots into the
  fewest TaskManagers the runtime's largest node holds (at most 8 slots each, plus 512 MiB per
  TaskManager) and keeps that shape until the next deploy, so nodes coming and going never restart
  it. `shape: manual` with `task_managers: {count, slots, cpu, memory}` sets the TaskManagers
  yourself. `flink_cluster_fit` blocks a cluster whose slot or TaskManager is bigger than any node,
  even when forced, and is *unknown* when the runtime doesn't report its nodes. A deployed pipeline
  can change its TaskManagers, slots, CPU and memory: the deploy saves the position, recreates the
  cluster and restores it. Its number of readers stays while a saved position is left to restore
  (the position is kept per reader): `flink_readers_changed` blocks that deploy, even when forced,
  until you pause it and use Positions → Start over. **Shrink after the first copy**
  (`streaming_slot: {cpu, memory}`, smaller than `slot`; automatic shape only): streaming reads the
  log with one reader, so once every table is copied and a position was saved after streaming
  began, the controller repacks the cluster for the streaming size — a restart from that position,
  audited as `controller`, shown on the Overview's cluster card. It grows back to the copy size
  before copying again (Start over that copies every table, restoring a position saved during the
  copy, a deploy that adds tables). If the streaming shape fails before it saves a position it goes
  back to the copy size; after three such failures BeziSync stops shrinking it (alert *Cluster
  stopped shrinking*). With the project policy `shrink_after_copy: suggest` the Overview offers
  **Shrink now** instead. Preflight and project limits count the copy size, the peak. Preflight checks the runtime has room
  (`runtime_capacity`). Every reader copies in parallel, but streaming reads the binlog or
  WAL with one. On PostgreSQL each reader opens its own temporary replication slot while it copies,
  so a first copy needs 1 + readers free slots and WAL senders: `flink_replication_slots` blocks a
  deploy that would run out part-way. Kubernetes runtimes need the Role to allow Services
  (`flink_kubernetes_services`; both charts grant it from 0.2.0), and agent runtimes an agent of this
  BeziSync version (`flink_agent_version`).

**Oracle on Flink CDC.** Flink CDC 3.7 reads Oracle through its own copy of Debezium (1.9), so it
asks more of the database than Debezium Server does, and reads less:

- **Releases:** Enterprise, Standard or Express Edition, multitenant or not, up to 23ai. Oracle
  Database **Free** (every release) and **Oracle AI Database 26ai** can't be read: preflight blocks
  them (`flink_oracle_version`, even when forced). Run those pipelines on Debezium. A database that
  moves to 26ai later stops its Flink pipeline at the next restart, and health says why. Measured end
  to end on Express Edition 21c; Enterprise Edition passes the same version rule.
- **Grants:** what Debezium needs (the checks give the script), plus **`ANALYZE ANY`** in the PDB:
  each first copy runs `ANALYZE TABLE … COMPUTE STATISTICS` on every table before it splits the
  copy into chunks. That reads the table in full and **replaces its optimizer statistics**, so query
  plans may change. Preflight warns before every deploy that copies, blocks when the right is
  missing (`flink_oracle_analyze`, with the `GRANT` to run in the PDB) and blocks tables whose
  statistics are locked (ORA-38029).
- **Multitenant:** BeziSync keeps `LOG_MINING_FLUSH` (one row) in the connector user's schema of
  the **CDB root**, where LogMiner runs, the table Debezium Server's Oracle connector also creates
  there. The user needs `CREATE TABLE` and a quota in the root, or a DBA creates it
  (`flink_oracle_flush_table` gives the SQL). It is kept when pipelines are deleted, because every
  pipeline mining as that user shares it; drop it with `DROP TABLE <user>.LOG_MINING_FLUSH` once
  none remain. Debezium 1.9 also leaves an empty copy in the PDB. Managed services that allow no
  objects in the root can't run Oracle on Flink CDC.
- **Tables:** names must be upper case (Oracle's unquoted names); quoted lower- or mixed-case names
  are refused (`flink_table_name`). There is no signal or canary table on Flink CDC.
- **Schema changes:** added columns reach the destination. **Renaming or dropping a column stops
  the pipeline**: LogMiner can no longer rebuild the changes made before it ("Oracle failed to
  re-construct redo SQL"), and health says so (`oracle_schema_change`). Pause it and use Positions →
  Start over. Choose Debezium for tables whose columns get renamed.
- **Types:** a bare `NUMBER` is written as `DECIMAL(38,19)` (more than 19 digits before the decimal
  point don't fit; more after it are rounded); `NUMBER(p,0)` up to 18 digits as `BIGINT`, `DATE` as a
  timestamp.
- **Positions:** Start over copies every table again or reads from now; there is no start from a
  time. **Adding tables** to a deployed pipeline needs a Start over first: the reader never copies a
  table added to a pipeline that restores a saved position, so preflight blocks that deploy
  (`flink_tables_added`, even when forced) and a position saved before tables were added can no
  longer be restored. Capture new tables in a second pipeline to avoid the full copy.
- **Source protection:** the Overview shows how long the archive keeps redo, but not how far behind
  a Flink pipeline is: its position is inside its encrypted saved state.
- `source.jdbc.properties.*` are refused (the reader ignores them); `source.debezium.*` take
  Debezium 1.9's names.

**Other sources.** Flink CDC 3.7 has pipeline sources only for MySQL, PostgreSQL, SQL Server and
Oracle. MongoDB, Vitess and TiDB have none (only Flink SQL readers, which can't apply schema changes
or routes), and its OceanBase connector writes rather than reads, so these stay on Debezium (TiDB and
OceanBase have no BeziSync connection type).

**Changing columns.** Iceberg and Paimon write a row's values into a table by position, and Flink CDC
3.7 never changes a table that already exists, so a job writing other columns than its table has would
put values into the wrong columns. BeziSync keeps that from happening:

- **While the pipeline runs**, columns the source adds, renames, widens or drops reach the destination
  as Flink CDC applies them (with *Apply them*): an added column at the end, a renamed one as a new
  column with the old one kept empty, a dropped one kept empty.
- **A version that changes the columns the pipeline writes** (a step that adds, computes or removes a
  column, keeping deleted rows, leaving a column out, rolling back to a version with other columns)
  switches columns at the saved position: once the running job has stopped with a savepoint, BeziSync
  adds the new columns to the destination tables (planned as below) and the job resumes from exactly
  there writing the new columns. Nothing is copied again and no change the source made is lost; rows
  written before keep the columns they had (a new column is empty there). Preflight says so
  (`flink_columns_changed`). To fill the new columns for the old rows too, Start over and copy the
  tables again. A source column added after the last saved position (while the pipeline was stopped)
  isn't written at a switch on a table whose order BeziSync fixed: it waits for the next deploy.
- **A column added while the pipeline was stopped.** Flink CDC learns a table's new column only when it reads
  the change that added it, so a deploy that names it before the resumed job got there would fail the job.
  When that happens BeziSync holds the column back (`column_not_read`, audited `pipeline.columns_held`) and
  resumes; the next deploy writes it, switching columns at the saved position.
- **A start without a saved position** (the first one, one after Start over) is planned: BeziSync reads
  every destination table that exists, writes its columns in the table's order (a column the pipeline
  no longer writes stays, empty for new rows), and adds the columns a table lacks at its end before the
  start (Iceberg, StarRocks, Doris and OceanBase; Paimon tables you change yourself: preflight gives the
  statement). It refuses a start that would empty a key, partition or NOT NULL column, needs a type it
  can't name, or whose table it can't read (`flink_destination_columns`). Where it had to fix the order,
  new source columns wait for the next deploy. BeziSync can't read Fluss tables: preflight warns that an
  existing one drops the columns it doesn't have.
- **The helper never starts fresh on its own.** With nothing to restore and no planned start (a crash
  before the first saved position, state too large to save), the pipeline stops with `no_position`
  instead: Restore a saved position or Start over.
- A saved position from before the last Start over can't be restored (`position_before_layout`): its job
  wrote other columns. One from before a column switch can: it resumes with the new columns too.
- **StarRocks** reserves `__op`, so *Keep deleted rows* writes the change type into `__bz_op` there.

**Operating it.** The Overview shows the cluster: the job, its JobManager and TaskManagers, the
last saved position and where the pipeline started from. Health comes from the job itself and is
never green before it runs and reads changes. Logs choose the JobManager or a TaskManager. Pause, resume, restart, new versions and rollback work as for Debezium: each stop saves
the position first and the next start resumes from it. The **Positions** tab (instead of Snapshots)
lists the saved positions (encrypted in the state database; positions set aside by a restore or
start over, and all but the newest 10 savepoints, are removed after 7 days) and, while the pipeline is paused, can **Restore** an earlier one (the job reads the changes since that position again: keyed tables are upserted, tables without a key get those rows twice; one from before a source column change the destination already has fails with `schema_change_replayed`, a Flink CDC 3.7 limit) or **Start over**: copy every table
again, read from now, or read since a time (MySQL and SQL Server). Re-copying selected tables is
not available: start over instead. Deleting the pipeline removes its saved positions and, for
PostgreSQL, every replication slot it created.

**Scripts and agents.** Manifests carry `spec.engine: flink-cdc`, the `flink_cdc` settings and an
optional `flinkCdcImage`; exports write `engine` only for Flink CDC pipelines. `bezisync remote get`
has an ENGINE column; `bezisync remote pipelines positions NAME`, `… restore NAME POSITION_ID --yes`
and `… start-over NAME --from-scratch|--from-now|--since TIME --yes` operate positions. MCP adds
`list_engines`, `list_saved_positions` and `restore_saved_position`, `reset_pipeline_offsets` takes
`from_time`, and pipeline logs take a component.

```yaml
spec:
  engine: flink-cdc
  runtime: prod-k8s
  source: {connection: orders-db, tables: {include: [shop.orders]}}
  sink: {connection: warehouse-starrocks, naming: "{schema}_{table}"}
  flink_cdc: {schema_changes: apply, new_columns: hold, commit_seconds: 60, freshness_seconds: 5}
  policies: {resources: medium}
```

## Personal data

Each pipeline decides what happens to personal data, in the **Tables** step (and later from
**Change → Tables and columns** or the Data tab's **Tables and personal data**). When you pick a
table, every column whose name looks like personal data (email, phone, address, birth date, card,
IBAN, passport, tax or national ID numbers) is protected at once, and the **Personal data** card
lists them with one choice each:

- **Mask**: replaced by a salted SHA-256 hash, unique to the pipeline, so equal values still match
  for joins. Debezium hashes **text columns only**, so Mask is offered only for those (MongoDB can't
  hash at all). Flink CDC has no hash function, so a Flink CDC pipeline offers Remove and Keep only.
- **Remove**: left out of every event. Columns that can't be hashed are removed by default.
- **Keep as is**: sent unchanged, on purpose; the choice is recorded in the spec (`keep`).

**Protect another column** masks or removes any other column of the captured tables; **Choose for
all** sets every row at once. Choices you made are never changed when you come back to the step.

Final checks (Configuration) back this up: a masked column that can't be hashed **blocks** the
deploy (it would leak), personal-data columns nobody decided on (pipelines made through YAML, the
API or MCP) are a **warning** naming them, and a rule on a column that was renamed or dropped is a
warning. A new column that looks like personal data shows as **looks like personal data · sent as
is** in the Data tab's schema changes until you choose. In YAML:

```yaml
columns:
  public.customers: {mask: [email], exclude: [birth_date], keep: [phone]}
```

Earlier releases had a project policy, "Require masking personal data"; it was replaced by these
per-pipeline choices and is accepted but ignored when sent to the API.

## Guardrails: policies, approvals, templates

**Settings → Policies** (Admins, per project): allowed runtimes, source/destination types and snapshot
modes; **Resources** — the largest preset size (a custom size is held to its CPU and memory, a
custom Flink CDC cluster to the totals of the Flink CDC preset of that name), the most CPU, memory
and readers one pipeline may ask for at its peak (`resource_limits: {max_cpu, max_memory,
max_parallelism}`; a Flink CDC cluster's peak is its first copy; no lower than Small, or nothing
could deploy) and whether clusters shrink after their first copy or only offer Shrink now;
**Setting limits** — per typed setting, at most or at least a value, only some choices, always a
value (written into each pipeline's version when it deploys; Review shows it as set by project
policy), or hidden; checked against the setting when you add it, with the pipelines it would block
counted before you save; a pipeline over a limit keeps running and its next deploy is blocked
(`policy_setting_limit`), Promote's plan names what a target project's limits refuse, and a limit on
a setting BeziSync no longer offers is shown obsolete and ignored; pipelines per source (each
holds a replication slot or log reader); whether "Deploy anyway" exists; whether deploys need
**approval**; which transform types pipelines may use; and whether **custom transforms** (any SMT
class in the image) and **scripted transforms** (Groovy/JavaScript filter scripts, which run next to
the connector's credentials) are allowed, and whether **advanced Debezium properties** (raw
properties BeziSync can't verify) are allowed — all three are off by default. Upgrading to the
release that added the advanced-properties policy turned it on for every project that already had a
pipeline using them, with an audit entry, so nothing that deployed before is blocked. The section groups these by what
you decide, refuses an empty allow-list, and before you save lists the running pipelines whose next
deploy the change would block. The wizard applies policies up front (disallowed options stay visible
and say why); the server enforces them at save and deploy.

If a project's stored policies can't be read (for example a value an older or newer release wrote
that this one refuses), BeziSync fails closed: the page lists what couldn't be read, those fields
show their strictest setting, the rest keep what was saved, and every change to the project's
pipelines (saving a changed spec, creating, deploying, even with Deploy anyway) is refused with
`policy_unreadable` until an Admin fixes the fields and saves. Running pipelines keep running, and
pause, resume and delete still work.

With approval required, deploying (including rollbacks and image moves) creates a request with the
preflight result; another member with approval rights approves it from the pipeline page (preflight
runs again) or rejects it with a comment. Nobody approves their own request.

**Templates** (History → Save as template) capture a destination, runtime, format, transforms, size
and snapshot mode — never tables, column rules, message keys or steps limited to some tables — so
teams start from agreed settings.

## Secrets and secret stores

Connection secrets are encrypted (AES-256-GCM), write-only and never returned or logged. To keep
them in your own vault, an Admin adds a store under **Settings → Secret stores** (HashiCorp Vault
KV v2, AWS Secrets Manager, Google Secret Manager, Kubernetes Secrets). Then every secret field of a
source or destination offers **Type it** or **From a secret store**:

1. **Store** — the project's stores, with whether each is connected.
2. **Secret** — listed from the store (searchable; at most 1,000, beyond that or when the store's
   credentials may read but not list, you type the path).
3. **Key** — the secret's key names, pre-picked when one is named like the field (`password`,
   `pwd`, `secretAccessKey`, …) or is the only one. A plain-text secret (AWS, Google) has no keys
   and is used whole.

The reference is checked as you pick, and the dialog saves only once it resolves (**Save anyway**
is there for a store that isn't reachable from BeziSync yet; the deploy checks block it if it stays
broken). BeziSync stores only the reference — `ref+<store>://<path>#<key>`, e.g.
`ref+vault://cdc/orders-db#password` — and reads the value when a connector needs it. Values never
reach the browser: listing returns secret names and key names only. The API and MCP tools take the
same reference string; "Try a reference" on a store's card confirms one without showing the value.

Each store's card shows what it is set to and which credentials are saved (with when they were last
replaced, never their values). Adding a store and **Edit** both test the settings before saving: if
the test fails, nothing is saved, unless you choose **Save anyway** (for a vault that is not
reachable yet). A store's name can't change, because connections refer to it.

What each store needs:

- **Vault** — KV version 2; read on the secrets, and `list` on the mount's `metadata/` paths for the
  picker to list them.
- **AWS Secrets Manager** — `secretsmanager:GetSecretValue`, plus `ListSecrets` for the picker; keys,
  or the control plane's IRSA / instance role when left empty. *Endpoint* (Expert) takes a VPC
  endpoint or an emulator.
- **Google Secret Manager** — *Secret Manager Secret Accessor*, plus `secrets.list` for the picker; a
  service-account key, or Workload Identity when left empty. *Endpoint* (Expert) takes a private
  endpoint or an emulator; with no key, BeziSync sends no sign-in, as emulators expect.
- **Kubernetes Secrets** — `get` and `list` on Secrets in one namespace. **This cluster** uses
  BeziSync's own service account (the chart grants it in runtime namespaces). **Another cluster**
  asks for a kubeconfig whose user has a static ServiceAccount token; sign-in plugins (`exec`, such
  as `gke-gcloud-auth-plugin`, or auth providers) can't run in BeziSync. Paste it or **Load from a
  file**. Its API server goes through the same network guard as every other store. To make one:

```bash
NS=cdc-secrets
kubectl -n $NS create serviceaccount bezisync-reader
kubectl -n $NS create role bezisync-reader --verb=get,list --resource=secrets
kubectl -n $NS create rolebinding bezisync-reader --role=bezisync-reader --serviceaccount=$NS:bezisync-reader
kubectl -n $NS apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata: { name: bezisync-reader-token, annotations: { kubernetes.io/service-account.name: bezisync-reader } }
type: kubernetes.io/service-account-token
EOF
SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
CA=$(kubectl -n $NS get secret bezisync-reader-token -o jsonpath='{.data.ca\.crt}')
TOKEN=$(kubectl -n $NS get secret bezisync-reader-token -o jsonpath='{.data.token}' | base64 -d)
cat > bezisync-kubeconfig.yaml <<EOF
apiVersion: v1
kind: Config
current-context: bezisync
clusters: [{ name: c, cluster: { server: $SERVER, certificate-authority-data: $CA } }]
contexts: [{ name: bezisync, context: { cluster: c, user: reader } }]
users: [{ name: reader, user: { token: $TOKEN } }]
EOF
```

  The `server` must be an address BeziSync can reach. Delete the local file once it's saved in
  BeziSync.

Kubernetes **runtimes** ask the same way: their kubeconfig, GKE and EKS fields appear only for the
sign-in chosen under **Another cluster**.

### Copy to another project

A connection's **⋯ → Copy to project…** (or several selected rows) copies sources and destinations
into another project where you manage connections; the originals stay. The dialog shows the plan
first: a name the other project already uses is copied as `name-2`, or **Update the existing one**
replaces its settings (listing the changes and the pipelines there that use it) and keeps its own
password. Saved passwords and keys are left out by default: the copy says **Needs its password**,
and nothing that uses it deploys until someone there enters it. Admins can choose **Copy them**
(copied on the server, still encrypted, never shown). A secret that comes from a store the other
project lacks needs a choice: copy that store too (Admins), or use one of the other project's stores.
The other project's policies apply, and both audit logs record the copy. Admins copy a secret store
on its own with **⋯ → Copy to project…** on its card. **Promote** offers the same copy when the
other project has no matching source or destination.

## Single sign-on

Admins set up sign-in in **Settings → Sign-in**, with as many providers as they need, of three kinds:

- **OpenID Connect**: Google, Microsoft Entra ID, Okta, Keycloak, GitLab, Dex, Auth0, Authentik, …
  Enter the issuer URL, client ID and client secret. ID tokens are verified against the issuer's
  keys and need a verified email.
- **OAuth2** for providers without OpenID Connect. **GitHub** (github.com or GitHub Enterprise
  Server: put your host in the three URLs) reads the person's primary *verified* address from
  `/user/emails` and their organizations (`acme`) and teams (`acme/data-platform`) as groups; it
  needs the `user:email` scope, and `read:org` for groups. **Other OAuth2** providers (Gitea, …)
  take an authorization, token and user URL and the names of the user answer's fields (id,
  email, "email verified", name, groups); an address counts as verified only when that field
  says so.
- **SAML 2.0**: Okta, Entra ID, ADFS, Google Workspace, JumpCloud, … Give the identity provider
  BeziSync's entity ID / metadata URL (`<public URL>/api/v1/auth/saml/metadata`) and ACS URL
  (`<public URL>/api/v1/auth/saml/acs`, HTTP-POST), all shown in the form; then give BeziSync the
  identity provider's metadata URL (re-read at every **Test**, so rotated certificates are picked
  up), paste its metadata, or enter its entity ID, HTTP-Redirect sign-in URL and signing
  certificate. Assertions must be signed by that certificate, name BeziSync as their audience,
  answer the request BeziSync sent (each request and assertion is accepted once) and carry a
  persistent or email NameID; the email, name and groups attributes are found under their common
  names unless you name them. Sign-in started at the identity provider is off unless you allow it.

**Attribute mapping.** OpenID Connect providers are read from the ID token's `email`,
`email_verified` and `name` claims unless you name others (Entra ID without a mailbox: email
claim `upn`); OAuth2 providers from the user answer's fields you name; SAML from the attributes
you name or the common ones (`givenname` + `surname` make the name when there is no display
name; repeated attributes are merged). A claim may be a dotted path into the token (Keycloak's
`realm_access.roles`); an exact name wins, so Auth0's namespaced claims such as
`https://acme.com/groups` work too.

**Verified email.** An address counts only when the provider vouches for it: OpenID Connect's
"email verified" claim must be true (Entra ID: add the optional claim `xms_edov` and name it, as
the Entra preset does); OAuth2 needs a field you name for it (GitHub: its primary *verified*
address); a signed SAML assertion vouches for its address. **Accept unverified email** lets such
people in, but only creates new accounts: an unverified address never signs in to an existing one.

**Groups that don't fit.** When a provider leaves groups out (Entra ID sends a link instead of the
groups past 200 in a token or 150 in a SAML assertion; GitHub hides organizations whose SAML single
sign-on the sign-in isn't authorized for, `X-GitHub-SSO: partial-results`), BeziSync refuses the
sign-in with the reason instead of syncing an incomplete list, which would remove roles. Send only
the groups assigned to the app, or authorize the organization on GitHub (`github.com/orgs/<org>/sso`)
and sign in again.

**GitHub.** Use an OAuth app (Settings → Developer settings → OAuth Apps); a GitHub App's user token
needs the "Email addresses" and "Members" read permissions. Organizations that restrict OAuth apps
show no teams until an owner approves BeziSync's app. github.com names itself
`https://github.com/login/oauth` in its callback (RFC 9207): the preset checks it, so a response
from anywhere else is refused.

**Callbacks name their issuer.** An OpenID Connect callback's `iss` must be the provider's issuer,
and is required when the provider advertises it; an OAuth2 provider's is checked when you set its
issuer identifier.

The redirect URI for OpenID Connect and OAuth2 is `<public URL>/api/v1/auth/oidc/callback`, the
same for every provider. Client secrets are encrypted and never shown again; leave the secret empty
when editing to keep it. **Test** checks what can be checked without a person (discovery and keys,
reachable endpoints, certificate validity); the first real sign-in proves the rest. The sign-in
page shows one button per provider that is turned on.

Group rules grant access from the provider's groups (name the claim or attribute if it is not
`groups`, and ask for the scope that adds it):

| Group | Grants |
|---|---|
| `data-platform` | Admin |
| `payments` | `editor` in `payments`, may approve |
| `analysts` | `viewer` in every project |

Project roles are `viewer`, `operator` and `editor`. People whom no rule grants any project
(including when the provider sends no groups) get the provider's **default role** in its
**default projects**, or no access when there is no default role. Roles granted by rules and the
default role follow the person's groups at every sign-in and show **Managed by SSO** in
**Settings → People**. Changing such a person's role or approver flag by hand takes that membership
off group sync, so the change sticks; roles granted by hand are never changed by SSO.

**One sign-in option per person.** The first sign-in links a person to that provider's identity
(and to an existing account with the same email). Someone already linked to another provider is
refused, so one provider can't take over another's accounts. When you move to a new provider, turn
on **Take over people who sign in with another option** on it: people then move to it at their
next sign-in (audited).

**Email and password sign-in** can be turned off once a provider passed its test and an Admin has
signed in with it (so someone can still manage BeziSync). While it is off, the sign-in page shows
only the providers; API tokens and MCP keep working. BeziSync refuses to turn off or remove the last
provider while password sign-in is off. If the provider breaks, run
`bezisync enable-password-login` in an API or controller pod to turn password sign-in back on.

The `BEZISYNC_OIDC_*` settings of earlier releases are no longer read (the API logs a warning
while any is still set): add the provider in Settings instead.

## AI agents (MCP)

BeziSync speaks the [Model Context Protocol](https://modelcontextprotocol.io), so agents such as
Claude Code, Claude Desktop, ChatGPT, Codex, Gemini CLI, Cursor, VS Code or Windsurf can list, build, check, deploy and operate pipelines for
people. An Admin turns it on in **Settings → MCP**; the endpoint is `<public URL>/api/v1/mcp`
(Streamable HTTP). **Account → AI agents** shows everyone how to connect: pick your agent and it
shows that client's own command or config (ChatGPT signs in only; the rest also take an API
token). For example:

```sh
claude mcp add --transport http bezisync https://bezisync.example.com/api/v1/mcp
```

**Two ways in**, each switchable in Settings → MCP:

- **Sign in (OAuth 2.1).** The agent finds BeziSync's authorization server from the endpoint's
  `401` (`/.well-known/oauth-protected-resource`), registers itself, and opens BeziSync in the
  browser. The person picks which projects it may open (one or all of theirs), whether it may
  only read, and for how long (an Admin sets the maximum). New agents can be usable at once or
  wait for an Admin. People see and disconnect their agents under Account; Admins see every agent
  and connection in Settings → MCP and can block or remove an agent.
- **API tokens.** A token from Account → API tokens in the agent's `Authorization: Bearer`
  header. A read-only token gives read-only tools; a project-scoped token one project; revoking
  the token disconnects the agent. The dialog that shows a new token once also shows the Claude
  Code command with the token filled in, and Account → AI agents has the steps under **API token**:

  ```sh
  claude mcp add --transport http bezisync https://bezisync.example.com/api/v1/mcp \
    --header "Authorization: Bearer bzt_…"
  ```

  Clients configured with JSON (Cursor, VS Code, Windsurf, Claude Desktop through `mcp-remote`, …)
  take the same header; Codex reads it from an environment variable
  (`--bearer-token-env-var`). The generic form:

  ```json
  {
    "mcpServers": {
      "bezisync": {
        "type": "http",
        "url": "https://bezisync.example.com/api/v1/mcp",
        "headers": { "Authorization": "Bearer bzt_…" }
      }
    }
  }
  ```

  To check a token, list the tools it may use:

  ```sh
  curl -s https://bezisync.example.com/api/v1/mcp -H "Authorization: Bearer bzt_…" \
    -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
  ```

**Tools follow the person.** Each request only lists the tools the person's live roles allow (an
Editor gets no Admin tools, a viewer no write tools), and every tool calls BeziSync's own API as
them: permissions, validation, policies, approvals ("deploy" asks for approval where required) and
the audit log (the actor reads `email (token MCP · <agent>)`) work exactly as in the app. Tools that
delete or reset (delete a pipeline, reset offsets, …) are **off** until an Admin turns them on;
Admins can turn any tool off.

**No credentials.** No tool takes or returns a password, key or token: creating API or agent tokens,
runtime, secret-store and alert-channel credentials, sign-in providers, passwords and the MCP
settings themselves are left to people in the app. A connection's secret fields can only be
secret-store references (`ref+vault://…`); event previews always mask personal data.

## Config-as-code and the CLI

Every pipeline exports as YAML (History → Export YAML, or `bezisync remote export`):

```yaml
apiVersion: bezisync.io/v1
kind: Pipeline
metadata: {name: orders-cdc, owner: payments-oncall, labels: {team: payments}}
spec:
  runtime: prod-k8s
  source: {connection: orders-db, tables: {include: [public.orders]}, columns: {public.orders: {mask: [email]}}}
  sink: {connection: events, naming: "cdc.{schema}.{table}", format: {type: json}}
  snapshot: {mode: initial}
  policies: {resources: medium}
  tuning: {debezium.source.snapshot.max.threads: "4"}
```

`tuning` holds typed engine settings from BeziSync's registry (`GET /api/v1/engines` lists them per
engine, with their bounds and what changing one does to a running pipeline). Each must apply to the
pipeline's engine, source and destination, and its value be written the one way BeziSync stores it
(`4`, not `04`); otherwise saving refuses it and says why. Debezium keys start with `debezium.`,
Flink CDC ones with the block they go in (`source.`, `sink.`, `pipeline.`); Flink settings stay
under `flink_cdc.flink_conf`. A setting always wins over an advanced property with the same key.
Preflight (and the version diff) lists every setting a deploy changes, typed settings, Flink
settings and advanced properties, from what to what, with what it does to the running pipeline
(applies live, restarts it, matters only for the first copy, takes effect on the next copy, or
starts it over) and the heaviest of those (`settings_change`).

Connections and runtimes are referenced by name. Apply from **Import YAML** or CI:

```bash
bezisync remote login --url https://bezisync.example.com --project prod --token <API token>
bezisync remote diff -f pipelines/                # what would change (a file, a directory of *.yaml, or -)
bezisync remote apply -f pipelines.yaml --deploy  # all or nothing; respects approvals
bezisync remote get
```

`diff` (and `apply`) print, for each pipeline, the spec diff, each setting a deploy changes from
what to what with what it does to the running pipeline, what BeziSync makes of each advanced
property (passed through, or refused and why), and what would **break** a deployed pipeline: a
setting that starts it over, another destination naming, another event format, or tables no longer
captured. `diff` then exits **3** (so CI can stop for a person), and `apply --deploy` changes
nothing, until `--confirm` is given. The CLI sends manifests as written, so fields it doesn't know
reach the server unchanged.

Agents read what a pipeline may tune with the MCP tool `list_engine_settings` (or `GET
…/pipelines/{id}/settings`): its engine's settings for its source and destination, each with its
bounds, what changing it does, the current value and this project's limit (a hidden one is left
out), and whether advanced properties are allowed (their keys, never their values). They change
them with `update_pipeline`, which applies the same checks, limits, policies and approvals as for
their person.

The CLI is the `bezisync` binary in the `bezisync` image: run it from the image (mount your manifests,
e.g. `docker run --rm -v "$PWD:/w" -w /w -e BEZISYNC_URL -e BEZISYNC_TOKEN
ghcr.io/daun-gatal/bezisync remote get`) or copy it out once (`docker create`, then
`docker cp <container>:/usr/local/bin/bezisync .`); it is a single static Linux binary.

API tokens: **Account → API tokens** (user menu) (project-scoped, optionally read-only, expiring after 30 days to a year; "Never" is possible but flagged). A new token is shown once. In CI use
`BEZISYNC_URL`, `BEZISYNC_TOKEN`, `BEZISYNC_PROJECT`. **Promote** (History tab) copies a pipeline's
latest version into another project as a new version there, mapping its connections; when the
other project has no matching source or destination, it offers to copy this pipeline's own.

## Adopt an existing Debezium Server

**Pipelines → Adopt existing:** paste its `application.properties`. BeziSync maps tables, snapshot
mode, naming, format, value formats, key columns and transforms (known SMTs become steps, any other
becomes a custom step that needs its policy), and sorts every property into five buckets: typed
settings (created as settings; one out of its bounds is listed as needing a change, never clamped),
wizard choices (a key a choice sets stays that choice, even when it is also a setting), managed by
BeziSync (connection, offsets, slot, history), kept as advanced, and refused. Excluded and masked
columns (`column.exclude.list`, `column.mask.with.N.chars`) become the Tables step's choices;
patterns it can't map are named in a warning. Stop the old deployment first;
BeziSync uses its own slot and offsets, so choose "Only new changes" to avoid copying rows again,
and drop the old slot afterwards.

## Administration

**Settings** (Admins only) is grouped by job: *People & projects*, *Running pipelines* (runtimes,
secret stores, engine versions) and *Guardrails* (policies, templates, audit log). Sections that
belong to one project show which at the top, with a picker to switch. Links to the former `/admin/…`,
Members and Pipeline images pages redirect to their new place.

- **People** — every account (local and SSO), the one place for access. Add a person with a role in
  a project (or as Admin; leave the password empty for SSO users: the account links to their
  identity by email at the first sign-in). Filter by **Can open <project>** to see and change who
  can open a project (its members and every Admin); **Manage** shows one person across all
  projects, makes them Admin, or deactivates the account (signed out everywhere, API tokens stop
  working; reactivating restores access). You cannot demote or deactivate yourself, and the last
  Admin is protected.
- **Projects** — create, rename and delete projects with their member and pipeline counts; **People**
  on a row opens who can open it. A project with pipelines cannot be deleted: delete those first so
  their connectors and source objects are cleaned up.
- **Feedback** — what people sent with **Feedback** at the bottom of the sidebar: something broken,
  a security issue, an idea or anything else, with the page they were on and their browser. Filter
  by open/resolved and type, open a report to read it in full, **Reply by email** or **Mark
  resolved** (and reopen). Every Admin gets an inbox entry per report in the project it came from
  (they can mute these under *Notification settings*). Anyone signed in can send up to 10 reports an hour; API
  tokens cannot send any. Messages are shown as plain text and never reach the audit log.

Upgrading from a version with per-project admins: each project admin becomes an editor with the
approver right in that project (never an Admin). Make people Admins under **People** as needed.

- **Audit log:** Settings → Audit log; export as CSV or JSON Lines.
- **Monitoring:** `GET /metrics` (Prometheus) when `BEZISYNC_METRICS_TOKEN` is set; `/healthz`, `/readyz`.
- **Master key rotation:** set the new key as `BEZISYNC_MASTER_KEY`, the old one in
  `BEZISYNC_PREVIOUS_MASTER_KEYS`, restart, run `bezisync rotate-master-key`. Sessions and tokens
  move to the new key when used; remove the old key once users signed in again and tokens were used
  or re-issued.
- **Telemetry:** off by default. With `BEZISYNC_TELEMETRY_ENABLED=true` and `BEZISYNC_TELEMETRY_URL`,
  the controller sends anonymous aggregate counts weekly; preview the exact payload at
  `GET /api/v1/system/telemetry` (instance admins).
- **Uninstall:** `bezisync uninstall` prints everything BeziSync created on sources and runtimes;
  `bezisync uninstall --yes [--drop-destinations]` removes it and reports each item. Then delete
  BeziSync and its database.
- **Version and licenses:** **Help & tour → About BeziSync** shows the server version and the license
  (Apache 2.0). BeziSync ships the licenses and notices of everything it includes: the web UI's at
  `/licenses.txt`, the server's at `GET /api/v1/system/licenses` (public, plain text), from
  `bezisync licenses`, and as `LICENSES.txt` in each CLI archive. Each starts with BeziSync's own NOTICE
  and LICENSE. Debezium Server is a separate project under its own license and is not included.

## Configuration reference

All settings are environment variables prefixed with `BEZISYNC_`.

| Variable | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `development` | `production` enforces secure settings |
| `DATABASE_URL` | local Postgres | Metadata database (SQLAlchemy URL) |
| `STATE_DATABASE_URL` | = `DATABASE_URL` | Connector offsets and schema history |
| `CONNECTOR_STATE_DATABASE_URL_OVERRIDE` | — | The state DB as connectors reach it, if different |
| `MASTER_KEY` / `PREVIOUS_MASTER_KEYS` | dev key / — | Secret encryption (base64, 32 bytes) and rotation |
| `PUBLIC_URL`, `COOKIE_SECURE`, `SESSION_TTL_HOURS` | `http://localhost:8080`, `false`, `12` | Browser access |
| `ALLOW_PLAIN_HTTP` | `false` | Production over http when the network encrypts traffic (e.g. Tailscale) |
| `BOOTSTRAP_ADMIN_EMAIL` / `_PASSWORD` | `admin@bezisync.local` / — | First administrator |
| `SSRF_ALLOW_CIDRS` / `SSRF_EXTRA_DENY_CIDRS` | — | Networks BeziSync may (not) reach for connections, secret stores and alerts: checks and every later connection (loopback, link-local and cloud metadata are refused by default) |
| `DEBEZIUM_DEFAULT_IMAGE` | `quay.io/debezium/server:3.7.0.Final` | Image when neither pipeline nor runtime picks one, unless an Admin chose a catalog default |
| `ICEBERG_IMAGE` | the pinned `ghcr.io/memiiso/debezium-server-iceberg` build | Image of Apache Iceberg pipelines (e.g. a mirror in a private registry) |
| `AGENT_URL` / `AGENT_IMAGE` / `AGENT_CHART` / `AGENT_CHART_VERSION` | `PUBLIC_URL` / this release's `bezisync` image / OCI chart / the agent chart version the control-plane chart was released with (unset: the newest) | Used in generated agent install commands |
| `LAB` | `false` | An open lab: the sign-in page invites anyone in and links the public testing policy (`/testing-policy`); feedback is addressed to the BeziSync team |
| `METRICS_TOKEN`, `METRICS_RETENTION_DAYS` | —, `30` | Prometheus endpoint and chart history |
| `SMTP_*` | — | Email alert channel |
| `TELEMETRY_ENABLED`, `TELEMETRY_URL` | `false`, — | Opt-in anonymous statistics |
| `SANDBOX_*` | — | Demo stack: create the Sandbox project |
| `CONTROLLER_TICK_SECONDS`, `LOG_LEVEL`, `LOG_JSON` | `5`, `INFO`, `true` | Operations |
