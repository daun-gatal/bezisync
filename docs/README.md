# BeziSync guide

BeziSync is a self-service control plane for [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html):
platform teams connect runtimes and set guardrails once; anyone can then stream changes from a
database to a destination safely. This guide covers installing, using and operating it.

- [Install](#install) · [Concepts](#concepts) · [Your first pipeline](#your-first-pipeline) · [Sources](#sources)
- [Destinations](#destinations) · [Databases behind a bastion (SSH)](#databases-behind-a-bastion-ssh) · [Operate](#operate) · [Manage runtimes](#manage-runtimes) · [Remote runtimes (agents)](#remote-runtimes-agents) · [Google GKE and Amazon EKS](#google-gke-and-amazon-eks) · [Debezium versions](#debezium-versions)
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
| Connection | A reusable source (PostgreSQL, MySQL/MariaDB, SQL Server, MongoDB, Oracle, CockroachDB, Vitess, Google Cloud Spanner) or destination (Kafka, HTTP, Pub/Sub, Kinesis, Redis Streams, NATS JetStream, Apache Iceberg, PostgreSQL / MySQL / MariaDB / SQL Server through JDBC, Amazon SQS, Amazon SNS, Azure Event Hubs, Apache Pulsar) |
| Pipeline | One source → one destination, the tables, format, transforms and settings. Every saved change is a **version**; deploying runs one version |
| Health | Healthy · Lagging · At risk (source impact) · Down · Unknown. Never green without fresh evidence |

**Admin** is account-wide: Admins open every project with every permission and are the only ones who
see **Settings** (people, projects, runtimes, Debezium versions, policies). Everyone else gets a role
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
   happens to personal data (see [Personal data](#personal-data)), pick the destination and naming, shape the events (optional,
   see below), review the generated `application.properties`, and deploy. A step opens once the
   ones before it are done, and leaving a step by any route saves it. On **Review**, settings save
   as you change them and the final checks re-run on their own; Deploy says why while it is
   unavailable. Final checks must pass; "Deploy anyway" (if the project allows it) needs a recorded
   reason and never skips configuration, runtime or policy failures. Closing a fresh draft asks
   whether to keep or delete it.

## Sources

Every source runs the readiness checks of its own database, each failure with the exact fix for the
provider, and shows **source protection** in its own terms (see [Operate](#operate)). Sources Debezium
marks *incubating* carry a **Preview** badge when you add them. What a source can do decides what the
wizard offers: the snapshot modes on Review, re-copies on the Snapshots tab, masking, message keys
and the delivery canary.

| Source | Copies existing rows | Re-copies while streaming | Mask columns | Delivery canary |
|---|---|---|---|---|
| PostgreSQL, MySQL / MariaDB, SQL Server | every mode | yes | yes | PostgreSQL, MySQL |
| MongoDB | every mode | yes | no | no |
| Oracle | every mode | yes | yes | yes |
| CockroachDB | every mode | yes | yes | yes |
| Vitess | "Copy existing rows" or "Only new changes" | no: reset the position from scratch | yes | no |
| Google Cloud Spanner | no: it streams from its first start | no | no (columns can be removed) | no |

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
| Keep events | Keep only some operations (insert, update, delete, snapshot) and/or rows matching rules on columns (`is`, `is one of`, `>`, `is empty`, …), for all or some tables | Runs on the full change event, so deletes are judged on the row before. PostgreSQL sends only the key of a deleted row unless the table has `REPLICA IDENTITY FULL`. Dropped events are gone for that pipeline; re-snapshot to bring rows back. MongoDB can filter on operations only |
| Event shape | The row after the change (with deletes kept as `__deleted = true`, as tombstones, or dropped, and optional metadata fields such as `__op`) or the full change envelope | |
| Field changes | Rename fields, add a field (fixed text or the pipeline's name), flatten nested fields, custom transforms (Experts, policy) | Order matters within this stage; they need the row shape. The message key keeps column names |
| Message key | Key columns per table, e.g. for tables without a primary key | PostgreSQL, MySQL, Oracle, CockroachDB and Vitess. A key column the table lacks is ignored by Debezium |

Fields are picked from the discovered columns; only new names and rule values are typed, and
anything with `${…}` is refused (Debezium Server would read it as a variable, for example a
secret). The **preview** builds a sample row from the table's columns (no data is read) and shows
the event after every step for an insert, update, delete or snapshot, with its key and
destination. Custom classes and filter scripts cannot be previewed and show as Unknown.

At deploy, preflight checks every step against the tables: a field added or renamed onto one the
event already has **blocks the deploy** (Debezium would stop with "field name duplication"); steps
on missing, excluded or masked columns, rules that can never match a column's type, missing key
columns and custom classes Debezium 3.7 does not ship are warnings. Review (and the approval
request) lists **what changes for consumers**: Breaking, Key change or Additive. After a deploy,
the Events tab says whether real events match the preview, and when a schema change breaks a step
the pipeline's Overview shows it with a **Fix** link and the *Transform needs attention* alert
fires. Every step is in the spec (`transforms`, `values`, `keys`), so YAML, templates, promote and
rollback carry it; `debezium.transforms*` cannot be set as advanced properties.

## Destinations

Each destination states what it needs from a pipeline, and the wizard only offers that: the names
it accepts (with a live preview), its event formats, the event shape it requires, its own
pipeline settings and the smallest connector size it runs in. Preflight checks the rest.

### Apache Iceberg

Changes land in Iceberg tables, one per source table, through an Iceberg REST catalog or AWS Glue.
Presets: **Iceberg REST catalog** (Polaris, Lakekeeper, Gravitino, Nessie), **Amazon S3 Tables**
(SigV4, service `s3tables`, warehouse = table bucket ARN), **AWS Glue Data Catalog** (warehouse =
an `s3://` location), **Snowflake Open Catalog** (OAuth2, scope `PRINCIPAL_ROLE:ALL`, vended
credentials), **Databricks Unity Catalog** (token, vended credentials) and **RustFS S3 Tables**
(preview: SigV4 service `s3`, warehouse = the table bucket, path-style S3; change logs only, as it
refuses the first upsert commit of a new table). Data files are written
with the object-storage keys of the connection, the runtime's IAM role, or credentials the catalog
vends. Snowflake, BigQuery (BigLake), Databricks, Trino, StarRocks and ClickHouse read the tables
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
- **Partitioning**: upserted tables are bucketed by their first key column (`bucket(id, 16)`; 4 or 64
  buckets in the Destination step), change logs by the day of the change (`day(__source_ts_ms)`).
  The wizard records each table's key column in `sink.options.partitions` (set it there through
  YAML or the API); existing tables keep their partitioning. Expert mode can turn it off.
- **Event shape**: Iceberg needs the row after change with `__op` and `__source_ts_ms`; BeziSync adds
  that step (or completes yours) and sends timestamps as ISO-8601 text (Kafka Connect types for SQL
  Server). Outbox steps and tombstone deletes are not available.
- **Preflight** signs in to the catalog, checks the namespace, writes a one-row test table, reads it
  back and drops it, then checks keys and tables that already exist. The **Events** tab and the
  delivery canary read the newest rows of each table.
- **Image**: pipelines run `ghcr.io/memiiso/debezium-server-iceberg` (see [Debezium
  versions](#debezium-versions)), amd64 only. It serves health on port 9000, which BeziSync probes;
  agent runtimes need an agent of this BeziSync version or newer (preflight says so). Deleting a pipeline with "drop destinations" drops its
  tables (purging files where the catalog allows; RustFS keeps them).

### Database (JDBC)

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
- **Preflight** reaches the database, signs in, creates and drops a test table, and checks keys and
  name lengths (63 characters in PostgreSQL, 64 in MySQL, 128 in SQL Server). The **Events** tab
  and the delivery canary read rows of the tables.
- Deleting a pipeline with "drop destinations" drops its tables.
- Across databases, Debezium copies column defaults into the tables it creates: a PostgreSQL
  `timestamptz DEFAULT now()` column fails to create in MySQL or MariaDB ("Invalid default value").
  Exclude the column (Tables step) or create the table yourself and choose "Use existing tables only".

### Amazon SQS and Amazon SNS

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

Every table of the pipeline goes to the connection's event hub (it must exist), partitioned by the
row key so each row keeps its order; consumers tell tables apart by `source.table` (or `__table`).
Sign in with a namespace connection string (shared access policy with **Send**), without
`EntityPath`. The Events tab and the delivery canary read the last events of each partition, which
needs **Listen**: add a second connection string for reading when the first cannot listen
(preflight warns). Events over the tier's limit (1 MB on Standard) stop the pipeline. Entra ID
sign-in is not available: Debezium's sink supports connection strings only.

### Apache Pulsar

One persistent topic per table in the connection's tenant and namespace, keyed by the row key. The
connector produces to the **service URL** (`pulsar://…:6650`, or `pulsar+ssl://…:6651` with an
optional private CA); BeziSync uses the **admin URL** (`http(s)://…:8080`) to check the namespace,
create missing topics on deploy, and read the newest messages for the Events tab and the delivery
canary (`examinemessage`: no subscription, your consumers are untouched). Sign in with a token (JWT,
or a StreamNative API key). A send that does not complete within 30 seconds fails the batch, which
Debezium retries, rather than being lost.

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
  one small canary topic, stream or table named like the others.
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
ConfigMaps, Secrets, pods, logs and `pods/proxy`.

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
  Settings → Debezium versions).

**Checks run where connectors run.** For pipelines on an agent runtime, the final checks before a
deploy run on the agent, inside the cluster's VPC, so a database reachable only from there passes
(the sections say "checked by the agent"). If the agent cannot answer, BeziSync checks from its own
network and says so. Checks on the Connections page always run from BeziSync.

## Debezium versions

**Settings → Debezium versions** (Admins) is the one list of Debezium Server images every project
picks from, with how many pipelines run each one per project (each count links to that project's
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
deploying the previous one.

## Personal data

Each pipeline decides what happens to personal data, in the **Tables** step (and later from
**Change → Tables and columns** or the Data tab's **Tables and personal data**). When you pick a
table, every column whose name looks like personal data (email, phone, address, birth date, card,
IBAN, passport, tax or national ID numbers) is protected at once, and the **Personal data** card
lists them with one choice each:

- **Mask**: replaced by a salted SHA-256 hash, unique to the pipeline, so equal values still match
  for joins. Debezium hashes **text columns only**, so Mask is offered only for those (MongoDB can't
  hash at all).
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
modes; the largest size; pipelines per source (each
holds a replication slot or log reader); whether "Deploy anyway" exists; whether deploys need
**approval**; which transform types pipelines may use; and whether **custom transforms** (any SMT
class in the image) and **scripted transforms** (Groovy/JavaScript filter scripts, which run next to
the connector's credentials) are allowed — both are off by default. The section groups these by what
you decide, refuses an empty allow-list, and before you save lists the running pipelines whose next
deploy the change would block. The wizard applies policies up front (disallowed options stay visible
and say why); the server enforces them at save and deploy.

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
```

Connections and runtimes are referenced by name. Apply from **Import YAML** or CI:

```bash
bezisync remote login --url https://bezisync.example.com --project prod --token <API token>
bezisync remote diff -f pipelines/                # what would change (a file, a directory of *.yaml, or -)
bezisync remote apply -f pipelines.yaml --deploy  # all or nothing; respects approvals
bezisync remote get
```

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
becomes a custom step that needs its policy), keeps safe extras as advanced properties, and lists
what it manages itself (connection, offsets, slot, history) with warnings. Stop the old deployment first;
BeziSync uses its own slot and offsets, so choose "Only new changes" to avoid copying rows again,
and drop the old slot afterwards.

## Administration

**Settings** (Admins only) is grouped by job: *People & projects*, *Running pipelines* (runtimes,
secret stores, Debezium versions) and *Guardrails* (policies, templates, audit log). Sections that
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
| `SSRF_ALLOW_CIDRS` / `SSRF_EXTRA_DENY_CIDRS` | — | Networks connection checks may (not) reach |
| `DEBEZIUM_DEFAULT_IMAGE` | `quay.io/debezium/server:3.7.0.Final` | Image when neither pipeline nor runtime picks one, unless an Admin chose a catalog default |
| `ICEBERG_IMAGE` | the pinned `ghcr.io/memiiso/debezium-server-iceberg` build | Image of Apache Iceberg pipelines (e.g. a mirror in a private registry) |
| `AGENT_URL` / `AGENT_IMAGE` / `AGENT_CHART` / `AGENT_CHART_VERSION` | `PUBLIC_URL` / this release's `bezisync` image / OCI chart / the agent chart version the control-plane chart was released with (unset: the newest) | Used in generated agent install commands |
| `LAB` | `false` | An open lab: the sign-in page invites anyone in and links the public testing policy (`/testing-policy`); feedback is addressed to the BeziSync team |
| `METRICS_TOKEN`, `METRICS_RETENTION_DAYS` | —, `30` | Prometheus endpoint and chart history |
| `SMTP_*` | — | Email alert channel |
| `TELEMETRY_ENABLED`, `TELEMETRY_URL` | `false`, — | Opt-in anonymous statistics |
| `SANDBOX_*` | — | Demo stack: create the Sandbox project |
| `CONTROLLER_TICK_SECONDS`, `LOG_LEVEL`, `LOG_JSON` | `5`, `INFO`, `true` | Operations |
