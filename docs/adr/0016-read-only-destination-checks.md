# 0016 — Destination checks only read: no test tables or indexes on the destination

- **Status:** Accepted
- **Date:** 2026-11-10
- **Amends:** 0009 (D2: the Elasticsearch check no longer writes and deletes a test index)
- **Scope:** `backend/internal/connectors` (Iceberg, JDBC, StarRocks/Doris/OceanBase, Elasticsearch checks; an OLAP
  `PrepareSink`; a new Elasticsearch `CheckPipeline`; a shared MySQL-format grant reader), `backend/internal/services`
  (`checkWhere`) and `backend/internal/api` (agent poll: older agents), `backend/internal/agent` and `runtime` (the
  `readonly_destination_checks` feature), `backend/internal/checks/catalog.json`, `docs/reference/checks.md`
  (generated), `docs/README.md`, the connectors, agent and runtime contracts.

## Context

Before a pipeline deploys, BeziSync checks its destination (the connection check and the pipeline's preflight run the
same `Check`). Four destination types proved "BeziSync may create tables" by creating a throwaway object and dropping
it again:

| Destination | Created | Cleanup |
|---|---|---|
| Apache Iceberg (REST, Glue) | a one-row table `<ns>.bezisync_check_<n>`: create, append a Parquet file, commit, read back | `icebergDrop`, error ignored |
| JDBC (PostgreSQL, MySQL, MariaDB, SQL Server) | `CREATE TABLE <schema>.bezisync_check_<n>` | `DROP TABLE`, error ignored |
| StarRocks, Doris, OceanBase | `CREATE DATABASE IF NOT EXISTS <db>`, then a test table `<db>.bezisync_check_<n>` | `DROP TABLE`, error ignored |
| Elasticsearch | a document in a new index `bezisync_check_<n>` | `DELETE /<index>`; a failure warned |

A least-privilege login (create and write, no drop) keeps every one of them. On an Apache Polaris 1.8.0 catalog
`lakehouse.cdc` filled up with `bezisync_check_*` tables, one per check run: the wizard, the deploy preflight and the
lab's apply each run the check. Polaris even commits the table before it answers, so a check that *failed* left one
too. Users don't want a check to create anything on their destination: the objects are noise, and a reusable fixed
table is still an object nobody asked for.

BeziSync already has the read-only pattern: the Kafka check calls `ValidateCreateTopics`, which asks the broker
whether a create *would* succeed and creates nothing. Paimon and Redis write one short-lived object (an S3 object, a
Redis key), delete it in the same check and fail the check when the delete fails; they create no table or index and
stay as they are (see *Alternatives considered*).

What makes this hard: most destinations have no "may I create a table?" question. Some answer exactly (PostgreSQL's
privilege functions, SQL Server's `HAS_PERMS_BY_NAME`, Elasticsearch's `_has_privileges`); some only show grant text
that can't prove an absence (roles, Ranger, LDAP, inactive default roles); Iceberg REST and Glue have no such call at
all. And agents run destination checks for agent-mode runtimes, so an agent from before this change keeps creating
tables until it is upgraded.

## Decision

### D1. The rule

A destination check never creates a table, a database, a namespace or an index on the destination, and never sends a
statement or request that would: no `CREATE TABLE`/`CREATE DATABASE`, no Iceberg table create (staged or not), no
index-creating write, no `BEGIN; CREATE …; ROLLBACK`. It asks instead: privilege functions and APIs, catalog lookups,
`SHOW GRANTS`, `HEAD` requests. Whatever the pipeline needs is created on deploy (`PrepareSink`) or by the connector,
as before. New connectors follow the same rule (the connectors contract says so).

One narrow exception stays: a probe that creates no table, database, namespace or index, is deleted in the same check,
and fails the check when the delete fails — Paimon's object under the warehouse, Redis's `bezisync:permission-probe`
key. Those prove a right the connector needs (deleting files, appending to streams) and leave nothing behind.

*Recorded decision:* the stricter wording ("never sends a create") was chosen over "never leaves anything visible" on
the requester's behalf (an autonomous run). It is the simplest to explain and test; Iceberg's `stage-create` (which
may answer "may create" without persisting a table) is left for a separate, per-catalog verified change.

### D2. One result rule

- **Pass:** the server confirmed the right (a function, `_has_privileges`, or an exact grant match by the rules below).
- **Fail / Warn:** only on a definite "no" from a server that answers yes or no (PostgreSQL, SQL Server,
  Elasticsearch). Each check keeps its level: the JDBC create check stays a non-blocking Warn; Elasticsearch's index
  rights block.
- **Unknown** (grey, never blocking): everything else — grant text that doesn't show the right (it may come from a
  role or an external authorizer), a question the login may not ask, an API that's absent, any error. Each Unknown says
  what wasn't verified, what will show it (the first deploy), and the grant to apply.

Grant text (MySQL, MariaDB, OceanBase, StarRocks, Doris) yields Pass or Unknown, never Fail. A check that can't sign in
still fails as today (sign-in is not a privilege question).

### D3. Apache Iceberg

`iceberg_write` no longer creates a table. It reports **Unknown**: "Table create and commit aren't checked before
deploy: Iceberg catalogs have no read-only way to ask. The first change written shows any error." Its title becomes
*Table create and commit* (the old title stays in the catalog). Catalog sign-in (`iceberg_catalog`) and the namespace
lookup (`iceberg_namespace`) are unchanged. No storage listing: listing proves `ListBucket`, not writing, with the
wrong identity for vended credentials, and RustFS S3 Tables and S3 Tables can't do it.

### D4. StarRocks, Doris, OceanBase

**The database is created on deploy.** A new `olapSink.PrepareSink` looks the database up
(`SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?`): found → `exists`; not found →
`CREATE DATABASE IF NOT EXISTS` (MySQL error 1007, "database exists", also counts as `exists`) → `created`. It dials
through `olapConfig.open` (the SSRF guard, ADR 0004) with the check's 10 s timeout, from the controller like Iceberg's
namespace. When it can't (unreachable, or the login may not create databases) the outcome is `failed` and the deploy
stops with "create database `<db>` on the destination, or let BeziSync reach `<host>:<port>`".

**The check reads.** It looks the database up the same way, then reads `SHOW GRANTS`:

| Engine | Exists, may create tables (Pass) | Missing or not visible |
|---|---|---|
| StarRocks | a row whose `Catalog` is `default_catalog` and whose `Grants` is `GRANT <privs> ON DATABASE <db> …` or `… ON ALL DATABASES …` with `CREATE TABLE` or `ALL` among the comma-separated privileges | Unknown ("created on deploy if the login may") |
| Doris | `CREATE_PRIV` or `ADMIN_PRIV` in `GlobalPrivs`, in `CatalogPrivs` for `internal`, or in `DatabasePrivs` for `internal.<db>` | Unknown |
| OceanBase | the MySQL-format rule (D6) on `` `<db>`.* `` or `*.*` | Pass when `CREATE`/`ALL` is on `*.*`, else Unknown |

Anything else is Unknown with the grant SQL (code `<olap>_create_table`, unchanged). Columns are read by name; StarRocks
2.x and Doris 1.x layouts, inactive roles (`current_role()` = `NONE` with no direct grant) and role-only grants give
Unknown. StarRocks hides databases a login has no right on, so "not found" is worded "missing or not visible".

The probe table used to catch a `replication_num` above the live backends. That isn't replaced (`SHOW BACKENDS` needs
operator rights and is empty on shared-data clusters); a too-high replica count shows when the first table is created.

### D5. Elasticsearch

- The connection check keeps sign-in and version; it no longer writes or deletes an index.
- A new `CheckPipeline` asks, for the pipeline's own indexes (the routed destinations as the sink names them: dots as
  `_`, lower-case), one `HEAD /<index>` each, then one
  `POST /_security/user/_has_privileges` (6.x: `/_xpack/security/user/_has_privileges`) with
  `{"index":[{"names":[…],"privileges":["index","delete","create_index","delete_index"(,"auto_configure")]}]}`.
  The version comes from `GET /`; `auto_configure` is asked from 7.9; below 6.4 there is no API → Unknown.
  - An index lacking `index` or `delete`, or a missing index lacking both `create_index` and `auto_configure` → Fail,
    blocking (`elasticsearch_create_index`), naming the indexes.
  - Lacking `delete_index` → Warn: deleting the pipeline with "drop destinations" would fail (as before).
  - Any error or unexpected answer (including security disabled, 401, 403) → Unknown. A cluster without security
    passes nothing here; its Unknown says why.
- It runs where pipeline checks run (the control plane) and lands in the configuration section, which a forced deploy
  can't skip: intended, the answer is definite. A cluster only the runtime can reach gets Unknown.
- Automatic index creation isn't read from cluster settings (needs `monitor`; templates override it); the fix names
  it.

### D6. JDBC

- **PostgreSQL:** for the connection's schema (lower-cased as the sink names it; `current_schema()` when empty):
  `to_regnamespace` null → Warn "schema `<s>` doesn't exist: create it" (the sink doesn't); else
  `has_schema_privilege(<s>, 'USAGE')` and `'CREATE'` both true → Pass, either false → Warn.
- **SQL Server:** the schema is the login's default (`SCHEMA_NAME()`; the connection has none for SQL Server):
  `HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')` and `HAS_PERMS_BY_NAME(SCHEMA_NAME(), 'SCHEMA', 'ALTER')`:
  both 1 → Pass, either 0 → Warn, any NULL (no default schema) → Unknown. The fix names that schema instead of `dbo`.
- **MySQL, MariaDB (and OceanBase, D4):** one MySQL-format reader over `SHOW GRANTS`: rows `GRANT <privs> ON <scope> TO
  …`; privileges split on `,` and trimmed; `CREATE` or `ALL [PRIVILEGES]` must be an exact item (`CREATE VIEW`,
  `CREATE ROUTINE`, `CREATE USER`, `CREATE TEMPORARY TABLES`, `CREATE SYNONYM` don't count); scope `*.*` or `<db>.*`
  (backticks optional; `_` and `%` are wildcards, `\_` a literal); table-level grants ignored; any `REVOKE … ON` row
  (partial revokes) → Unknown; nothing matched → Unknown.
- A definite "no" stays a Warn (`jdbc_create_table`), as before: existing tables still work.

### D7. Older agents

Agents advertise a new feature, `readonly_destination_checks`. For a sink of these four families (Iceberg, JDBC,
StarRocks/Doris/OceanBase, Elasticsearch):

- **Preflight** (`checkWhere`): when the runtime's stored agent state lacks the feature (or has none), the check isn't
  sent to the agent and isn't run from the control plane either; it returns one Unknown, `agent_readonly_checks`:
  "The agent on `<runtime>` (`<version>`) predates read-only destination checks and would create test tables; upgrade
  it to check this destination." Running it from the control plane instead would turn destinations only the runtime
  can reach into blocking network failures.
- **Dispatch** (agent poll): a pending `check` for one of these families is handed only to an agent whose poll lists
  the feature; otherwise the request is completed at once with that Unknown report. This covers an agent that polls
  before its state is stored, a rolling update and a rollback.

Rollout: upgrade the control plane before agents. A new agent under an older control plane creates no OLAP database
and has no Elasticsearch privilege check (both moved to the control plane); the changelog says so.

### D8. Codes and titles

All four codes stay; one is new: `agent_readonly_checks`. Titles, whys and fixes that described creating are rewritten
(old titles stay in `titles`); the Elasticsearch `{probe}` fix goes; Doris's grant keeps `DROP_PRIV` ("drop
destinations" needs it). The catalog test only sees literal codes, so the OLAP codes (built from `o.typ`) get a test
that loops over the three sinks.

### Implementation plan

One PR, one commit per step:

1. This ADR (index row; 0009's status notes the amendment).
2. **Iceberg:** `icebergWriteCheck` removed, `iceberg_write` Unknown; catalog text.
3. **Agent gate:** `runtime.FeatureReadonlyDestinationChecks`, advertised by the agent; `checkWhere` and the agent poll
   (`api/agent.go`) skip the four families for agents without it; `agent_readonly_checks`.
4. **Elasticsearch:** connection check without the test index; `CheckPipeline` with `HEAD` and `_has_privileges`.
5. **JDBC:** PostgreSQL and SQL Server privilege queries; the MySQL-format grant reader.
6. **OLAP:** `PrepareSink` (database); the read-only check (`SCHEMATA`, `SHOW GRANTS`); `olap_live_test.go`.
7. **Docs:** regenerated `docs/reference/checks.md`, `docs/README.md`, the contracts, the threat model's outbound list,
   the changelog fragment (rollout order, and how to drop the `bezisync_check_*` objects earlier versions left).

### Verification plan

Grant and answer interpreters are pure functions, tested on outputs captured from real servers (StarRocks 3.4, Doris
3.0, OceanBase CE 4.3, MySQL 8.4, Elasticsearch 8.15 in the BeziSync lab, 2026-11-10). Checks that talk to a server are
tested against fakes that record every request or statement and fail on any create: `httptest` for Iceberg REST and
Elasticsearch, a recording `database/sql` driver for the OLAP and MySQL-format paths. The PostgreSQL check runs against
the test database (`BEZISYNC_TEST_DATABASE_URL`) with roles that lack `USAGE` or `CREATE`.

As implemented: the StarRocks and Doris column layouts (`UserIdentity`/`Catalog`/`Grants`;
`GlobalPrivs`/`CatalogPrivs`/`DatabasePrivs`) follow the vendors' `SHOW GRANTS` reference, because the lab
namespace was removed before every header could be re-read. The readers find columns by name, and a missing column reads
as "nothing shown" (Unknown), so a different layout can't produce a false Pass. No live StarRocks, Doris, OceanBase,
Elasticsearch or Iceberg server was used for these tests; PostgreSQL ran against a real server.

| # | Case | Expected | Test |
|---|---|---|---|
| E1 | Iceberg namespace exists | no create request; `iceberg_write` Unknown | `TestIcebergCheckCreatesNothing` |
| E2 | Iceberg namespace missing | "created on deploy", no create | same |
| E3 | Agent lacking the feature (or no state), gated sink | one Unknown, nothing sent | `TestReadonlyGate` |
| E4 | Agent lacking the feature, other sink or a source | sent to the agent as before | same |
| E5 | Agent with the feature | sent as before | same |
| E6 | Poll from an agent lacking the feature, gated `check` pending | completed with the Unknown report, not handed out | `TestAgentPollHoldsGatedChecks` |
| E7 | ES all privileges, indexes missing | Pass, no index created | `TestElasticsearchPipelinePrivileges` |
| E8 | ES no `create_index`, `auto_configure` (≥7.9) | Pass | same |
| E9 | ES no `delete` / no `index` | Fail, blocking, names the index | same |
| E10 | ES no create rights, every index exists | Pass | same |
| E11 | ES no `delete_index` | Warn | same |
| E12 | ES 6.x, 7.8, below 6.4 | `_xpack` path; no `auto_configure`; Unknown | same |
| E13 | ES 401/403/security disabled/other error | Unknown | same |
| E14 | ES connection check | no index written or deleted | `TestElasticsearchCheckCreatesNothing` |
| E15 | PostgreSQL USAGE+CREATE / no CREATE / no USAGE / schema missing / mixed-case schema | Pass / Warn / Warn / Warn / lower-cased | `TestJDBCPostgresCreateRight` (test DB) |
| E16 | SQL Server 1,1 / a 0 / a NULL | Pass / Warn / Unknown | `TestSQLServerCreateRight` |
| E17 | MySQL `ALL PRIVILEGES ON *.*`, `CREATE ON db.*`, `` `db`.* ``, pattern `d\_b` / `d%` | Pass | `TestMySQLGrantsAllowCreate` |
| E18 | MySQL `CREATE VIEW` only, `CREATE TEMPORARY TABLES`, other db, table-level, partial revoke, role only | Unknown | same |
| E19 | StarRocks captured grants; external catalog row; `ALL ON ALL DATABASES`; `NONE` roles only; 2.x layout | Pass / Unknown / Pass / Unknown / Unknown | `TestStarRocksGrants` |
| E20 | Doris captured row; `CatalogPrivs internal`; global; `N/A` columns; 1.x layout | Pass / Pass / Pass / Unknown / Unknown | `TestDorisGrants` |
| E21 | OceanBase captured grants; `CREATE SYNONYM` only; database missing with `CREATE ON *.*` | Pass / Unknown / Pass | `TestOlapCheckCreatesNothing` |
| E22 | OLAP database missing (StarRocks, Doris) | Unknown, "created on deploy if the login may" | `TestOlapCheckCreatesNothing` |
| E23 | OLAP check over a recording driver | no `CREATE` statement sent | same |
| E24 | `PrepareSink` exists / created / 1007 / unreachable | exists / created / exists / failed with the fix | `TestOlapPrepareSink` |
| E25 | Every OLAP code in the catalog | entries for all three sinks | `TestOlapCodesInCatalog` |

## Consequences

- **No check creates a table, database, namespace or index on a destination.** Least-privilege logins stop collecting
  `bezisync_check_*` tables; the objects earlier versions left can be dropped by hand (the changelog says how).
- **Detection moves later for Iceberg.** A catalog that refuses commits, storage keys that can read but not write, a
  wrong S3 endpoint: these show on the first change written, as a connector error, not before deploy. On a PostgreSQL
  source a failing pipeline holds WAL in its slot until fixed; preflight can no longer prevent that. The check says
  so as an Unknown note.
- **Grant-based checks get weaker where grants are indirect.** A role-only or externally authorized login gets Unknown
  instead of the old blocking Fail (OLAP) when it can't create; the deploy then fails at table creation. OLAP replica
  counts above the live backends are no longer caught before deploy.
- **OLAP deploys now dial the destination from the control plane** (`PrepareSink`), also for agent-mode runtimes,
  and stop with a fix when they can't; the database is no longer created by a check.
- **Elasticsearch privileges are checked against the real index names**, which fixes a login allowed only `cdc_*`
  failing a test index named `bezisync_check_*`. The connection check alone no longer decides index rights; the
  pipeline check runs from the control plane and can't be skipped by a forced deploy.
- **Older agents** skip these destination checks (one Unknown says to upgrade) until upgraded. Upgrade the control
  plane before agents.

## Alternatives considered

- **One fixed, reused test table** (`bezisync_metadata_check`: create if missing, write a row, never drop). Keeps
  the full proof and bounds the pile-up to one table per database. Rejected by the requester: it is still an object on
  the destination nobody asked for.
- **Create, drop, and report a failed drop** (Elasticsearch's behaviour, everywhere). Makes leftovers visible but
  still creates one per run for a no-drop login.
- **`BEGIN; CREATE TABLE; ROLLBACK`** (PostgreSQL, SQL Server have transactional DDL). Proves the right exactly and
  leaves nothing, but sends a create; excluded by D1.
- **Iceberg `stage-create`.** May answer "may create" without persisting a table, but catalogs differ (Lakekeeper is
  believed to persist staged tables; Polaris authorizes it as create plus write) and iceberg-go always commits the
  second phase. Left for a separate change that verifies it per catalog.
- **List the Iceberg storage location.** Proves listing, not writing, with the wrong identity for vended credentials;
  can't run for S3 Tables or RustFS. Dropped.
- **Older agents: run the check from the control plane** (the existing fallback). Turns destinations only the runtime
  can reach into blocking network failures for every not-yet-upgraded agent. Rejected for one Unknown.
- **OLAP: rely on the Flink CDC sink to create the database.** OceanBase's sink URL names the database, so it must
  exist; StarRocks's and Doris's behaviour would have to be verified per version. Rejected for `PrepareSink`.
- **OLAP replicas from `SHOW BACKENDS`.** Needs operator rights and is empty on shared-data clusters (a false Fail).
  Dropped.
- **Drop rights for the BeziSync login, or a nightly sweeper.** Destination-side workarounds every user would have to
  set up; the requester didn't want the objects created at all.
