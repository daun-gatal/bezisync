# 0016 — Destination checks only read: no test tables or indexes on the destination

- **Status:** Proposed
- **Date:** 2026-11-10
- **Amends:** 0009 (D2: the Elasticsearch check no longer writes and deletes a test index)
- **Scope:** `backend/internal/connectors` (Iceberg, JDBC, StarRocks/Doris/OceanBase, Elasticsearch checks; an OLAP
  `PrepareSink`; a new Elasticsearch `CheckPipeline`), `backend/internal/services` (`checkWhere`: older agents),
  `backend/internal/agent` and `runtime` (the `readonly_destination_checks` feature), `backend/internal/checks/catalog.json`,
  `docs/reference/checks.md` (generated), `docs/README.md`, the connectors contract.

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

A destination check never sends a create: no `CREATE TABLE`/`CREATE DATABASE`, no Iceberg table create (staged or
not), no index-creating write, no `BEGIN; CREATE …; ROLLBACK`. It only reads: privilege functions and APIs, catalog
lookups, `SHOW GRANTS`, server and node listings. Whatever the pipeline needs created is created on deploy
(`PrepareSink`) or by the connector, as before. New connectors follow the same rule (the connectors contract says so).

*Recorded decision:* the stricter wording ("never sends a create") was chosen over "never leaves anything visible" on
the requester's behalf (an autonomous run). It is the simplest to explain and test; Iceberg's `stage-create` (which
may answer "may create" without persisting a table) is left for a separate, per-catalog verified change.

### D2. One result rule

- **Pass:** the server confirmed the right (a function, `_has_privileges`, or an exact grant match).
- **Fail / Warn:** only on a definite "no" from a server that answers yes or no (PostgreSQL, SQL Server,
  Elasticsearch), at the level the check already had: the JDBC create check stays a non-blocking Warn,
  Elasticsearch's index rights block.
- **Unknown** (grey, never blocking): everything else — grant text that doesn't show the right (it may come from a
  role or an external authorizer), a question the login may not ask, an API that's absent. Each Unknown says what
  wasn't verified, what will show it (the first deploy), and the grant to apply.

Grant text (MySQL/MariaDB, StarRocks, Doris, OceanBase) yields Pass or Unknown, never Fail.

### D3. Apache Iceberg

`iceberg_write` no longer creates a table. It reports **Unknown**: "Table create and commit aren't checked before
deploy: Iceberg catalogs have no read-only way to ask. The first change written shows any error." Its title becomes
*Table create and commit* (the old title stays in the catalog). Catalog sign-in (`iceberg_catalog`) and the namespace
lookup (`iceberg_namespace`) are unchanged. No storage listing: listing proves `ListBucket`, not writing, with the
wrong identity for vended credentials, and RustFS S3 Tables and S3 Tables can't do it.

### D4. StarRocks, Doris, OceanBase

- **The database is created on deploy.** A new `olapSink.PrepareSink` runs `CREATE DATABASE IF NOT EXISTS` (outcome
  `exists` or `created`), like Iceberg's namespace (it runs in the controller, as Iceberg's does). The check no longer
  creates it; the guide's "created if missing" still holds.
- **The check reads:** `information_schema.SCHEMATA` for the database, then `SHOW GRANTS`:
  - database exists, and a grant covers creating tables in it (StarRocks `CREATE TABLE` or `ALL` on the database or
    all databases; Doris `Create_priv`/`Admin_priv` on `internal.<db>` or globally; OceanBase `CREATE`/`ALL
    PRIVILEGES` on `` `<db>`.* `` or `*.*`) → Pass;
  - database missing, and a grant covers creating databases (StarRocks `CREATE DATABASE` on the catalog; Doris global
    `Create_priv`/`Admin_priv`; OceanBase `CREATE`/`ALL PRIVILEGES` on `*.*`) → Pass, "created on deploy";
  - otherwise → Unknown with the grant SQL.
  The code `<olap>_create_table` stays.
- The probe table also caught a `replication_num` above the live backends. The check now reads `SHOW BACKENDS`
  (StarRocks, Doris): more replicas than alive backends → Fail (definite); not allowed to ask → Unknown. New codes
  `starrocks_replicas`, `doris_replicas`.

### D5. Elasticsearch

- The connection check keeps sign-in and version; it no longer writes or deletes an index.
- A new `CheckPipeline` asks `_has_privileges` (`/_security/user/_has_privileges`; on 6.x
  `/_xpack/security/user/_has_privileges`) about the pipeline's own indexes (the routed destinations, as the sink
  names them), for `index`, `create_index`, `auto_configure` and `delete_index`. It never creates them.
  - No `index`, or neither `create_index` nor `auto_configure` → Fail, blocking (`elasticsearch_create_index`).
  - No `delete_index` → Warn: deleting the pipeline with "drop destinations" fails (as before).
  - Security disabled (a recognised `security_exception`/"Security must be explicitly enabled" answer) → Pass, saying
    there are no privileges to check. Any other error or answer → Unknown.
- Automatic index creation isn't read from cluster settings (needs `monitor`; templates override it); the fix names
  it.

### D6. JDBC

- **PostgreSQL:** `has_schema_privilege(<schema>, 'CREATE')` for the connection's schema (lower-cased as the sink
  names it; `current_schema()` when empty). A schema that doesn't exist (`to_regnamespace` is null): the
  database's `CREATE` right (`has_database_privilege`) decides, since the sink must create the schema.
- **SQL Server:** `HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')` and `HAS_PERMS_BY_NAME(<schema>, 'SCHEMA',
  'ALTER')` (the schema defaulting to `SCHEMA_NAME()`).
- **MySQL, MariaDB:** `SHOW GRANTS`: an exact `CREATE` or `ALL [PRIVILEGES]` token on `*.*` or a pattern matching the
  database → Pass; anything else → Unknown.
- A definite "no" stays a Warn (`jdbc_create_table`), as before: existing tables still work.

### D7. Older agents

Agents advertise a new feature, `readonly_destination_checks`. When preflight would send a destination check of one
of these four types to an agent without it, it runs the (read-only) check from the control plane instead, with the
existing `checked_from_control_plane` note saying why ("the agent predates read-only checks and would create test
tables; upgrade it"). No old code runs the creating check; results may differ inside the runtime's network, which the
note says.

### D8. Codes and titles

All four codes stay. Titles, whys and fixes that described creating are rewritten (old titles stay in `titles`); the
Elasticsearch `{probe}` fix and Doris's `DROP_PRIV` in the grant go. New codes: `starrocks_replicas`,
`doris_replicas`. The catalog test only sees literal codes, so the OLAP codes (built from `o.typ`) get a test that
loops over the three sinks.

### Implementation plan

One PR, one commit per step:

1. This ADR (index row; 0009's status notes the amendment).
2. **Iceberg:** `icebergWriteCheck` removed, `iceberg_write` Unknown; catalog text.
3. **Agent gate:** `runtime.FeatureReadonlyDestinationChecks`, advertised by the agent; `checkWhere` runs the four
   types from the control plane for agents without it.
4. **Elasticsearch:** connection check without the test index; `CheckPipeline` with `_has_privileges`; catalog text.
5. **JDBC:** PostgreSQL and SQL Server privilege queries, MySQL/MariaDB grant parsing; catalog text.
6. **OLAP:** `PrepareSink` (database), the read-only check (`SCHEMATA`, `SHOW GRANTS`), `<olap>_replicas`; catalog.
7. **Docs:** regenerated `docs/reference/checks.md`, `docs/README.md`, the connectors contract, the changelog fragment
   (with how to drop the `bezisync_check_*` objects earlier versions left).

### Verification plan

Grant and answer interpreters are pure functions, tested on outputs captured from real servers (StarRocks 3.4, Doris
3.0, OceanBase CE 4.3, MySQL 8.4, Elasticsearch 8.15 in the BeziSync lab, 2026-11-10). Checks that talk to a server
are tested against fakes that fail the test on any create (`httptest` for Iceberg REST and Elasticsearch); the
PostgreSQL check runs against the test database (`BEZISYNC_TEST_DATABASE_URL`) with a role lacking `CREATE`.

| # | Case | Expected | Test |
|---|---|---|---|
| E1 | Iceberg namespace exists | no create request; `iceberg_write` Unknown | `TestIcebergCheckCreatesNothing` |
| E2 | Iceberg namespace missing | as today ("created on deploy"), no create | `TestIcebergCheckCreatesNothing` |
| E3 | Agent without the feature, Iceberg/JDBC/OLAP/ES sink | checked from the control plane, note | `TestCheckWhereOlderAgent` |
| E4 | Agent without the feature, other sink | still checked on the agent | `TestCheckWhereOlderAgent` |
| E5 | ES all privileges | Pass, no index created | `TestElasticsearchPipelinePrivileges` |
| E6 | ES no `create_index`, has `auto_configure` | Pass | same |
| E7 | ES no `index` / no create rights | Fail, blocking | same |
| E8 | ES no `delete_index` | Warn | same |
| E9 | ES security disabled | Pass with note | same |
| E10 | ES 6.x | `_xpack` path | same |
| E11 | ES other error (403, proxy) | Unknown | same |
| E12 | ES connection check | no index written or deleted | `TestElasticsearchCheckCreatesNothing` |
| E13 | PostgreSQL schema with / without `CREATE` | Pass / Warn | `TestJDBCPostgresCreateRight` (test DB) |
| E14 | PostgreSQL schema missing | database `CREATE` decides | same |
| E15 | PostgreSQL schema case | lower-cased | same |
| E16 | MySQL `ALL PRIVILEGES ON *.*`, `CREATE ON db.*`, pattern `db\_%` | Pass | `TestMySQLGrantsAllowCreate` |
| E17 | MySQL `CREATE VIEW`/`CREATE ROUTINE` only, role-only, other db | Unknown | same |
| E18 | SQL Server query shape | both permissions asked | `TestSQLServerCreateRightQuery` |
| E19 | StarRocks grants (captured) | Pass on the database; Pass "created on deploy" with `CREATE DATABASE` | `TestOlapGrants` |
| E20 | Doris grants (captured), global and role-only | Pass / Unknown | same |
| E21 | OceanBase grants (captured) | Pass | same |
| E22 | OLAP replicas > alive backends | Fail; `SHOW BACKENDS` denied → Unknown | `TestOlapReplicas` |
| E23 | OLAP database created on deploy | `PrepareSink` exists/created | `TestOlapPrepareSink` |
| E24 | Every OLAP code in the catalog | entries for all three sinks | `TestOlapCodesInCatalog` |
| E25 | No connector check sends a create | grep-style test over the four check paths | `TestDestinationChecksCreateNothing` |

## Consequences

- **No check leaves or creates an object on a destination.** Least-privilege logins stop collecting
  `bezisync_check_*` tables; the objects earlier versions left can be dropped by hand (the changelog says how).
- **Detection moves later for Iceberg.** A catalog that refuses commits, storage keys that can read but not write, a
  wrong S3 endpoint: these show on the first change written, as a connector error, not before deploy. On a PostgreSQL
  source a failing pipeline holds WAL in its slot until fixed; preflight can no longer prevent that. The check says
  so as an Unknown note.
- **OLAP checks get weaker where grants are indirect.** A role-only or externally authorized login gets Unknown
  instead of the old blocking Fail when it can't create; the deploy then fails at table creation.
- **Elasticsearch privileges are checked against the real index names**, which fixes a login allowed only `cdc_*`
  failing a test index named `bezisync_check_*`; the connection check alone no longer decides index rights.
- **Older agents** get their destination checks run from the control plane until upgraded; results may differ
  inside the runtime's network.

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
- **Drop rights for the BeziSync login, or a nightly sweeper.** Destination-side workarounds every user would have to
  set up; the requester didn't want the objects created at all.
