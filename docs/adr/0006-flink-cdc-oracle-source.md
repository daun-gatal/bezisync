# 0006 — Flink CDC reads Oracle; MongoDB, Vitess, TiDB and OceanBase stay on Debezium

- **Status:** Accepted
- **Date:** 2026-11-07
- **Scope:** `backend/internal/engine` (Flink sources, capabilities, settings registry),
  `backend/internal/connectors` (`flinkcdc.go`, `oracle.go`), `backend/internal/compiler`
  (`flinkcdc.go`, the compile matrix), `backend/internal/services` (Flink preflight checks, driver
  property refusals), `backend/internal/checks/catalog.json` and `docs/reference/checks.md`, the
  MCP/CLI/OpenAPI texts that name Flink CDC's sources, e2e (`15-source-oracle-xe.yaml`,
  `32-flink-cdc.spec.ts`), the guide (`docs/README.md`, Flink CDC engine), the threat model, the
  engine/connectors/compiler/services contracts, `ARCHITECTURE.md`, `.rules/backend.md`,
  `.rules/testing.md`. The reference Flink CDC image (`daun-gatal/image-repo`, `flink-cdc/`) adds
  the Oracle connector in its own pull request.

## Context

Flink CDC is BeziSync's optional second engine. It reads MySQL/MariaDB, PostgreSQL and SQL Server
into Iceberg, StarRocks, Doris and Fluss. The request was to add MongoDB, Oracle and Vitess, and
TiDB and OceanBase if possible. BeziSync already reads all five except TiDB and OceanBase with
Debezium.

BeziSync runs Flink CDC one way only: as a **YAML pipeline** (`pipeline.yaml` submitted by
`flink-cdc-dist`'s CLI). That gives BeziSync routes, transforms, schema changes applied to the
destination and saved positions. A source can join only if Flink CDC has a *pipeline source
connector* for it. The research (sources, documentation and a live sandbox; notes and copies in
`.egm/research/flink-oracle/`) found:

1. **Only Oracle has a pipeline source in Flink CDC 3.7.0** (`flink-cdc-pipeline-connector-oracle`,
   since 3.6). Neither the release nor upstream `master` has a MongoDB, Vitess or TiDB pipeline
   source. Those exist only as Flink SQL / DataStream connectors (`flink-sql-connector-*-cdc`).
   OceanBase's pipeline connector is a **sink**. OceanBase's source connector now reads only the
   OceanBase Binlog Service, which speaks the MySQL replication protocol.
2. The Oracle source embeds **Debezium 1.9.8** (LogMiner) and its own ojdbc8. It accepts a fixed
   set of options and fails the job on any other key: there is no `scan.newly-added-table.enabled`,
   `schema-change.enabled` or `heartbeat.interval` (schema changes are always on). Its startup modes
   are `initial`, `latest-offset` and `snapshot`, with **no start from a time**.
3. **The version banner decides whether it connects at all.** Debezium 1.9.8 reads
   `V$VERSION.BANNER_FULL LIKE 'Oracle Database%'` and matches it against two fixed patterns.
   Enterprise, Standard and Express Edition banners (`… - Production` plus a `Version` line) match.
   **Oracle Database Free (every release) and Oracle AI Database 26ai do not**, so the job fails
   with "Failed to resolve Oracle database version". This was measured on XE 21c (works), Free 23.9
   (fails) and Free 23.26 / 26ai (fails). BeziSync's own e2e Oracle fixture is 26ai.
4. **Multitenant needs two workarounds.** At job build time the source lists tables over its own
   connection. With a CDB and a PDB that connection lands in the CDB root, which has no PDB tables,
   so the job fails with "Cannot find any table". Pointing `jdbc.url` at the PDB service, with
   `debezium.database.pdb.name`, fixes the listing (Debezium moves the mining session to the root
   itself). Debezium 1.9 then creates its `LOG_MINING_FLUSH` table in the PDB but writes it from
   the root, which fails with ORA-00942. The fix is to create
   `<user>.LOG_MINING_FLUSH (LAST_SCN NUMBER(19,0))` with one row in the CDB root, which is what
   Debezium Server's Oracle connector does when it signs in there.
5. **The first copy runs `ANALYZE TABLE … COMPUTE STATISTICS` on every table** (the chunk
   splitter's row estimate). It needs `ANALYZE ANY`, which Flink CDC's CDB grant list omits. Without
   it the copy fails with ORA-01031. The statement reads each table in full and replaces its
   optimizer statistics.
6. **A table added to a running pipeline is not copied.** Measured: savepoint, then restore with
   one more table in `tables`. That table's existing rows never arrived; only rows written
   afterwards did. Nothing reports the loss.
7. **Renaming or dropping a column stops the reader.** After `RENAME COLUMN` or `DROP COLUMN`,
   LogMiner can't rebuild the redo of that table ("Oracle failed to re-construct redo SQL",
   columns shown as `COL 1`, `COL 2`), and the job fails. This happened with both the default
   `online_catalog` strategy and `redo_log_catalog`, even with the reader caught up between the
   steps. ADD COLUMN works (`AddColumnEvent`).
8. Everything else measured works: the parallel first copy (ROWID chunks), inserts, updates with
   the old row, deletes, stop-with-savepoint and restore (also with a transaction open across the
   savepoint: its rows arrive after the restore), upper-case `SCHEMA.TABLE` names in `tables` and
   routes, and the `debezium.heartbeat.interval.ms` and `debezium.lob.enabled` pass-throughs. It
   needs `classloader.resolve-order: parent-first`, which BeziSync already sets.

Why it matters: Oracle is where much operational data lives, and StarRocks, Doris and Fluss are
Flink-only destinations. Today an Oracle table can't reach them through BeziSync at all, and
reaching Iceberg means Debezium's single-reader copy.

### Goals and non-goals

- **Goals:** Oracle → Iceberg, StarRocks, Doris and Fluss on Flink CDC, with the same wizard,
  checks, positions and operations as the other Flink sources. Every upstream limit found above is
  either worked around or caught by preflight or health, with the cause named.
- **Non-goals:** MongoDB, Vitess, TiDB and OceanBase on Flink CDC (see *Alternatives considered*);
  Oracle Database Free and 26ai on Flink CDC; reading Flink's encrypted saved state to measure how
  much redo a Flink pipeline still needs; any change to Debezium pipelines.

## Decision

**Flink CDC reads Oracle.** MongoDB, Vitess, TiDB and OceanBase are not offered on Flink CDC, for
the reasons under *Alternatives considered*. The guide names them and says why. Everything below is
Oracle-only unless it says otherwise. The rules marked *(review)* came from the Goldfish review.

### Rules

1. **Engine table.** `oracle` joins `engine.flinkSources` (`engine/engine.go`). It is offered
   wherever Flink CDC is: the Flink-only destinations, and Iceberg as a choice (Debezium stays the
   default, `engine.Default`). Existing Oracle pipelines keep their stored engine.
2. **Capabilities** (`engine.CapsFor(FlinkCDC, "oracle")`, now per source): `StartFromTime` false
   ("Flink CDC's Oracle reader starts only from a full copy or from now") and `AddTables` false
   (Rule 8). The other sources keep theirs and their own `Why` text.
3. **Source block** (`connectors.oracleSource.FlinkSource`, new, `connectors/oracle_flink.go`), in
   this order: `type: oracle`, `hostname`, `port` (the connection's, or `bz-tunnel` and the tunnel
   port), `username`, `password: ${BZ_SRC_PASSWORD}`, `database` (the connection's database: the
   CDB of a multitenant one), `jdbc.url: jdbc:oracle:thin:@//<host>:<port>/<service>` (service =
   `oraConfig.tableService`: the PDB's service name, else the PDB, else the database), `tables`
   (the spec's names, joined by commas), then `debezium.database.pdb.name: <PDB upper case>` when a
   PDB is set, `debezium.lob.enabled: true|false` (always written), and
   `debezium.heartbeat.interval.ms` (seconds × 1000) when the heartbeat policy is above 0. The
   compiler adds `scan.startup.mode`, the fetch size and, when not the default, the chunk size.
   `server-time-zone` is left unset (the TaskManager's zone, UTC in the image). The connector JAR is
   `oracle`, and no driver is added. `FlinkTableID` is the spec's table as given (`SCHEMA.TABLE`),
   as for PostgreSQL. The keys BeziSync writes are owned, so raw options can't override them (the
   existing `managed` map).
4. **Names and the URL** *(review: host)*. Oracle tables on Flink CDC must match
   `^[A-Z0-9_]+$` in both parts (Oracle's unquoted names), checked in `FlinkSource` after the
   shared `FlinkTableNames`; otherwise `422 flink_table_name`, naming the table and pointing to
   Debezium. The URL is built by `oraFlinkURL`. The service and the database must match
   `^[A-Za-z0-9_.$#-]+$`. The host must be a host name (`^[A-Za-z0-9.-]+$`) or an IP address (IPv6
   written in brackets). The port must be 1 to 65535. Anything else fails with
   `422 flink_oracle_service`, so a host or service can't add a path, `?` parameters or a TNS
   descriptor to the URL.
5. **Raw options** *(review)*. `connectors.FlinkSourceIgnores(sourceType, option)` (new) returns
   why `jdbc.properties.*` does nothing on Oracle. It is applied in four places: on save
   (`services.DriverPropertyRefusal`, which `refuseDriverProps` already calls), in the classify
   endpoint (`api/settings.go`, Flink branch), in preflight (`advancedCheck`, now given the source
   type) and at compile (`compiler.CompileFlink`). The embedded engine is Debezium 1.9, so
   `compiler.ClassifyFlinkAdvanced` also refuses, for **every** Flink source, the 1.9 keys that
   load classes or send data elsewhere: `debezium.database.history*`, `debezium.rac.nodes` (opens
   sessions with the source's credentials to the hosts it names) and
   `debezium.database.connection.adapter`. This closes a gap that also existed for MySQL,
   PostgreSQL and SQL Server. The typed settings *connection pool size*, *connect timeout* and
   *connect attempts* apply to Oracle.
6. **Prepare and Cleanup** (`connectors/oracle.go`, Oracle only, `CompileContext.Engine ==
   "flink-cdc"`, which the controller sets for both). On Flink, Prepare creates no signal or canary
   table. With a PDB, it connects to the CDB root (`cfg.Database`) and, in the connector user's
   schema (`cfg.owner()`), creates `LOG_MINING_FLUSH (LAST_SCN NUMBER(19,0))` when it is missing
   (ORA-00955 from a concurrent Prepare counts as present). It then **always** runs
   `INSERT … SELECT 0 FROM dual WHERE NOT EXISTS (…)` *(review)*, so a table left empty (by a DBA,
   or a Prepare that failed after `CREATE`) gets its row. go-ora runs each statement with
   autocommit. A failing Prepare fails the deploy job, which can be retried. Cleanup on Flink drops
   nothing: the root table belongs to the user and is shared by every pipeline mining as that user
   on either engine. Debezium 1.9 also leaves its own copy in the PDB, which discovery already
   hides. The guide names both. Debezium pipelines are unchanged.
7. **Preflight** (`connectors.oracleSource.FlinkChecks(ctx, conn, OraFlinkFacts)`, reached through
   the new optional interface `connectors.FlinkSourceChecker` from `services.FlinkChecks`). It runs
   from BeziSync, as the Flink table checks already do. The source's own `ora_login` check already
   blocks an unreachable database, so this one only warns.
   - `flink_oracle_version`, **config section** (a forced deploy can't skip it, because the job can
     only fail): reads `V$VERSION` in the root (`BANNER_FULL`, falling back to `BANNER` on
     ORA-00904), then `connectors.FlinkReadsOracleBanner(banner, found)`. That function is a Go
     copy of Debezium 1.9.8's rule: the banner starts with `Oracle Database` and fully matches one
     of the two patterns, with `.` excluding `\r` and `\n` *(review)*. ORA-00942/01031 on
     `V$VERSION` blocks too, with the grant `SELECT_CATALOG_ROLE`. The fix names the banner and says
     to use Debezium (Iceberg and event destinations) or an Enterprise, Standard or Express Edition
     up to 23ai.
   - `flink_oracle_analyze`, engine section *(review)*. It passes when the user has `ANALYZE ANY`
     in the tables' container, or owns every captured table. It fails (blocking) otherwise, with
     the least grant: `GRANT ANALYZE ANY TO <user>` run inside the PDB, not `CONTAINER=ALL`. When
     the pipeline will not copy (`no_data`), it is a warning instead: a later Start over that copies
     needs it. Tables whose statistics are locked (`ALL_TAB_STATISTICS.STATTYPE_LOCKED`) block,
     because ANALYZE fails on them with ORA-38029; the fix is `DBMS_STATS.UNLOCK_TABLE_STATS`. When
     this deploy will copy, and no saved position is left to restore, a passing check is a
     **warning** that the copy runs `ANALYZE TABLE … COMPUTE STATISTICS` on each table, reading it
     in full and replacing its optimizer statistics.
   - `flink_oracle_flush_table` (PDB only), engine section: passes when the table is in the root,
     or the user has `CREATE TABLE` there and room (`UNLIMITED TABLESPACE`, or quota on its default
     tablespace). Otherwise it blocks, with the DBA's `CREATE TABLE … ; INSERT … VALUES (0);
     COMMIT;` in the root. On managed services where nobody may create objects in the root (some
     RDS and OCI multitenant set-ups), this check is what says so.
   - `flink_oracle_unknown`: a warning when the database can't be reached or a query fails.
   - **Engine-aware source checks** *(review)*: on Flink pipelines `services` drops two Debezium
     checks from the Oracle source report. `ora_managed_tables` is about the signal table's quota in
     the PDB, and Flink has no signal table (the flush check covers the root). `ora_undo` is about
     Debezium's flashback copy; Flink copies with plain chunk reads plus redo.
8. **Added tables** *(review: baseline, restore)*. `flink_tables_added`, config section, for
   sources whose `AddTables` capability is false. Its baseline is
   `domain.SummarizeChange(deployed, spec).TablesAdded`. With no deployed version, or nothing
   added, there is no check. With tables added and a saved position left (`positionLeft`), it
   blocks with `flinkReadersCheck`'s wording pattern. The fix offers pausing and Start over (which
   copies every table again), or capturing the new tables in a second pipeline. With nothing left
   to restore it passes, saying the pipeline starts as Start over chose. When the saved positions
   can't be read, it blocks and says so. **Restore:** when a deploy adds tables to such a pipeline,
   BeziSync stores `artifacts.flink_tables_floor`, the newest saved-position id at that moment.
   `RestoreSavedPosition` refuses (`409 position_before_tables`) a position at or below the floor,
   because it was saved before the added tables were captured. The comment in `services.Deploy`
   that says Flink CDC copies added tables itself is corrected.
9. **Health** *(review)*. `flinkhelper`'s table of failures that don't heal gains Oracle's, each
   stopping the job with a named reason instead of a restart loop: `oracle_version`
   ("Failed to resolve Oracle database version"), `oracle_analyze` (ORA-01031 in the chunk
   splitter), `oracle_flush_table` ("Failed to flush Oracle LogWriter"), `oracle_schema_change`
   ("Oracle failed to re-construct redo SQL": a column was renamed or dropped; Start over), and
   `tables_missing` ("Cannot find any table by the option 'tables'").
10. **Source protection** *(review)*. `oracleSource.Probe` still reports the archive's retention
    window. A Flink pipeline's SCN is inside its encrypted saved state, so the summary says that how
    much redo it still needs isn't measured, the MySQL precedent (`mysql.go`). No "lost" verdict is
    claimed without evidence.
11. **Delivery canary** *(review; a pre-existing bug, fixed here for every Flink source)*. Flink CDC
    captures only the spec's tables, so a canary row never reaches the destination. Preflight's
    `delivery_canary_unavailable` warns on every Flink pipeline that has the policy, and
    `controller.canary` skips Flink pipelines.
12. **Unchanged.** Every existing spec compiles to the same `config_hash`: no existing golden row
    changes. The new matrix rows use Oracle-cased names.

### Implementation plan

One PR to `preview`. Each commit is gated by `make verify-quick GO_PKGS=…` on its packages (and
the e2e specs it touches) and pushed as it lands.

1. **ADR** — create `docs/adr/0006-flink-cdc-oracle-source.md`; modify `docs/adr/README.md`.
2. **Flink CDC reads Oracle** (Rules 1–6) — modify `backend/internal/engine/{engine.go,caps.go,settings.go,README.md,engine_test.go,settings_test.go}`;
   create `backend/internal/connectors/oracle_flink.go`, `backend/internal/connectors/oracle_flink_test.go`;
   modify `backend/internal/connectors/{oracle.go,flinkcdc.go,README.md}`, `backend/internal/compiler/{flinkcdc.go,flinkadvanced.go,flinkcdc_test.go,matrix_test.go,README.md}`,
   `backend/testdata/compile_matrix.json`; create `backend/internal/compiler/flinkoracle_test.go`, `backend/internal/compiler/testdata/flink/oracle-cdb.yaml`;
   modify `backend/internal/services/pipelines.go` (`DriverPropertyRefusal`, `advancedCheck`), `backend/internal/api/settings.go`.
3. **Preflight, restore and health** (Rules 7–10) — modify `backend/internal/services/{flinkchecks.go,cluster.go,pipelines.go,flinkcdc.go,README.md}`
   and tests; `backend/internal/connectors/oracle.go` (Probe); `backend/internal/flinkhelper/{jobmanager.go,jobmanager_test.go,README.md}`;
   `backend/internal/checks/catalog.json`; regenerate `docs/reference/checks.md` (`bezisync check-docs`).
4. **Delivery canary on Flink** (Rule 11) — modify `backend/internal/services/pipelines.go`,
   `backend/internal/controller/controller.go`, tests.
5. **Docs** — modify `docs/README.md` (Flink CDC engine, the image layout), `docs/security/threat-model.md`,
   `ARCHITECTURE.md`, `.rules/backend.md`, `.rules/testing.md`; create `changelogs/unreleased/<pr>-flink-cdc-oracle.md`.
6. **e2e** — create `deploy/e2e/fixtures/15-source-oracle-xe.yaml`: `gvenzl/oracle-xe:21-slim-faststart`
   pinned by digest; CDB `XE` and PDB `XEPDB1`; ARCHIVELOG; supplemental logging; the e2e common
   user with the Debezium grants plus `ANALYZE ANY`; `LOG_MINING_FLUSH` **not** pre-created, so
   Prepare is proved; 2 GiB. It is applied with the other fixtures. Modify `e2e/tests/fixtures.ts`
   (`ORACLE_XE`); move the sqlplus writer from `07-sources.spec.ts` into `e2e/tests/data.ts`; modify
   `e2e/tests/32-flink-cdc.spec.ts`. The Flink image is the reference Dockerfile with the Oracle
   connector (the image-repo pull request), built locally until it merges.

The frontend needs no change: engine sources come from `/api/v1/engines`, and Start over reads
`caps.start_from_time`. Added tables are caught by preflight, not by the Tables step.

### Verification plan

- Unit: `engine`, `connectors`, `compiler` (golden matrix, every existing row unchanged), `services`,
  `flinkhelper`, `controller`. Live Oracle tests use `BEZISYNC_TEST_ORACLE` (skipped without it),
  run against an Oracle XE 21c container.
- e2e with `E2E_FLINK_IMAGE`: `32-flink-cdc` (the rows below) and `07-sources` (Debezium Oracle
  still streams).

| ID | Case | Expected | Commit · test |
|---|---|---|---|
| E1 | CDB + PDB connection | `database` = CDB, `jdbc.url` → PDB service (`service_name` when set), `pdb.name` = PDB upper case | 2 · `TestFlinkOracleSource` (golden `oracle-cdb.yaml`) |
| E2 | Non-CDB connection | `jdbc.url` → the database's service, no `pdb.name` | 2 · `TestFlinkOracleSource` |
| E3 | SSH tunnel | `hostname` and `jdbc.url` use `bz-tunnel:<TunnelPort>` | 2 · `TestFlinkOracleTunnel` |
| E4 | Lower/mixed-case or `$` table name | `422 flink_table_name` naming the table, pointing to Debezium | 2 · `TestFlinkOracleNames` |
| E5 | Hostile service, database or host; IPv6 host | `422 flink_oracle_service`; IPv6 in brackets | 2 · `TestFlinkOracleNames` |
| E6 | Snapshot modes | initial/when_needed → `initial`, no_data → `latest-offset`, initial_only → `snapshot`, always refused | 2 · `TestFlinkOracleSource` |
| E7 | Start from a time | compile refuses `timestamp` (`engine_unsupported`); `CapsFor` reason names Oracle | 2 · `TestFlinkOracleSource`, `TestCapsFlinkOracle` |
| E8 | Heartbeat policy | `debezium.heartbeat.interval.ms` in ms; absent at 0; never `heartbeat.interval` | 2 · `TestFlinkOracleSource` |
| E9 | Tuning | pool/timeout/attempts offered; WAL delay and open-ended chunk not | 2 · `TestSettingsForFlinkOracle` |
| E10 | Raw options | `source.jdbc.properties.*` refused on Oracle (save, classify, preflight, compile), allowed on MySQL; Debezium 1.9 history, `rac.nodes`, connection adapter refused on every Flink source | 2 · `TestFlinkOracleRawOptions`, `TestClassifyFlinkAdvancedDebezium19`, `TestDriverPropertyRefusalFlinkOracle` |
| E11 | Banners | XE 21c, EE 19c/23ai (`\n` and `\r\n`) pass; Free 23.9, 26ai (no row), garbage fail | 2 · `TestFlinkReadsOracleBanner` |
| E12 | ANALYZE | privilege or ownership passes; missing blocks with the PDB grant; `no_data` warns; locked statistics block; copy warns of statistics | 3 · `TestOracleFlinkChecksLive` + `TestAnalyzeCheck` (pure) |
| E13 | Flush table | exists / creatable pass; not creatable blocks with SQL; non-CDB has no check | 3 · `TestOracleFlinkChecksLive` |
| E14 | Unreachable | `flink_oracle_unknown` warning only | 3 · `TestOracleFlinkChecksUnreachable` |
| E15 | Prepare on Flink + PDB | creates the root table with one row, twice is idempotent, an emptied table gets its row back; no signal/canary table | 2 · `TestOraclePrepareFlinkLive` |
| E16 | Cleanup on Flink | drops nothing | 2 · `TestOraclePrepareFlinkLive` |
| E17 | Added tables | none deployed / nothing added → no check; added + position left → block; added + nothing left → pass; positions unreadable → block; MySQL/PG/SQL Server → no check; forced deploy still blocked | 3 · `TestFlinkTablesAddedCheck` + e2e |
| E18 | Restore below the floor | `409 position_before_tables`; above the floor works; deploy without added tables stores no floor | 3 · `TestRestoreBelowTablesFloor` |
| E19 | Existing specs | identical `config_hash` (no existing golden row changes) | 2 · `TestCompileMatrixUnchanged` |
| E20 | Debezium Oracle | unchanged compile, Prepare and checks | 2 · golden + `07-sources` |
| E21 | Health hints | each Oracle failure text maps to its code and stops the job | 3 · `TestFatalOracle` |
| E22 | Probe on Flink | retention reported, no `lost`, summary says the position isn't measured | 3 · `TestOracleProbeFlinkSummary` |
| E23 | Canary on Flink | preflight warns, controller writes no canary; Debezium unchanged | 4 · `TestCanaryFlink`, `TestDeliveryCanaryWarnFlink` |
| E24 | Oracle 26ai source on Flink | `flink_oracle_version` fails, in the config section | 6 · e2e (26ai fixture) |
| E25 | Oracle XE → Iceberg on Flink | Prepare creates the flush table; first copy; a streamed insert readable through BeziSync; wizard walk round-trips every step | 6 · e2e |
| E26 | Image without `oracle.jar` | the helper names `/opt/flink-cdc/connectors/oracle.jar` | existing `TestPrepareExplainsWhatTheImageLacks` (generic over the connector name) |
| E27 | Roles | no new route or permission; the classify endpoint keeps its authz tests | existing `api` tests |

### Risks and mitigations

| Risk | Mitigation |
|---|---|
| A column rename or drop stops the pipeline (upstream, Rule 9) | Health names it (`oracle_schema_change`) with the fix (Start over); the guide warns before you choose Flink for tables whose columns get renamed |
| Bare `NUMBER` is written as DECIMAL(38,19): more than 19 integer digits overflow, more than 19 fractional digits are rounded | The guide names the mapping; columns declared `NUMBER(p,s)` keep their precision |
| ANALYZE replaces optimizer statistics on production tables | Preflight warns before every deploy that copies; the guide says it; the least grant is per PDB |
| A database moves to a 26ai release update after deploy | The next restart fails with `oracle_version` named in health; the guide says Flink reads up to 23ai |
| 23ai Enterprise Edition is accepted by the banner rule but not measured end to end | The guide says what was measured (XE 21c) |
| Many pipelines mining as one user share one flush-table row | The same table Debezium Server uses; row updates are short. Accepted. |
| The shared root object is left behind on uninstall | The guide names it and the drop statement |

## Consequences

- Oracle tables can be synced into StarRocks, Doris and Fluss, and into Iceberg with parallel first
  copies, ADD COLUMN applied, and restorable positions.
- Oracle on Flink CDC is narrower than on Debezium: Enterprise, Standard or Express Edition up to
  23ai (not Free, not 26ai), upper-case names, no start from a time, no adding tables without a
  Start over, and renamed or dropped columns stop it. The guide says so; preflight or health names
  each case.
- DBAs give one more grant (`ANALYZE ANY` in the PDB) and, on multitenant databases, BeziSync owns
  one more object in the CDB root (`LOG_MINING_FLUSH`, shared with Debezium).
- Raw Debezium 1.9 keys that load classes or open connections are now refused on every Flink
  source. A pipeline that already used one fails its next deploy's preflight with the reason.
- The banner rule copies Debezium 1.9.8's. A Flink CDC release with a newer Debezium means
  revisiting it, together with the image's version.
- The e2e suite gains a second Oracle fixture (XE 21c, about 2 GiB), because the current one is a
  release Flink CDC can't read.
- The delivery canary no longer pretends to run on Flink CDC pipelines.

## Alternatives considered

- **Run MongoDB, Vitess and TiDB as Flink SQL jobs** (`CREATE TABLE … WITH ('connector'='mongodb-cdc')`
  plus `INSERT INTO` a sink table). Rejected: it is a second job model inside the engine. It needs
  different sink connectors for Flink 2.2 (some don't exist for 2.x), declared column lists (MongoDB
  has no schema), no schema-change propagation, no routes or transforms, and its own submission and
  savepoint handling. It would offer a weaker product under the same name.
- **Write pipeline source adapters in Java** around those DataStream sources. Rejected: BeziSync
  ships Go only and publishes no Flink image. Users build their own image, so BeziSync can't put
  code into it. Such adapters belong upstream in Flink CDC.
- **OceanBase through the MySQL source and its Binlog Service.** Possible in principle: the
  Binlog Service speaks the MySQL replication protocol. Rejected for now: BeziSync has no OceanBase
  connection type, and the MySQL checks (binlog format, GTIDs, server ids, privileges) were not
  shown to hold against it. An OceanBase cluster with its Binlog Service also can't be run in the
  e2e suite to prove it, so it would ship unverified.
- **TiDB through the MySQL source.** Rejected: TiDB serves no MySQL binlog stream (changes leave
  through TiCDC), so the MySQL source can't read it.
- **Let Oracle 26ai and Free through and let the job fail.** Rejected: health would show a failing
  cluster with a Java stack trace, instead of a preflight check that names the cause before deploy.
- **Silently re-copy every table when a table is added** (force a Start over on deploy). Rejected:
  a full re-copy is disruptive and must be the user's choice. The check names it and blocks instead.
- **Use `redo_log_catalog` to survive column renames.** Rejected: measured, it fails the same way.
- **Restrict Oracle on Flink to "from now" so ANALYZE never runs.** Rejected: the first copy is the
  main reason to choose Flink CDC. The statistics effect is shown instead, and copying is the
  user's choice.
- **Point `database` at the PDB and drop `pdb.name`.** Rejected: LogMiner then runs inside the PDB.
  Its support differs by release and wasn't measured. The measured setup is the CDB plus a PDB URL.
