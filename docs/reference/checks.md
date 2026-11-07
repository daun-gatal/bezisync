# Preflight check reference

Every check BeziSync runs on connections, runtimes and deployments. Generated from the
source by `bezisync check-docs` — do not edit by hand. In the UI each check links here.

## PostgreSQL

<a id="pg-capacity"></a>
### `pg_capacity`

**Replication capacity**

Fix:

```
-- drop slots nobody uses:
SELECT slot_name, active FROM pg_replication_slots;
-- or raise the limits (restart required):
ALTER SYSTEM SET max_replication_slots = 20;
ALTER SYSTEM SET max_wal_senders = 20;
```

<a id="pg-create"></a>
### `pg_create`

**Manage publications**

Fix:

```
GRANT CREATE ON DATABASE {db} TO {user};
```

<a id="pg-failover"></a>
### `pg_failover`

**Failover safety**

<a id="pg-login"></a>
### `pg_login`

**Sign in**

Fix:

```
# pg_hba.conf
hostssl {database} {user} <runtime-cidr> scram-sha-256
hostssl replication  <runtime-cidr> scram-sha-256
```

<a id="pg-network-blocked"></a>
### `pg_network_blocked`

**Reach the database**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="pg-network-reachable"></a>
### `pg_network_reachable`

**Reach the database**

- Why: The host answered on the port.

<a id="pg-network-unreachable"></a>
### `pg_network_unreachable`

**Reach the database**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="pg-replication-role"></a>
### `pg_replication_role`

**Replication permission**

<a id="pg-ssh"></a>
### `pg_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="pg-ssh-blocked"></a>
### `pg_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="pg-ssh-reachable"></a>
### `pg_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="pg-ssh-tunnel"></a>
### `pg_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="pg-ssh-unreachable"></a>
### `pg_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="pg-tables"></a>
### `pg_tables`

**Selected tables**

<a id="pg-tables-exist"></a>
### `pg_tables_exist`

**Selected tables exist**

<a id="pg-tables-identity"></a>
### `pg_tables_identity`

**Replica identity**

<a id="pg-tables-keys"></a>
### `pg_tables_keys`

**Row keys**

<a id="pg-tables-owner"></a>
### `pg_tables_owner`

**Publication ownership**

Fix:

```
CREATE PUBLICATION <pipeline-publication> FOR TABLE ;
```

<a id="pg-tables-select"></a>
### `pg_tables_select`

**Read access**

- Why: The initial snapshot reads existing rows with SELECT.

<a id="pg-tls"></a>
### `pg_tls`

**TLS**

- Why: Check the TLS mode and CA certificate for this database.

<a id="pg-version"></a>
### `pg_version`

**PostgreSQL version**

<a id="pg-wal-level"></a>
### `pg_wal_level`

**Logical replication**

- Why: Debezium reads changes from the database log. The setting needs a restart, so plan it.

<a id="pg-wal-retention"></a>
### `pg_wal_retention`

**WAL retention limit**

- Why: A stopped pipeline's slot can fill the disk. BeziSync alerts on retained WAL; a cap protects the database but invalidates the slot (re-snapshot needed) when reached.

Fix:

```
ALTER SYSTEM SET max_slot_wal_keep_size = '50GB';
SELECT pg_reload_conf();
```

## MySQL / MariaDB

<a id="mysql-binlog"></a>
### `mysql_binlog`

**Binary log**

Fix:

```
# my.cnf
[mysqld]
log_bin = mysql-bin
server_id = 1
binlog_format = ROW
```

<a id="mysql-binlog-format"></a>
### `mysql_binlog_format`

**Row-based binlog**

<a id="mysql-grants"></a>
### `mysql_grants`

**Permissions**

Fix:

```
GRANT {REQUIRED_PRIVILEGES} ON *.* TO '{user}'@'%';
GRANT ALL ON {MANAGED_DB}.* TO '{user}'@'%';
```

<a id="mysql-gtid"></a>
### `mysql_gtid`

**Failover safety**

Fix:

```
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = OFF_PERMISSIVE;
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL gtid_mode = ON;
```

<a id="mysql-login"></a>
### `mysql_login`

**Sign in**

<a id="mysql-network-blocked"></a>
### `mysql_network_blocked`

**Reach the database**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="mysql-network-reachable"></a>
### `mysql_network_reachable`

**Reach the database**

- Why: The host answered on the port.

<a id="mysql-network-unreachable"></a>
### `mysql_network_unreachable`

**Reach the database**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="mysql-retention"></a>
### `mysql_retention`

**Binlog retention**

Fix:

```
SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7 days
```

Fix:

```
CALL mysql.rds_set_configuration('binlog retention hours', 72);
```

<a id="mysql-row-image"></a>
### `mysql_row_image`

**Full row images**

Fix:

```
SET GLOBAL binlog_row_image = 'FULL';
```

<a id="mysql-ssh"></a>
### `mysql_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="mysql-ssh-blocked"></a>
### `mysql_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="mysql-ssh-reachable"></a>
### `mysql_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="mysql-ssh-tunnel"></a>
### `mysql_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="mysql-ssh-unreachable"></a>
### `mysql_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="mysql-tables"></a>
### `mysql_tables`

**Selected tables**

<a id="mysql-tables-exist"></a>
### `mysql_tables_exist`

**Selected tables exist**

<a id="mysql-tables-keys"></a>
### `mysql_tables_keys`

**Row keys**

<a id="mysql-version"></a>
### `mysql_version`

**Server version**

## SQL Server

<a id="mssql-agent"></a>
### `mssql_agent`

**SQL Server Agent**

Fix:

```
Start the SQL Server Agent service (and set it to start automatically).
```

<a id="mssql-cdc-db"></a>
### `mssql_cdc_db`

**Change data capture**

- Why: Debezium reads SQL Server's CDC change tables.

<a id="mssql-login"></a>
### `mssql_login`

**Sign in**

<a id="mssql-network-blocked"></a>
### `mssql_network_blocked`

**Reach the database**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="mssql-network-reachable"></a>
### `mssql_network_reachable`

**Reach the database**

- Why: The host answered on the port.

<a id="mssql-network-unreachable"></a>
### `mssql_network_unreachable`

**Reach the database**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="mssql-permissions"></a>
### `mssql_permissions`

**Permissions**

Fix:

```
ALTER ROLE db_owner ADD MEMBER {user};
```

<a id="mssql-ssh"></a>
### `mssql_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="mssql-ssh-blocked"></a>
### `mssql_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="mssql-ssh-reachable"></a>
### `mssql_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="mssql-ssh-tunnel"></a>
### `mssql_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="mssql-ssh-unreachable"></a>
### `mssql_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="mssql-tables"></a>
### `mssql_tables`

**Selected tables**

<a id="mssql-tables-cdc"></a>
### `mssql_tables_cdc`

**CDC on tables**

<a id="mssql-tables-exist"></a>
### `mssql_tables_exist`

**Selected tables exist**

<a id="mssql-tables-keys"></a>
### `mssql_tables_keys`

**Row keys**

## MongoDB

<a id="mongo-change-stream"></a>
### `mongo_change_stream`

**Read change streams**

Fix:

```
db.grantRolesToUser('<user>', [{ role: 'read', db: '<database>' }, { role: 'readAnyDatabase', db: 'admin' }])
```

<a id="mongo-collections-exist"></a>
### `mongo_collections_exist`

**Selected collections exist**

<a id="mongo-connect"></a>
### `mongo_connect`

**Connect**

- Why: For replica sets, every member's advertised hostname must resolve from the runtime.

<a id="mongo-login"></a>
### `mongo_login`

**Sign in**

<a id="mongo-network-blocked"></a>
### `mongo_network_blocked`

**Reach MongoDB**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="mongo-network-reachable"></a>
### `mongo_network_reachable`

**Reach MongoDB**

- Why: The host answered on the port.

<a id="mongo-network-unreachable"></a>
### `mongo_network_unreachable`

**Reach MongoDB**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="mongo-topology"></a>
### `mongo_topology`

**Change streams available**

Fix:

```
mongod --replSet rs0 …
mongosh --eval 'rs.initiate()'
```

<a id="mongo-version"></a>
### `mongo_version`

**MongoDB version**

## Oracle

<a id="ora-archive-retention"></a>
### `ora_archive_retention`

**Archived log retention**

- Why: A pipeline that stops for longer than the archived logs reach back loses changes. Keep at least a day (RMAN deletion policy, recovery area size; on RDS 'archivelog retention hours').

Fix:

```
BEGIN rdsadmin.rdsadmin_util.set_configuration('archivelog retention hours', 24); END;
/
```

<a id="ora-archivelog"></a>
### `ora_archivelog`

**Archive log mode**

- Why: LogMiner reads archived redo logs; without ARCHIVELOG, redo is overwritten before Debezium reads it. Switching modes restarts the database.
- Why: Amazon RDS and OCI Base Database run in ARCHIVELOG mode while automated backups are on.

Fix:

```
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;   -- multitenant databases only
```

Fix:

```
aws rds modify-db-instance --db-instance-identifier <your-instance> --backup-retention-period 7 --apply-immediately
```

<a id="ora-login"></a>
### `ora_login`

**Sign in**

- Why: Debezium signs in to the database's own service (the CDB root of a multitenant database).
- Why: ORA-01017: wrong user name or password; ORA-28000: the account is locked; ORA-12514: the listener doesn't know the service name.

Fix:

```
ALTER USER {user} ACCOUNT UNLOCK;
```

Fix:

```
lsnrctl services   # lists the service names this listener serves
```

<a id="ora-managed-tables"></a>
### `ora_managed_tables`

**BeziSync tables**

- Why: Snapshots on demand go through BZ_SIGNAL_<id> (and the delivery canary through BZ_CANARY_<id>) in the connector user's schema: the user needs a quota on its default tablespace.

Fix:

```
ALTER USER {user} QUOTA 100M ON {tablespace};
```

<a id="ora-network-blocked"></a>
### `ora_network_blocked`

**Reach the database**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="ora-network-reachable"></a>
### `ora_network_reachable`

**Reach the database**

- Why: The host answered on the port.

<a id="ora-network-unreachable"></a>
### `ora_network_unreachable`

**Reach the database**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="ora-pdb"></a>
### `ora_pdb`

**Pluggable database**

- Why: BeziSync reads the tables through the PDB's service; it must open the PDB named in the connection.
- Why: Set the PDB service name (Expert) when it differs from the PDB's name.

<a id="ora-privileges"></a>
### `ora_privileges`

**Permissions**

- Why: Debezium reads the redo log with LogMiner and existing rows with flashback queries: the user needs CREATE SESSION, SET CONTAINER (multitenant), FLASHBACK ANY TABLE, SELECT ANY TABLE, SELECT ANY TRANSACTION, LOGMINING, CREATE TABLE, LOCK ANY TABLE, CREATE SEQUENCE, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE, SELECT on the V_$ views LogMiner reads and EXECUTE on DBMS_LOGMNR and DBMS_LOGMNR_D.
- Why: On Amazon RDS, SYS objects are granted with rdsadmin.rdsadmin_util.grant_sys_object.

Fix:

```
GRANT CREATE SESSION, SET CONTAINER, FLASHBACK ANY TABLE, SELECT ANY TABLE, SELECT ANY TRANSACTION, LOGMINING, CREATE TABLE, LOCK ANY TABLE, CREATE SEQUENCE, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO {user} CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO {user} CONTAINER=ALL;   -- and the other V_$ views
GRANT EXECUTE ON DBMS_LOGMNR TO {user} CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO {user} CONTAINER=ALL;
```

Fix:

```
BEGIN rdsadmin.rdsadmin_util.grant_sys_object('V_$DATABASE', '{user}', 'SELECT'); END;
/
```

<a id="ora-redo"></a>
### `ora_redo`

**Redo logs**

- Why: Each log switch makes LogMiner rebuild its session; Debezium recommends at least 3 redo log groups and about 4 switches an hour.

Fix:

```
ALTER DATABASE ADD LOGFILE GROUP <n> SIZE 1G;
```

<a id="ora-ssh"></a>
### `ora_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="ora-ssh-blocked"></a>
### `ora_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="ora-ssh-reachable"></a>
### `ora_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="ora-ssh-tunnel"></a>
### `ora_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="ora-ssh-unreachable"></a>
### `ora_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="ora-supplemental"></a>
### `ora_supplemental`

**Supplemental logging**

- Why: The redo log must record enough to rebuild changed rows: at least minimal supplemental logging on the database.

Fix:

```
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

Fix:

```
BEGIN rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD'); END;
/
```

<a id="ora-tables"></a>
### `ora_tables`

**Selected tables**

- Why: The selected tables exist and log all columns.

<a id="ora-tables-exist"></a>
### `ora_tables_exist`

**Selected tables exist**

- Why: Every selected table must exist and be readable by the connector user.

<a id="ora-tables-keys"></a>
### `ora_tables_keys`

**Row keys**

- Why: Without a primary key, events carry no key, so consumers cannot tell which row an update or delete belongs to; re-copies use ROWID.

<a id="ora-tables-logging"></a>
### `ora_tables_logging`

**Table logging**

- Why: Debezium needs supplemental logging of all columns on every captured table (or on the whole database) to rebuild updates.

Fix:

```
ALTER TABLE "{schema}"."{table}" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

<a id="ora-undo"></a>
### `ora_undo`

**Undo retention**

- Why: The first copy reads every table AS OF one SCN; when the undo it needs is gone it fails with ORA-01555 (snapshot too old).

Fix:

```
ALTER SYSTEM SET UNDO_RETENTION = <seconds>;
```

<a id="ora-version"></a>
### `ora_version`

**Oracle version**

- Why: Debezium 3 supports Oracle 19c and newer.

## CockroachDB

<a id="crdb-create"></a>
### `crdb_create`

**BeziSync tables**

- Why: Snapshots on demand go through bezisync.signal_<id> (and the delivery canary through bezisync.canary_<id>): the user needs to create the bezisync schema, or rights on it.

Fix:

```
CREATE SCHEMA IF NOT EXISTS {database}.bezisync;
GRANT ALL ON SCHEMA {database}.bezisync TO {user};
```

<a id="crdb-gc-ttl"></a>
### `crdb_gc_ttl`

**Garbage collection window**

- Why: A changefeed resumes only within the tables' gc.ttlseconds: a pipeline stopped for longer must copy its tables again. Warns under an hour, or when the zone configuration can't be read.

Fix:

```
ALTER TABLE {table} CONFIGURE ZONE USING gc.ttlseconds = 14400;
```

<a id="crdb-login"></a>
### `crdb_login`

**Sign in**

- Why: Debezium signs in with the same user and password; a cluster without passwords accepts the user as it is.

<a id="crdb-network-blocked"></a>
### `crdb_network_blocked`

**Reach the cluster**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="crdb-network-reachable"></a>
### `crdb_network_reachable`

**Reach the cluster**

- Why: The host answered on the port.

<a id="crdb-network-unreachable"></a>
### `crdb_network_unreachable`

**Reach the cluster**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="crdb-rangefeed"></a>
### `crdb_rangefeed`

**Rangefeeds**

- Why: Changefeeds need the cluster setting kv.rangefeed.enabled. Reading it needs VIEWCLUSTERSETTING; without it the check can't confirm it is on.

Fix:

```
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

Fix:

```
GRANT SYSTEM VIEWCLUSTERSETTING TO {user};
```

<a id="crdb-ssh"></a>
### `crdb_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="crdb-ssh-blocked"></a>
### `crdb_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="crdb-ssh-reachable"></a>
### `crdb_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="crdb-ssh-tunnel"></a>
### `crdb_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="crdb-ssh-unreachable"></a>
### `crdb_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="crdb-tables"></a>
### `crdb_tables`

**Selected tables**

- Why: The selected tables exist and the user may stream and read them.

<a id="crdb-tables-changefeed"></a>
### `crdb_tables_changefeed`

**Changefeed permission**

- Why: Debezium opens a changefeed on the selected tables: the user needs CHANGEFEED on each (or ALL, or admin).

Fix:

```
GRANT CHANGEFEED ON TABLE {tables} TO {user};
```

<a id="crdb-tables-exist"></a>
### `crdb_tables_exist`

**Selected tables exist**

- Why: Every selected table must exist in the database.

<a id="crdb-tables-select"></a>
### `crdb_tables_select`

**Read access**

- Why: The first copy and re-copies read the existing rows.

Fix:

```
GRANT SELECT ON TABLE {tables} TO {user};
```

<a id="crdb-tls"></a>
### `crdb_tls`

**TLS**

- Why: Secure clusters need TLS (require or stricter); CockroachDB Cloud also needs its CA certificate for verify-full.

<a id="crdb-version"></a>
### `crdb_version`

**CockroachDB version**

- Why: The CHANGEFEED privilege Debezium relies on needs CockroachDB v22.2 or newer.

## Vitess

<a id="vt-binlog-retention"></a>
### `vt_binlog_retention`

**Binlog retention**

- Why: How long the tablets keep their binlogs is not visible through VTGate: a pipeline stopped for longer must copy its tables again. Keep at least a day on every tablet's MySQL.

Fix:

```
SET PERSIST binlog_expire_logs_seconds = 86400;
```

<a id="vt-grpc-blocked"></a>
### `vt_grpc_blocked`

**Reach VTGate's gRPC port**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="vt-grpc-reachable"></a>
### `vt_grpc_reachable`

**Reach VTGate's gRPC port**

- Why: The host answered on the port.

<a id="vt-grpc-unreachable"></a>
### `vt_grpc_unreachable`

**Reach VTGate's gRPC port**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="vt-keyspace"></a>
### `vt_keyspace`

**Keyspace**

- Why: A Vitess connection captures one keyspace, across all its shards.

<a id="vt-login"></a>
### `vt_login`

**Sign in**

- Why: BeziSync reads tables through VTGate's MySQL port with the connection's user (VTGate's static authentication); Debezium streams over its gRPC port.

<a id="vt-mysql-blocked"></a>
### `vt_mysql_blocked`

**Reach VTGate's MySQL port**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="vt-mysql-reachable"></a>
### `vt_mysql_reachable`

**Reach VTGate's MySQL port**

- Why: The host answered on the port.

<a id="vt-mysql-unreachable"></a>
### `vt_mysql_unreachable`

**Reach VTGate's MySQL port**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="vt-ssh"></a>
### `vt_ssh`

**SSH tunnel**

- Why: A connection through a bastion needs the SSH user and private key.

<a id="vt-ssh-blocked"></a>
### `vt_ssh_blocked`

**Reach the SSH bastion**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="vt-ssh-reachable"></a>
### `vt_ssh_reachable`

**Reach the SSH bastion**

- Why: The host answered on the port.

<a id="vt-ssh-tunnel"></a>
### `vt_ssh_tunnel`

**SSH tunnel**

- Why: Connectors reach the database through this bastion, exactly like this check.
- Why: Fails when the bastion refuses the key, its host key does not match the pinned one, or it cannot reach the database (AllowTcpForwarding must be on).
- Why: Warns while the bastion's host key is not pinned: pin it (ssh-keyscan) so nobody can impersonate the bastion.

<a id="vt-ssh-unreachable"></a>
### `vt_ssh_unreachable`

**Reach the SSH bastion**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="vt-tables"></a>
### `vt_tables`

**Selected tables**

- Why: The selected tables exist in the keyspace.

<a id="vt-tables-exist"></a>
### `vt_tables_exist`

**Selected tables exist**

- Why: Every selected table must exist in the keyspace.

<a id="vt-tables-keyspace"></a>
### `vt_tables_keyspace`

**One keyspace**

- Why: Every selected table must be in the connection's keyspace.

<a id="vt-tablets"></a>
### `vt_tablets`

**Tablets**

- Why: VStream reads every shard from a serving tablet of the chosen type (primary, replica or read-only).

<a id="vt-version"></a>
### `vt_version`

**VTGate**

- Why: The connection must point at VTGate; a plain MySQL server needs a MySQL / MariaDB connection.

## Google Cloud Spanner

<a id="sp-capture-type"></a>
### `sp_capture_type`

**Captured values**

- Why: With OLD_AND_NEW_VALUES or NEW_VALUES, update events carry only the columns that changed: destinations that replace rows lose the others.

Fix:

```
ALTER CHANGE STREAM {name} SET OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES');
```

<a id="sp-change-stream"></a>
### `sp_change_stream`

**Change stream**

- Why: The connector reads a change stream. BeziSync creates one per pipeline (bz_<id>, watching the pipeline's tables without its removed columns) when the service account may change the schema (roles/spanner.databaseAdmin); otherwise create one yourself and use it.

Fix:

```
CREATE CHANGE STREAM {name} FOR ALL OPTIONS (retention_period = '7d', value_capture_type = 'NEW_ROW_AND_OLD_VALUES');
```

Fix:

```
ALTER CHANGE STREAM {name} SET FOR {tables};
```

Fix:

```
gcloud spanner databases add-iam-policy-binding {database} --instance={instance} --project={project} \
  --member=serviceAccount:<service-account-email> --role=roles/spanner.databaseAdmin
```

<a id="sp-coordination"></a>
### `sp_coordination`

**Coordination Kafka**

- Why: Debezium's Spanner connector keeps its partition state in two Kafka topics of its own (_sync_topic_spanner_connector_<name>, _rebalancing_topic_spanner_connector_<name>), even in Debezium Server. BeziSync checks it like a Kafka destination and removes the topics with the pipeline.

<a id="sp-credentials"></a>
### `sp_credentials`

**Credentials**

- Why: Without a service-account key, BeziSync and the connector use the default credentials where they run: on GKE with Workload Identity, the runtime's service account.

<a id="sp-database"></a>
### `sp_database`

**Database**

- Why: BeziSync reads the database's schema with the connection's credentials; the connector needs the same access (roles/spanner.databaseReader).

Fix:

```
gcloud spanner databases add-iam-policy-binding {database} --instance={instance} --project={project} \
  --member=serviceAccount:<service-account-email> --role=roles/spanner.databaseReader
```

<a id="sp-dialect"></a>
### `sp_dialect`

**SQL dialect**

- Why: Debezium's Spanner connector supports GoogleSQL databases only, not the PostgreSQL dialect.

<a id="sp-emulator"></a>
### `sp_emulator`

**Emulator**

- Why: The emulator address is host:port (its gRPC port, 9010 by default).

<a id="sp-emulator-blocked"></a>
### `sp_emulator_blocked`

**Reach the Spanner emulator**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="sp-emulator-reachable"></a>
### `sp_emulator_reachable`

**Reach the Spanner emulator**

- Why: The host answered on the port.

<a id="sp-emulator-unreachable"></a>
### `sp_emulator_unreachable`

**Reach the Spanner emulator**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="sp-retention"></a>
### `sp_retention`

**Change stream retention**

- Why: A stopped pipeline can resume only within the change stream's retention_period (BeziSync's streams keep 7 days; up to 30 are possible).

<a id="sp-tables-exist"></a>
### `sp_tables_exist`

**Selected tables exist**

- Why: Every selected table must exist in the database's default schema.

## Kafka

<a id="kafka-auth"></a>
### `kafka_auth`

**Sign in**

<a id="kafka-bootstrap"></a>
### `kafka_bootstrap`

**Broker list**

<a id="kafka-create-topics"></a>
### `kafka_create_topics`

**Create topics**

- Why: Turn off 'Create topics automatically' if topics are provisioned another way.

Fix:

```
kafka-acls --bootstrap-server <broker> --add --allow-principal User: --operation Create --operation Write --operation Describe --topic '*' --resource-pattern-type literal
```

<a id="kafka-metadata"></a>
### `kafka_metadata`

**Talk to Kafka**

- Why: An open port is not enough: the connector must authenticate and fetch metadata.

Fix:

```
Check security protocol (PLAINTEXT vs SASL_SSL) and that advertised.listeners resolve from the runtime.
```

<a id="kafka-network"></a>
### `kafka_network`

**Reach brokers**

<a id="kafka-network-blocked"></a>
### `kafka_network_blocked`

**Reach {b}:{b}**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="kafka-network-reachable"></a>
### `kafka_network_reachable`

**Reach {b}:{b}**

- Why: The host answered on the port.

<a id="kafka-network-unreachable"></a>
### `kafka_network_unreachable`

**Reach {b}:{b}**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="kafka-replication"></a>
### `kafka_replication`

**Replication factor**

## HTTP

<a id="http-network-blocked"></a>
### `http_network_blocked`

**Reach the endpoint**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="http-network-reachable"></a>
### `http_network_reachable`

**Reach the endpoint**

- Why: The host answered on the port.

<a id="http-network-unreachable"></a>
### `http_network_unreachable`

**Reach the endpoint**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="http-status"></a>
### `http_status`

**Endpoint answers**

## Google Pub/Sub

<a id="pubsub-auth"></a>
### `pubsub_auth`

**Sign in**

Fix:

```
gcloud projects add-iam-policy-binding {project_id} --member=serviceAccount:<sa-email> --role=roles/pubsub.editor
```

## AWS Kinesis

<a id="kinesis-auth"></a>
### `kinesis_auth`

**Sign in**

Fix:

```
Allow kinesis:ListStreams, kinesis:DescribeStreamSummary, kinesis:CreateStream, kinesis:PutRecord, kinesis:PutRecords for the BeziSync and runtime principals.
```

<a id="kinesis-connect"></a>
### `kinesis_connect`

**Reach Kinesis**

## Redis Streams

<a id="redis-auth"></a>
### `redis_auth`

**Sign in**

<a id="redis-connect"></a>
### `redis_connect`

**Talk to Redis**

<a id="redis-memory"></a>
### `redis_memory`

**Memory headroom**

- Why: Set a stream length limit or add memory.

<a id="redis-network-blocked"></a>
### `redis_network_blocked`

**Reach Redis**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="redis-network-reachable"></a>
### `redis_network_reachable`

**Reach Redis**

- Why: The host answered on the port.

<a id="redis-network-unreachable"></a>
### `redis_network_unreachable`

**Reach Redis**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="redis-write"></a>
### `redis_write`

**Write streams**

## NATS JetStream

<a id="nats-jetstream"></a>
### `nats_jetstream`

**JetStream available**

<a id="nats-network-blocked"></a>
### `nats_network_blocked`

**Reach NATS**

- Why: The address is loopback, link-local or a cloud metadata endpoint, which BeziSync never contacts unless an admin allows it (BEZISYNC_SSRF_ALLOW_CIDRS).

<a id="nats-network-reachable"></a>
### `nats_network_reachable`

**Reach NATS**

- Why: The host answered on the port.

<a id="nats-network-unreachable"></a>
### `nats_network_unreachable`

**Reach NATS**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

## Apache Iceberg

<a id="iceberg-auth"></a>
### `iceberg_auth`

**Sign in**

- Why: The catalog refused the credentials or the request signature.

Fix:

```
Give the credentials permission to list and create namespaces, and to create, load and commit tables.
```

<a id="iceberg-catalog"></a>
### `iceberg_catalog`

**Catalog**

- Why: BeziSync signs in to the catalog and lists namespaces, as the connector will.

<a id="iceberg-catalog-blocked"></a>
### `iceberg_catalog_blocked`

**Reach the catalog**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="iceberg-catalog-unreachable"></a>
### `iceberg_catalog_unreachable`

**Reach the catalog**

- Why: Connectors run where the runtime runs; if BeziSync cannot reach the catalog, neither can they.

Fix:

```
Allow HTTPS from the runtime to the catalog (firewall, security group, private endpoint).
```

<a id="iceberg-config"></a>
### `iceberg_config`

**Settings**

- Why: A choice in the connection needs another field (the catalog URL, OAuth2 client, token, region or secret key).

<a id="iceberg-existing-tables"></a>
### `iceberg_existing_tables`

**Existing tables**

- Why: The pipeline writes into tables that already exist, keeping their partitioning; without identifier fields they get changes appended.

Fix:

```
ALTER TABLE … SET IDENTIFIER FIELDS <key columns> (Spark), or drop the table so the pipeline creates it.
```

<a id="iceberg-flink-keyless-table"></a>
### `iceberg_flink_keyless_table`

**Table without a key**

- Why: Flink CDC only. Flink CDC 3.7.0 appends to an existing Iceberg table that has no identifier fields, and writes every delete, and the old values of every update, into it as ordinary rows, so readers see deleted and replaced rows as live ones (fixed upstream in FLINK-41057, after 3.7.0). A table the pipeline creates gets the source's key, so only a table created beforehand without one is affected.

Fix:

```
Drop them and let the pipeline create them with the table's key (Positions → Start over, dropping destinations), or write them with a Debezium pipeline, which appends every change correctly.
```

<a id="iceberg-key-columns"></a>
### `iceberg_key_columns`

**Row identity**

- Why: Debezium's Iceberg build makes a column part of a table's identity only when the event's key and its row both carry it under that name. A rename changes the row, not the key, so a key column renamed or left out drops out of the identity: with some key columns left, an upsert replaces every row sharing them (rows are lost); with none, the table gets every change appended.

Fix:

```
Keep key columns under their own names: remove them from the rename steps and the left-out columns, or choose other key columns in the Transform step.
```

<a id="iceberg-namespace"></a>
### `iceberg_namespace`

**Namespace**

- Why: Tables are created in the connection's namespace; BeziSync creates it on deploy when missing.

<a id="iceberg-no-key"></a>
### `iceberg_no_key`

**Row identity**

- Why: Upserts need a key; a table without one gets every change appended.

Fix:

```
Choose key columns for them in the Transform step (Message key).
```

<a id="iceberg-partition-column"></a>
### `iceberg_partition_column`

**Partitioning**

- Why: The engine fails creating a table partitioned by a column its rows don't carry, or by a transform the column's written type can't take (Iceberg hashes no true/false or floating-point value; truncate takes text, numbers and bytes; the time transforms a date or a timestamp). On Debezium the failure stops the pipeline when the table's first change arrives.

Fix:

```
Change Partition by in the Destination step.
```

Fix:

```
Run preflight again once the source is reachable.
```

<a id="iceberg-partition-existing"></a>
### `iceberg_partition_existing`

**Partitioning**

- Why: The engine sets a table's partitioning when it creates the table; Iceberg keeps it, so other partitioning asked for later never applies. A table the catalog can't read is left unchecked (a warning), never passed.

Fix:

```
Partition by what the table has, or drop the table (Positions → Start over, dropping destinations) so the pipeline creates it again.
```

Fix:

```
Check the catalog connection's access to the namespace, then run preflight again.
```

<a id="iceberg-partition-key"></a>
### `iceberg_partition_key`

**Partitioning**

- Why: Both engines upsert by writing equality deletes on the table's key, and Iceberg applies a delete only to rows in its own partition. On a table partitioned by a column outside its key, an update that changes that column writes the new row in another partition and never deletes the old one, so readers see both. For a table that already exists, its own identifier fields and partitioning are what count: they were fixed when it was created. A table the catalog can't read is left unchecked (a warning).

Fix:

```
In the Destination step, partition these tables by their key columns (buckets or the leading part of the key), or leave them unpartitioned.
```

Fix:

```
Drop and recreate them (Positions → Start over, dropping destinations). Outside BeziSync, a data engineer can instead give the table identifier fields that cover its partition columns and remove the duplicates (SELECT <key>, count(*) FROM <table> GROUP BY <key> HAVING count(*) > 1).
```

Fix:

```
Check the catalog connection's access to the namespace, then run preflight again.
```

Fix:

```
Run preflight again once the source is reachable.
```

<a id="iceberg-partitioning"></a>
### `iceberg_partitioning`

**Partitioning**

- Why: New tables are bucketed by their key column, recorded by the wizard from discovery.

Fix:

```
Open the Destination step and continue: BeziSync records each table's key again.
```

<a id="iceberg-rustfs-upsert"></a>
### `iceberg_rustfs_upsert`

**Rows**

- Why: RustFS S3 Tables (preview) refuses a row-level commit on a table without an earlier snapshot, which every upsert pipeline's first commit is.

Fix:

```
Choose "Every change appended" in the Destination step.
```

<a id="iceberg-write"></a>
### `iceberg_write`

**Write a test table**

- Why: The connector creates tables and writes Parquet files with these credentials: BeziSync creates a one-row table, reads it back and drops it.

## StarRocks

<a id="starrocks-config"></a>
### `starrocks_config`

**Settings**

<a id="starrocks-create-table"></a>
### `starrocks_create_table`

**Create tables**

Fix:

```
GRANT CREATE DATABASE ON CATALOG default_catalog TO USER '{user}';
GRANT ALL ON DATABASE {database} TO USER '{user}';
GRANT ALL ON ALL TABLES IN DATABASE {database} TO USER '{user}';
```

<a id="starrocks-http-blocked"></a>
### `starrocks_http_blocked`

**Reach the StarRocks FE (HTTP, Stream Load)**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="starrocks-http-reachable"></a>
### `starrocks_http_reachable`

**Reach the StarRocks FE (HTTP, Stream Load)**

- Why: The host answered on the port.

<a id="starrocks-http-unreachable"></a>
### `starrocks_http_unreachable`

**Reach the StarRocks FE (HTTP, Stream Load)**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="starrocks-login"></a>
### `starrocks_login`

**Sign in**

<a id="starrocks-network-blocked"></a>
### `starrocks_network_blocked`

**Reach the StarRocks FE (MySQL protocol)**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="starrocks-network-reachable"></a>
### `starrocks_network_reachable`

**Reach the StarRocks FE (MySQL protocol)**

- Why: The host answered on the port.

<a id="starrocks-network-unreachable"></a>
### `starrocks_network_unreachable`

**Reach the StarRocks FE (MySQL protocol)**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

## Apache Doris

<a id="doris-config"></a>
### `doris_config`

**Settings**

<a id="doris-create-table"></a>
### `doris_create_table`

**Create tables**

Fix:

```
GRANT SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV ON internal.{database}.* TO '{user}'@'%';
```

<a id="doris-http-blocked"></a>
### `doris_http_blocked`

**Reach the Doris FE (HTTP, Stream Load)**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="doris-http-reachable"></a>
### `doris_http_reachable`

**Reach the Doris FE (HTTP, Stream Load)**

- Why: The host answered on the port.

<a id="doris-http-unreachable"></a>
### `doris_http_unreachable`

**Reach the Doris FE (HTTP, Stream Load)**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="doris-login"></a>
### `doris_login`

**Sign in**

<a id="doris-network-blocked"></a>
### `doris_network_blocked`

**Reach the Doris FE (MySQL protocol)**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="doris-network-reachable"></a>
### `doris_network_reachable`

**Reach the Doris FE (MySQL protocol)**

- Why: The host answered on the port.

<a id="doris-network-unreachable"></a>
### `doris_network_unreachable`

**Reach the Doris FE (MySQL protocol)**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

## Apache Fluss

<a id="fluss-config"></a>
### `fluss_config`

**Settings**

<a id="fluss-network-blocked"></a>
### `fluss_network_blocked`

**Reach the Fluss coordinator**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="fluss-network-reachable"></a>
### `fluss_network_reachable`

**Reach the Fluss coordinator**

- Why: The host answered on the port.

<a id="fluss-network-unreachable"></a>
### `fluss_network_unreachable`

**Reach the Fluss coordinator**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

## Database (JDBC)

<a id="jdbc-config"></a>
### `jdbc_config`

**Settings**

- Why: Database and schema names go into a JDBC URL and SQL: letters, digits, _, - and $ only.

<a id="jdbc-create-table"></a>
### `jdbc_create_table`

**Create tables**

- Why: Pipelines that create their tables (the default) need it; BeziSync creates and drops a test table.

Fix:

```
GRANT USAGE, CREATE ON SCHEMA {schema} TO {user};
```

<a id="jdbc-login"></a>
### `jdbc_login`

**Sign in**

- Why: The connector signs in with the same user and TLS setting.

Fix:

```
GRANT USAGE, CREATE ON SCHEMA {schema} TO {user};
```

<a id="jdbc-name-length"></a>
### `jdbc_name_length`

**Table names**

- Why: PostgreSQL allows 63 characters, MySQL and MariaDB 64, SQL Server 128.

Fix:

```
Shorten the destination naming in the Destination step.
```

<a id="jdbc-network-blocked"></a>
### `jdbc_network_blocked`

**Reach the database**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="jdbc-network-reachable"></a>
### `jdbc_network_reachable`

**Reach the database**

- Why: The host answered on the port.

<a id="jdbc-network-unreachable"></a>
### `jdbc_network_unreachable`

**Reach the database**

- Why: No TCP connection: allow the port from where connectors run (firewall, security group, peering).

<a id="jdbc-no-key"></a>
### `jdbc_no_key`

**Row keys**

- Why: Upserts and deletes find rows by the record key; without one the connector stops.

Fix:

```
Choose key columns for them in the Transform step (Message key), or insert only in the Destination step.
```

## Amazon SQS

<a id="sqs-auth"></a>
### `sqs_auth`

**Sign in**

- Why: AWS rejected the request, or the queue does not exist.

Fix:

```
Allow sqs:GetQueueAttributes, sqs:SendMessage and sqs:ReceiveMessage on the queue for the BeziSync and runtime principals.
```

<a id="sqs-connect"></a>
### `sqs_connect`

**Reach SQS**

- Why: The SQS endpoint did not answer.

<a id="sqs-fifo-dedup"></a>
### `sqs_fifo_dedup`

**FIFO deduplication**

- Why: Debezium sends no deduplication ID, so a FIFO queue must deduplicate by content.

Fix:

```
aws sqs set-queue-attributes --queue-url <url> --attributes ContentBasedDeduplication=true
```

<a id="sqs-preview"></a>
### `sqs_preview`

**Events tab**

- Why: Peeking counts as a receive; with a low dead-letter redrive count BeziSync does not peek.

<a id="sqs-queue"></a>
### `sqs_queue`

**Queue**

- Why: BeziSync reads the queue's attributes, as the connector sends to it.

## Amazon SNS

<a id="sns-auth"></a>
### `sns_auth`

**Sign in**

- Why: AWS rejected the request.

Fix:

```
Allow sns:ListTopics, sns:CreateTopic, sns:GetTopicAttributes, sns:Publish, sns:Subscribe and sns:Unsubscribe on the topics.
```

<a id="sns-connect"></a>
### `sns_connect`

**Reach SNS**

- Why: The SNS endpoint did not answer.

<a id="sns-preview"></a>
### `sns_preview`

**Events tab**

- Why: SNS keeps nothing to read back: BeziSync subscribes a small queue of its own to each topic for the Events tab and the delivery canary.

Fix:

```
Allow sqs:CreateQueue, sqs:SetQueueAttributes, sqs:GetQueueAttributes, sqs:ReceiveMessage and sqs:DeleteQueue on bz-preview-* queues.
```

## Azure Event Hubs

<a id="eventhubs-auth"></a>
### `eventhubs_auth`

**Sign in**

- Why: The connector sends with this connection string; the hub must exist.

Fix:

```
Create a shared access policy on the namespace with Send (and Listen for the Events tab), and use its connection string.
```

<a id="eventhubs-config"></a>
### `eventhubs_config`

**Settings**

- Why: The sink adds the hub to the connection string itself, so it must not name one (EntityPath).

<a id="eventhubs-listen"></a>
### `eventhubs_listen`

**Events tab**

- Why: Reading the hub back (Events tab, delivery canary) needs Listen rights; sending needs only Send.

Fix:

```
Add a connection string with Listen rights for reading.
```

<a id="eventhubs-network-blocked"></a>
### `eventhubs_network_blocked`

**Reach Event Hubs**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="eventhubs-network-reachable"></a>
### `eventhubs_network_reachable`

**Reach Event Hubs**

- Why: The namespace answered on its AMQP port.

<a id="eventhubs-network-unreachable"></a>
### `eventhubs_network_unreachable`

**Reach Event Hubs**

- Why: No TCP connection on port 5671 (AMQP over TLS): allow it from where connectors run.

## Apache Pulsar

<a id="pulsar-admin"></a>
### `pulsar_admin`

**Admin API**

- Why: BeziSync checks the namespace, creates topics and reads recent messages through the admin URL.

<a id="pulsar-auth"></a>
### `pulsar_auth`

**Sign in**

- Why: Pulsar refused the token.

Fix:

```
bin/pulsar-admin namespaces grant-permission <tenant>/<namespace> --role <role> --actions produce,consume
```

<a id="pulsar-config"></a>
### `pulsar_config`

**Settings**

- Why: The connector needs a pulsar:// or pulsar+ssl:// service URL.

<a id="pulsar-namespace"></a>
### `pulsar_namespace`

**Namespace**

- Why: Topics are created in the connection's tenant and namespace.

Fix:

```
bin/pulsar-admin namespaces create <tenant>/<namespace>
```

<a id="pulsar-network-blocked"></a>
### `pulsar_network_blocked`

**Reach the Pulsar broker**

- Why: Connection checks may not contact loopback, link-local or cloud metadata addresses.

<a id="pulsar-network-reachable"></a>
### `pulsar_network_reachable`

**Reach the Pulsar broker**

- Why: The broker answered on its binary port.

<a id="pulsar-network-unreachable"></a>
### `pulsar_network_unreachable`

**Reach the Pulsar broker**

- Why: No TCP connection on the service URL's port (6650, 6651 with TLS): allow it from where connectors run.

## Deploy

<a id="advanced-properties"></a>
### `advanced_properties`

**Advanced properties**

- Why: {n} advanced properties can't be set: {keys}. BeziSync owns identity, state, credentials and routing, and every choice the wizard makes, so a raw property may only add what BeziSync doesn't set. Unsafe options (SQL on the source, dropping the slot, skipping changes, weakening TLS, loading classes) and credentials are never passed on.

Fix:

```
Remove the listed properties under Review → Advanced Debezium properties, or set what they did where the message says.
```

<a id="advanced-unverified"></a>
### `advanced_unverified`

**Advanced properties**

- Why: Advanced properties are passed to Debezium Server as typed. BeziSync can't check what they do, so this check is Unknown, never a pass: review them in Review, and approvers see them listed.

<a id="agent-upgrade-required"></a>
### `agent_upgrade_required`

**Agent version**

- Why: The destination's own Debezium Server build serves health on another port; older agents probe 8080 and restart the connector.

Fix:

```
Upgrade the agent to this BeziSync version (Settings → Runtimes shows the install command).
```

<a id="checked-from-control-plane"></a>
### `checked_from_control_plane`

**Checked from BeziSync**

<a id="config-valid"></a>
### `config_valid`

**Configuration**

<a id="debezium-image-deprecated"></a>
### `debezium_image_deprecated`

**Debezium version**

- Why: Your Admins marked this image deprecated; move to one they offer.

<a id="debezium-image-unlisted"></a>
### `debezium_image_unlisted`

**Debezium version**

- Why: Your Admins keep a catalog of tested Debezium Server images; this one is not among them.

<a id="debezium-source-version"></a>
### `debezium_source_version`

**Debezium version**

- Why: Some sources need a recent Debezium Server: CockroachDB's sinkless changefeed arrived in Debezium 3.7. Blocks for an older official image; warns for a custom image whose release BeziSync can't read.

Fix:

```
Move the pipeline to Debezium 3.7.0 or newer (Change → Debezium version).
```

<a id="delivery-canary-unavailable"></a>
### `delivery_canary_unavailable`

**Delivery canary**

<a id="pii-mask-unsupported"></a>
### `pii_mask_unsupported`

**Personal data**

- Why: These columns can't be masked, so they would reach the destination as they are: {columns}. Debezium hashes text columns only and sends any other column (a date, a number) as it is; MongoDB values can't be hashed at all.

Fix:

```
In Tables, choose Remove (or Keep as is) for each listed column.
```

<a id="pii-protected"></a>
### `pii_protected`

**Personal data**

<a id="pii-rule-column-missing"></a>
### `pii_rule_column_missing`

**Personal data**

- Why: These columns have a masking rule but are not in their table, so the rule does nothing: {columns}. The column was renamed or dropped after the rule was made; if it was renamed, the new name is sent as it is.

Fix:

```
In Tables, check the table's columns and choose again.
```

<a id="pii-unknown"></a>
### `pii_unknown`

**Personal data**

- Why: BeziSync could not read the tables' columns, so it could not check which ones hold personal data.

<a id="pii-unprotected"></a>
### `pii_unprotected`

**Personal data**

- Why: These columns look like personal data and are sent as they are: {columns}. Their names suggest personal data (email, phone, address, birth date, card or ID numbers), and nobody chose what to do with them yet.

Fix:

```
In Tables, choose Mask or Remove for each listed column, or Keep as is when it may be sent.
```

<a id="placement"></a>
### `placement`

**Placement**

- Why: The pipeline's pods run with one of its runtime's placement profiles (node selector, tolerations, priority class, labels). It blocks when the runtime has no such profile, when no schedulable node takes these pods, or when the runtime's agent can't apply placement; it is unknown when the runtime doesn't list its nodes.

Fix:

```
Choose one of the runtime's profiles in Review.
```

Fix:

```
Ask the runtime's Admin to fix the profile, or label or untaint nodes for it.
```

<a id="runtime-capacity"></a>
### `runtime_capacity`

**Runtime capacity**

- Why: The connector may stay pending until resources free up.
- Why: Over the namespace's ResourceQuota, Kubernetes refuses the pods, so the deploy is blocked (even forced). A redeploy counts only what it adds to what the pipeline runs now.
- Why: Measured when preflight runs; when the runtime doesn't report its CPU and memory, the check is unknown, never passed.

Fix:

```
Choose a smaller size, or ask an admin to raise the namespace's ResourceQuota.
```

<a id="runtime-disabled"></a>
### `runtime_disabled`

**Runtime {name}**

- Why: An admin disabled (cordoned) this runtime. Pipelines already running there keep running.

<a id="runtime-ready"></a>
### `runtime_ready`

**Runtime {name}**

<a id="setting-experimental"></a>
### `setting_experimental`

**Experimental settings**

- Why: The engine marks these settings experimental: they may change or go in a later release.

<a id="setting-fetch-memory"></a>
### `setting_fetch_memory`

**Copy fetch size**

- Why: The first copy reads this many rows at once into the heap.

Fix:

```
Lower the fetch size in the Snapshot step, or choose a bigger size.
```

<a id="setting-queue-memory"></a>
### `setting_queue_memory`

**Queue memory**

- Why: A full queue holds that many changes in the Java heap; estimated from the tables' average row size, since no changes are seen before a deploy.

Fix:

```
Lower Queue size, set a Queue memory limit, or choose a bigger size.
```

<a id="setting-threads"></a>
### `setting_threads`

**Connections for the copy**

- Why: Each snapshot thread holds a connection to the source while the first copy runs, besides the stream's own. Unknown when BeziSync can't read the source's connection limit (no permission, or the source is checked by an agent).

Fix:

```
Use fewer snapshot threads, or raise the source's connection limit.
```

<a id="setting-version"></a>
### `setting_version`

**Settings in this Debezium version**

- Why: A tuned setting must exist in the Debezium release the pipeline runs: some arrive in a release, some go or are renamed. Unknown when BeziSync can't tell which release an image is.

Fix:

```
Remove the setting in Review → Engine settings, or move the pipeline to a Debezium version that has it.
```

<a id="sink-image-platform"></a>
### `sink_image_platform`

**Image platform**

- Why: A connector scheduled on another architecture cannot start (exec format error).

Fix:

```
Run these connectors where nodes are {platforms} (a runtime namespace pinned to those nodes, or a Docker host of that architecture).
```

<a id="sink-resources"></a>
### `sink_resources`

**Connector size**

- Why: Smaller connectors run out of memory while the destination buffers its writes.

Fix:

```
Choose the {size} size (or larger) in the Review step.
```

<a id="tables-added-not-copied"></a>
### `tables_added_not_copied`

**Added tables**

- Why: Tables added to a running pipeline are copied with an incremental snapshot; Vitess and Spanner have none, so such tables stream only their new changes.

<a id="transform-column-missing"></a>
### `transform_column_missing`

**Transforms: columns**

- Why: {table}, step {n} refers to {column}, which the table does not send (it does not exist or is excluded). A rename then does nothing and a filter rule sees an empty value.

Fix:

```
Under Transform, pick a column the table has, or remove the step.
```

<a id="transform-columns-unknown"></a>
### `transform_columns_unknown`

**Transforms: columns**

- Why: Could not read the tables' columns to check the transform steps: {error}

Fix:

```
Fix the source connection, then run the checks again.
```

<a id="transform-computed-input"></a>
### `transform_computed_input`

**Transforms: computed columns**

- Why: Flink CDC stops the job when a projection reads a column the table lacks, or calls a function on a value of another type.

Fix:

```
Pick another column for the computed column, under Transform.
```

<a id="transform-custom-class-unknown"></a>
### `transform_custom_class_unknown`

**Transforms: custom classes**

- Why: Debezium Server 3.7 does not ship {class}; the pipeline's image must add it, or the connector stops when it starts.

Fix:

```
Use a Debezium image that ships the class (Settings → Debezium versions), or remove the step.
```

<a id="transform-field-conflict"></a>
### `transform_field_conflict`

**Transforms: field names**

- Why: {table}, step {n}: two fields would get one name. Debezium Server stops with "Cannot create field because of field name duplication" (an added field or a rename onto a field the event already has, or flattening that produces an existing name).

Fix:

```
Pick a field name the events do not have yet, under Transform.
```

<a id="transform-key-column-missing"></a>
### `transform_key_column_missing`

**Transforms: message keys**

- Why: {table} has no column {column}: Debezium ignores it, so the message key falls back to the primary key (or none).

Fix:

```
Under Transform, choose key columns the table has.
```

<a id="transform-route-idle"></a>
### `transform_route_idle`

**Routes**

- Why: A route that matches no captured table does nothing (a table added later that matches is routed).

<a id="transform-route-merge"></a>
### `transform_route_merge`

**Routes**

- Why: Tables a route sends to one destination must have the same key and columns, or the destination can't hold both.

Fix:

```
Route only tables of the same shape together, or give each its own destination.
```

<a id="transform-rule-type"></a>
### `transform_rule_type`

**Transforms: filter rules**

- Why: {table}, step {n}: the rule compares {column} ({type}) in a way that never matches (for example a number with text, or a masked column whose value is a hash).

Fix:

```
Compare the column with a value of its type, or pick another column.
```

<a id="transform-truncate-type"></a>
### `transform_truncate_type`

**Truncated columns**

- Why: A Truncate step cuts text only. On Debezium another type is left as it is; on Flink CDC it would fail the job, so it blocks. Unknown when BeziSync can't read the columns' types.

Fix:

```
Remove those columns from the Truncate step.
```

## Flink CDC

<a id="flink-agent-version"></a>
### `flink_agent_version`

**Agent version**

Fix:

```
Upgrade the agent to BeziSync 0.3.0 or later (Settings → Runtimes shows the install command).
```

<a id="flink-cluster-fit"></a>
### `flink_cluster_fit`

**Cluster fits the nodes**

- Why: A custom Flink CDC cluster is packed into TaskManagers the runtime's largest node can hold. When even one slot needs more CPU or memory than that node offers, or a manual TaskManager is bigger than it, its pods would never be scheduled. When the runtime doesn't report its nodes' size, BeziSync can't check and says so.

Fix:

```
Give each slot less CPU or memory in Review → Size, or add bigger nodes to the runtime.
```

Fix:

```
Use the automatic shape, or smaller TaskManagers, in Review → Size.
```

<a id="flink-kubernetes-services"></a>
### `flink_kubernetes_services`

**Kubernetes Services**

Fix:

```
# The runtime's Role (both BeziSync charts from 0.2.0 grant it)
- apiGroups: [""]
  resources: [configmaps, secrets, services]
  verbs: [get, list, create, update, patch, delete]
```

<a id="flink-oracle-analyze"></a>
### `flink_oracle_analyze`

**Table statistics**

- Why: Flink CDC's Oracle reader runs ANALYZE TABLE … COMPUTE STATISTICS on each table before it splits the first copy into chunks. That needs ANALYZE ANY (or owning the tables) and unlocked statistics, and it reads each table in full and replaces its optimizer statistics, which can change query plans. A pipeline that reads from now only warns; a deploy that copies warns of the effect.

Fix:

```
Grant ANALYZE ANY to the connector user inside the PDB (the check shows the statement).
```

Fix:

```
Unlock the tables' statistics with DBMS_STATS.UNLOCK_TABLE_STATS.
```

<a id="flink-oracle-flush-table"></a>
### `flink_oracle_flush_table`

**LogMiner flush table**

- Why: On a multitenant database Flink CDC's Oracle reader (Debezium 1.9) records its position in the connector user's LOG_MINING_FLUSH table in the CDB root. BeziSync creates it at deploy when the user may create tables there; otherwise every flush fails with ORA-00942. The table is shared with every pipeline mining as that user.

Fix:

```
In the CDB root, create the table with one row (the check shows the statements), or give the user CREATE TABLE and quota there.
```

<a id="flink-oracle-unknown"></a>
### `flink_oracle_unknown`

**Oracle for Flink CDC**

- Why: BeziSync could not read what Flink CDC's Oracle reader needs from the database (its version, ANALYZE rights, the flush table). The source's own checks say whether the database can be reached.

Fix:

```
Fix what the source checks report, then run the checks again.
```

<a id="flink-oracle-version"></a>
### `flink_oracle_version`

**Oracle version**

- Why: Flink CDC 3.7 reads Oracle with Debezium 1.9, which knows Enterprise, Standard and Express Edition up to 23ai, not Oracle Database Free or Oracle AI Database 26ai: their pipelines fail at start. It also reads V$VERSION, which needs SELECT_CATALOG_ROLE. A forced deploy can't skip this check.

Fix:

```
Run this pipeline on Debezium, which reads this release (Iceberg and the event destinations), or read from an Enterprise, Standard or Express Edition database up to 23ai.
```

Fix:

```
Grant SELECT_CATALOG_ROLE to the connector user.
```

<a id="flink-primary-key"></a>
### `flink_primary_key`

**Primary keys**

Fix:

```
Pick key columns for these tables in the Transform step (Primary key), or add a primary key in the database.
```

<a id="flink-readers-changed"></a>
### `flink_readers_changed`

**Readers**

- Why: A Flink CDC pipeline's saved position holds what each of its readers had read, so it restores only with the same number of readers. After Start over nothing is left to restore and the number can change.

Fix:

```
Keep the readers it runs, or pause the pipeline and use Positions → Start over, then deploy.
```

<a id="flink-replica-identity"></a>
### `flink_replica_identity`

**Whole-row changes**

- Why: PostgreSQL logs the old row of an update only with REPLICA IDENTITY FULL. It makes the WAL larger for these tables.

Fix:

```
ALTER TABLE "{schema}"."{table}" REPLICA IDENTITY FULL;
```

<a id="flink-replication-slots"></a>
### `flink_replication_slots`

**Replication slots for the copy**

- Why: While a Flink CDC pipeline copies a PostgreSQL source, each of its readers opens its own temporary replication slot (and WAL sender) next to the pipeline's own one, so a copy needs 1 + parallelism of each. Without enough, the copy stops part-way.

Fix:

```
Drop replication slots nobody uses, or raise max_replication_slots and max_wal_senders (a restart).
```

<a id="flink-server-ids"></a>
### `flink_server_ids`

**MySQL server ids**

Fix:

```
Create this pipeline again: a new pipeline gets other server ids.
```

<a id="flink-tables-added"></a>
### `flink_tables_added`

**Added tables**

- Why: Flink CDC's Oracle reader never copies a table added to a pipeline that restores a saved position: only rows written afterwards arrive. With nothing left to restore (after Start over) the pipeline starts as Start over chose. BeziSync also refuses to restore a position saved before the tables were added.

Fix:

```
Pause the pipeline and use Positions → Start over (it copies every table again), then deploy. Or capture the new tables in a second pipeline.
```

<a id="flink-tables-unknown"></a>
### `flink_tables_unknown`

**Tables for Flink CDC**

## Secret stores

<a id="secret-missing"></a>
### `secret_missing`

**Saved secrets**

- Why: The connection was copied from another project without its secrets, and nobody has entered them here yet.

<a id="secret-ref-unresolved"></a>
### `secret_ref_unresolved`

**Secrets from the secret store**

- Why: The connection takes a secret from an external store, and reading it failed.

## Project policies

<a id="policy-advanced-properties"></a>
### `policy_advanced_properties`

**Advanced properties**

- Why: This pipeline sets {n} advanced Debezium properties ({keys}); this project does not allow them. BeziSync can't verify raw properties, so an Admin decides whether a project may use them.

Fix:

```
Remove the advanced properties in Review, or ask an Admin to allow them under Settings → Policies.
```

<a id="policy-custom-transform"></a>
### `policy_custom_transform`

**Custom transforms**

- Why: {steps} run(s) a custom transform class ({names}); this project does not allow custom transforms. Custom classes cannot be previewed or checked by BeziSync, so an Admin decides who may use them.

Fix:

```
Ask an Admin to change the policy under Settings → Policies.
```

<a id="policy-engine"></a>
### `policy_engine`

**Engine**

- Why: This project does not allow pipelines on {engine}.

<a id="policy-ok"></a>
### `policy_ok`

**Project policies**

<a id="policy-resource-limits"></a>
### `policy_resource_limits`

**Resource limits**

- Why: The project caps the CPU, memory and readers one pipeline may ask for at its peak (a Flink CDC cluster during its first copy). A running pipeline over a new limit keeps running; its next deploy is blocked.

Fix:

```
Choose a smaller size, fewer readers, or ask an Admin to raise the limits under Settings → Policies.
```

<a id="policy-resources"></a>
### `policy_resources`

**Resources**

- Why: The '{size}' preset exceeds this project's limit ({max_resources}).

Fix:

```
Choose {max_resources} or smaller.
```

<a id="policy-runtime"></a>
### `policy_runtime`

**Runtime**

- Why: This project does not allow pipelines on that runtime.

<a id="policy-scripted-transform"></a>
### `policy_scripted_transform`

**Scripted transforms**

- Why: {steps} run(s) a script you wrote ({names}); this project does not allow scripted transforms. Scripts run inside the connector, next to its database credentials, so an Admin decides who may use them.

Fix:

```
Ask an Admin to change the policy under Settings → Policies.
```

<a id="policy-setting-limit"></a>
### `policy_setting_limit`

**Setting limits**

- Why: The project bounds some settings (a range, fewer choices, a pinned value, or hidden). A pipeline saved before a limit keeps running; its next deploy is blocked until its settings fit.

Fix:

```
Change or remove them in Review → Engine settings.
```

<a id="policy-sink-type"></a>
### `policy_sink_type`

**Destination type**

- Why: This project does not allow {sink_type} destinations.

<a id="policy-snapshot-mode"></a>
### `policy_snapshot_mode`

**Snapshot mode**

- Why: Snapshot mode '{mode}' is not allowed here. Allowed: {allowed_snapshot_modes}.

Fix:

```
Pick an allowed snapshot mode in the pipeline settings.
```

<a id="policy-source-budget"></a>
### `policy_source_budget`

**Pipelines per source**

- Why: The source already feeds {other_pipelines_on_source} pipeline(s); this project allows {limit}. Each one holds a replication slot or log reader on the database.

Fix:

```
Reuse an existing pipeline (add tables to it) or ask an admin to raise the limit.
```

<a id="policy-source-type"></a>
### `policy_source_type`

**Source type**

- Why: This project does not allow {source_type} sources.

<a id="policy-transform-type"></a>
### `policy_transform_type`

**Transforms**

- Why: This project does not allow {steps}. Admins choose which transform types pipelines may use.

Fix:

```
Ask an Admin to change the policy under Settings → Policies.
```

<a id="policy-unreadable"></a>
### `policy_unreadable`

**Project policies**

- Why: This project's policies can't be read, so changes are blocked until an Admin fixes them.

Fix:

```
Ask an Admin to open Settings → Policies, fix the fields it lists and save.
```
