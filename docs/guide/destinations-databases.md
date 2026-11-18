# Databases

These destinations keep a live copy of the source tables in another database. The tables are
created from the source and rows are usually upserted (inserted or updated) by key. See
[Destinations](destinations.md) for what every destination shares.

Each section below has the same rows, so you can compare destinations. "Reading it back" means
the pipeline's **Events** tab and the [delivery canary](operate.md#delivery-canary). "Drop
destinations" is the choice made when you [delete a pipeline](operate.md#delete-a-pipeline).

## Database (JDBC)

Changes land in PostgreSQL, MySQL, MariaDB or SQL Server tables, one per source table, written by
Debezium's JDBC sink connector. The official images ship the drivers.

| | Database (JDBC) |
|---|---|
| Engine | Debezium |
| What you get | One table per source table (naming `{schema}_{table}`; dots become `_`), with the source's key as primary key. Tables get new columns as the source changes. "Use existing tables only" turns creating and changing tables off |
| Where tables go | PostgreSQL: the connection's schema. SQL Server: the login's default schema (`dbo` unless set otherwise). MySQL and MariaDB: the database |
| Pipeline settings | Destination step. Rows: *upsert by key* (default: the table mirrors the source) or *insert only* (every change is a new row; needs no key). Deletes: delete the row, or are ignored |
| Preflight checks | Reaches the database, signs in, and asks whether the user may create tables in the schema, without creating one. Checks keys and name lengths |
| Reading it back | The Events tab and the delivery canary read rows of the tables |
| Deleting a pipeline | "Drop destinations" drops its tables |

What preflight asks each database about creating tables, and the longest name it allows:

| Database | Create-table right checked | Longest name |
|---|---|---|
| PostgreSQL | `USAGE` and `CREATE` | 63 characters |
| SQL Server | `CREATE TABLE` and `ALTER` on the schema | 128 characters |
| MySQL, MariaDB | The `SHOW GRANTS` text; "not known" when a role may grant it | 64 characters |

### Keys and deletes

- Upserts need a key on every table. Preflight blocks tables without one until you pick key
  columns (Transform step) or choose insert only.
- With the "row after change" shape, BeziSync turns deletes into what the Deletes setting needs.

### Column defaults across databases

Across databases, Debezium copies column defaults into the tables it creates. A PostgreSQL
`timestamptz DEFAULT now()` column fails to create in MySQL or MariaDB ("Invalid default
value"). Exclude the column (Tables step), or create the table yourself and choose "Use existing
tables only".

## OceanBase

One table per source table in a database of a MySQL tenant.

| | OceanBase |
|---|---|
| Engine | Flink CDC |
| What you get | One table per source table, created by Flink CDC over JDBC with OceanBase Connector/J (in the reference image; no MySQL driver needed) |
| Connection | Host and port (2881, or 2883 through OBProxy); the database (created on deploy if missing); the user as `user@tenant` (`#cluster` through OBProxy) and its password |
| Preflight checks | Signs in and reads the user's `SHOW GRANTS`. It creates nothing |
| Reading it back | The Events tab reads rows |
| Deleting a pipeline | "Drop destinations" drops the tables |
| Limits | Not encrypted (no TLS options). Times are written in UTC. Only new and renamed columns are applied as schema changes; others stop the pipeline |

The connection has no TLS options: keep OceanBase and the runtime on a private network.

---

[All documentation](../README.md)
