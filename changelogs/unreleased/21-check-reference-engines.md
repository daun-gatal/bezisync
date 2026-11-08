type: patch

### Changed
- **The preflight check reference names the engines** - each source and destination section says whether Debezium, Flink CDC or both read or write it, and the PostgreSQL, MySQL / MariaDB, SQL Server and Oracle sections link the Flink CDC checks that also run for them (replica identity, replication slots for the copy, server ids, the Oracle checks), which were listed only under Flink CDC.
