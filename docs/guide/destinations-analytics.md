# Analytics and search

These are Flink CDC destinations for real-time analytics (StarRocks, Apache Doris) and search
(Elasticsearch). See [Destinations](destinations.md) for what every destination shares.

Each section below has the same rows, so you can compare destinations. "Reading it back" means
the pipeline's **Events** tab; Flink CDC pipelines run no
[delivery canary](operate.md#delivery-canary). "Drop destinations" is the choice made when you
[delete a pipeline](operate.md#delete-a-pipeline).

## StarRocks

Changes land in one primary-key table per source table in a StarRocks database, written by Flink
CDC with Stream Load.

| | StarRocks |
|---|---|
| Engine | Flink CDC |
| What you get | One primary-key table per source table in a database (`cdc` by default, created on deploy if missing). Schema changes are applied to the tables |
| Connection | The FE host, its query port (9030, MySQL protocol) and HTTP port (8030, Stream Load), a user and password, and the replicas of each table (1 on a single-node cluster) |
| Preflight checks | Signs in over the MySQL protocol and reads the user's `SHOW GRANTS`; it creates nothing. Every table needs a key (`flink_primary_key`) |
| Reading it back | The Events tab reads rows back |
| Deleting a pipeline | "Drop destinations" drops its tables |
| Limits | No column defaults, timestamps to the second, no TLS (see below) |

Flink CDC's StarRocks connector carries its own MySQL JDBC driver (Connector/J 5.1.49), so
StarRocks needs no driver added to the image.

The grants check passes when `SHOW GRANTS` shows `CREATE TABLE` on the database. Otherwise it says
"not known" (the right may come from a role) and shows the `GRANT` statements the user needs.

### Limits

- **Column defaults** are not copied. Flink CDC passes them on as the source writes them
  (`nextval(...)`, `now()`, `'NEW'::character varying`), which StarRocks refuses. So BeziSync
  writes every source column that has a default without it; the values the source filled in still
  arrive. Such a column arrives as `STRING` if it is text, and a timestamp one with millisecond
  precision. Because of that, new source columns are held back until the next deploy ("Send" is
  not offered).
- **Timestamps** arrive to the second: Flink CDC's StarRocks connector writes no fractions.
- **No TLS**: Stream Load and the MySQL protocol connection are plain text upstream. Keep
  StarRocks and the runtime in a network you trust.

## Apache Doris

Doris works the same as StarRocks, with one unique-key table per source table.

| | Apache Doris |
|---|---|
| Engine | Flink CDC |
| What you get | One unique-key table per source table, written with Stream Load |
| Connection | As for StarRocks, plus **BE nodes** (`host:HTTP port`) when needed (see below) |
| Preflight checks | As for StarRocks |
| Reading it back | Rows are read back over the MySQL protocol |
| Deleting a pipeline | Tables are dropped over the MySQL protocol |
| Limits | No TLS. Column defaults are left out the same way as for StarRocks, so new columns are held back until the next deploy |

Doris needs no MySQL Connector/J. When the BE addresses the FE hands out can't be reached from the
runtime (Doris in Docker or behind NAT), list the BEs under **BE nodes** and Flink CDC loads into
them directly.

## Elasticsearch

One index per source table, one document per row whose id is the primary key.

| | Elasticsearch |
|---|---|
| Engine | Flink CDC |
| What you get | One index per source table (the naming pattern, dots as `_`, lower-cased). An update replaces the document and a delete removes it |
| Connection | The hosts (`http(s)://host:port`), the version (6, 7 or 8, matching the server) and an optional user and password |
| Preflight checks | Signs in and compares the version, then asks about the user's rights (see below). It creates no index |
| Reading it back | The Events tab reads documents |
| Deleting a pipeline | "Drop destinations" deletes its indexes |
| Limits | The sink takes no CA: use `https` only with a certificate from a public CA, otherwise keep the cluster in a network you trust |

Indexes are created by their first document with dynamic mappings: decimals arrive as text, and
NULL fields are left out. For other mappings, create an index template beforehand.

The pipeline check asks Elasticsearch (`_has_privileges`, 6.4 and later) whether the user may:

- write and delete documents in the pipeline's indexes (`index`, `delete`);
- create the missing ones (`create_index` or `auto_configure`).

It warns without `delete_index`, and says "not known" when security is off or the question is
refused.

---

[All documentation](../README.md)
