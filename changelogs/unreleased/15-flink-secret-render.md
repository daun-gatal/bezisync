type: patch

### Fixed
- **Flink CDC pipelines keep `$` in values as written** - the Flink CDC helper filled `$name` and `${name}` anywhere in the pipeline definition, so a literal such as `'US$5'` in a transform reached Flink CDC changed, and a value containing `$BZ_…` could have had a secret written into it. Now only the placeholders BeziSync writes as whole values are filled. Pipelines restart only when their helper restarts; nothing else changes.
- **Connection settings can't name another secret** - a setting (not a secret) containing `${`, such as a user name of `${BZ_STATE_PASSWORD}`, would have been filled in by Debezium Server or the Flink CDC helper and sent to that connection's server. Saving one is refused, and a pipeline using a connection saved with one before stops compiling (422 `connection_config`) until the setting is changed; its running workload is left as it is.
- **Flink CDC refuses an existing Iceberg table without a key** - Flink CDC 3.7.0 writes every delete, and the old values of every update, into such a table as ordinary rows. Preflight now blocks it (`iceberg_flink_keyless_table`); a table the pipeline creates is unaffected.
