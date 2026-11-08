type: patch

### Fixed
- **Flink CDC key editor shows an expression column's type** - it said "decided when it runs" while the reason beside it named the type; it now shows the kind the server's check reads.
- **A nullable key column next to the primary key no longer warns** (`transform_key_column_nullable`) - with the whole primary key in the key, rows can't share a key.

### Changed
- **A computed date as a key says how to partition by date** - put the timestamp column itself in the key and partition it by its day under Partition by.
