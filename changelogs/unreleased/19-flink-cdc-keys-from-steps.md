type: minor

### Added
- **Column types in the Destination table** - the Transform step's Destination table (Flink CDC) shows each column's type: the source's type for a source column, and the kind a computed or added column writes (text, a whole number, …), and when a column is cut short.
- **Primary keys from any written column (Flink CDC)** - the Primary key picker offers every column the table writes, with its type: source columns under their new names, added fields and computed columns (ADR 0010). A computed key column must be text or a whole number, never empty and the same each time a row is written; BeziSync writes it NOT NULL, as destinations such as Iceberg require. On Debezium, which builds the message key before any step runs, added fields are shown but can't be picked.

### Fixed
- **Flink CDC key checks** - a key column the table doesn't write (left out, or a step's column since removed or renamed) is now refused before deploy (`transform_key_column_unknown`), instead of failing the job; the left-out check no longer compares the renamed name, and Debezium's "falls back to the primary key" finding no longer appears on Flink CDC pipelines.
- **A Flink CDC key column that can be empty is now a warning** (`transform_key_column_nullable`): Flink CDC writes it as an empty text, so the rows where it is empty share one key and replace each other.
