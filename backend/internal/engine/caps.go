package engine

// Caps is what an engine can do for one pipeline. Every action the UI, CLI or MCP offers on a
// pipeline reads these; an action an engine cannot do is shown as unavailable with Why[action].
// They are engine-level: a source's own limits (descriptor "capture", such as a source that can't
// copy a table again) still apply on top.
type Caps struct {
	Engine ID `json:"engine"`
	// EditPosition: the stored position can be read and edited (Debezium's offsets).
	EditPosition bool `json:"edit_position"`
	// RecopyTable: one table can be copied again while the others keep streaming.
	RecopyTable bool `json:"recopy_table"`
	// AddTables: tables can be added to a deployed pipeline without copying the others again
	// (Flink CDC on Oracle: only after Start over).
	AddTables bool `json:"add_tables"`
	// StartFromTime: Start over can begin at a point in time.
	StartFromTime bool `json:"start_from_time"`
	// SavedPositions: the engine keeps savepoints that can be restored (Flink CDC).
	SavedPositions bool `json:"saved_positions"`
	// EventFormats: the event format, schema registry and message keys can be chosen.
	EventFormats bool `json:"event_formats"`
	// SchemaChanges: how source schema changes reach the destination can be chosen.
	SchemaChanges bool `json:"schema_changes"`
	// ParallelCopy: the first copy reads tables in parallel chunks.
	ParallelCopy bool `json:"parallel_copy"`
	// HashMask: personal data can be masked with a hash.
	HashMask bool `json:"hash_mask"`
	// ResizeParallelism: a deployed pipeline can change how many readers it runs.
	ResizeParallelism bool `json:"resize_parallelism"`
	// Why explains each unavailable action ("edit_position" → the reason and what to use instead).
	Why map[string]string `json:"why"`
}

// CapsFor returns the engine's capabilities for a pipeline reading sourceType.
func CapsFor(id ID, sourceType string) Caps {
	switch id {
	case FlinkCDC:
		why := map[string]string{
			"edit_position":      "Flink CDC keeps the position inside its saved state. Use Start over, or restore a saved position from History.",
			"recopy_table":       "Flink CDC can't copy one table again while the others stream. Use Start over to copy every table again.",
			"event_formats":      "Flink CDC writes tables, not events: there is no event format, schema registry or message key.",
			"hash_mask":          "Flink CDC has no hash function for masks. Remove the column or use a partial or fixed mask.",
			"resize_parallelism": "A Flink CDC pipeline keeps the number of readers it was deployed with. Change memory, or Start over to change readers.",
		}
		c := Caps{Engine: id, AddTables: true, SavedPositions: true, SchemaChanges: true, ParallelCopy: true, Why: why}
		switch sourceType {
		case "mysql", "sqlserver":
			c.StartFromTime = true
		case "oracle":
			// Flink CDC's Oracle reader has no timestamp start and never copies a table added to a
			// pipeline that restores a saved position (ADR 0006).
			why["start_from_time"] = "Flink CDC's Oracle reader starts only from a full copy or from now."
			c.AddTables = false
			why["add_tables"] = "Flink CDC's Oracle reader doesn't copy a table added to a running pipeline. Pause it, use Positions → Start over, then deploy."
		default:
			why["start_from_time"] = "A PostgreSQL replication slot can't go back in time."
		}
		return c
	default:
		return Caps{Engine: Debezium, EditPosition: true, RecopyTable: true, AddTables: true, EventFormats: true, HashMask: true,
			ResizeParallelism: true, Why: map[string]string{
				"saved_positions": "Debezium keeps its position as offsets; view or edit them instead.",
				"schema_changes":  "Debezium sends schema changes as events; your consumers apply them.",
				"parallel_copy":   "Debezium copies each table with one reader.",
				"start_from_time": "Debezium starts over from a full copy or from now.",
			}}
	}
}
