type: minor

### Changed
- **The Destination step follows the source** - it lists only the destinations an engine the project allows can write from the pipeline's source, names the rest with the reason, and disables those whose engine isn't set up; Add a destination, the engine picker and the Source step's sources and templates follow the same rule (ADR 0011).
- **Flink CDC to Kafka speaks events** - the Transform step, its preview, Review and the pipeline Overview describe topics, events and the message key instead of tables (ADR 0011).
- **Allowed engines choose the default** - when a destination takes both engines and the project allows one, that one is chosen, through the UI, API, CLI and MCP.

### Fixed
- **Kafka on Flink CDC keeps its one format** - choosing Flink CDC sets JSON without the schema, and saving another format is refused at save instead of at Review.
