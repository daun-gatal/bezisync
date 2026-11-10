type: minor

### Added
- **Discard a deployed pipeline's undeployed changes (ADR 0017)** - a change saved from the wizard but not deployed could only be deployed or rolled back over (which restarts the connector). Closing the wizard now asks "Keep vN?" (keep it, or discard it), and the "vN is saved but not deployed" notice has **Discard vN**. Discarding points the pipeline back at its deployed version: the connector keeps running and doesn't restart, no approval is needed, and the discarded version stays in History, deployable. Also `POST …/pipelines/{id}/changes:discard` (with `If-Match`), `bezisync remote pipelines discard NAME` and the MCP tool `discard_pipeline_changes`. It is refused while a change request is pending.

### Changed
- **Editing back to what is deployed clears the change** - saving a spec equal to the deployed one (with the project's pinned settings) now points the pipeline back at the deployed version instead of keeping a draft that reads "saved but not deployed"; `pipelines:apply` of a manifest that matches what runs does the same.
