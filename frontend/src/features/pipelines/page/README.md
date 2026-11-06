# page

## Overview
The tabs of one pipeline's page (`/p/$projectSlug/pipelines/$pipelineId?tab=`), rendered by `../PipelinePage.tsx`: Overview, Data, Events, Logs, Snapshots (Positions on Flink CDC) and History. Card wording is decided by the pure functions in `overview.ts` so it can be tested without rendering; server state comes from the hooks in `lib/pipelines.ts`.

## Interface
- `[Overview.tsx:Overview]` — flow strip, stale-status and reason notices, Flink cluster card, metrics chart (`TimeSeries`), Since start, Source protection, Transforms and delivery cards.
- `[overview.ts:protectionView]` / `[overview.ts:transformView]` / `[overview.ts:flinkView]` — what Source protection, Transforms (output vs steps; Flink CDC describes tables kept in sync) and the Flink cluster card say; helpers `formatWindow`, `formatLatency`, `latest`.
- `[Flow.tsx:Flow]` / `[Flow.tsx:flinkLine]` — source → engine → destination strip with link states and the engine line.
- `[DataTab.tsx:DataTab]` — captured tables with their destinations and column rules (masked / removed / kept), and the tracked schema with "Mark as reviewed" for breaking changes (`SchemaSection`, offered with `pipeline.operate` while `pending_breaking > 0`).
- `[StreamTabs.tsx:EventsTab]` / `[StreamTabs.tsx:LogsTab]` — sample events per table, and connector logs with level/search filtering (`[StreamTabs.tsx:filterLogs]`); on Flink CDC a component picker (`[StreamTabs.tsx:logComponents]`).
- `[SnapshotsTab.tsx:SnapshotsTab]` / `[SnapshotsTab.tsx:copyEstimate]` — Debezium: re-copy chosen tables (`Recopy`) and reset the stored position (`Position`); Flink CDC: `FlinkPositions` plus an explanation. `LARGE_TABLES`/`LARGE_ROWS` mark a large copy.
- `[FlinkPositions.tsx:FlinkPositions]` / `[FlinkPositions.tsx:startOverEffects]` / `[FlinkPositions.tsx:positionLabel]` — saved positions newest first, Restore, and Start over (copy again / from now / since a time).
- `[HistoryTab.tsx:HistoryTab]` / `[HistoryTab.tsx:DiffView]` — versions with rendered config, deploy/roll back (`DeployConfirm`), activity, diff, Export YAML, Promote to another project, Save as template.

## Invariants
- Source protection never turns a missing value into a reassuring one: an error or unmeasured probe is an `Unknown` grey pill (`[overview.ts:protectionView]`).
- Overview warns when status is stale (`isStale`) and hides the flow strip once the pipeline is deleting/deleted/cleanup_failed (`[Overview.tsx:Overview]`).
- Events need `pipeline.preview_data` and a deployed version; logs need a deployed version (`[StreamTabs.tsx:EventsTab]`). `filterLogs` keeps level-less lines with the line before them.
- Re-copy needs `pipeline.operate`, a running and deployed pipeline and at least one table; a copy over `LARGE_TABLES` tables or `LARGE_ROWS` rows asks to confirm first (`[SnapshotsTab.tsx:Recopy]`).
- Position reset and Flink restore/start-over need `offsets.reset`, a deployed version, the pipeline paused (`desired_state === 'paused'`) and the typed pipeline name (`typeToConfirm`) (`[SnapshotsTab.tsx:Position]`, `[FlinkPositions.tsx:FlinkPositions]`); "since a time" must be in the past and is gated by `caps.start_from_time`; the newest current saved position offers no Restore, and Debezium's "Re-snapshot from scratch" is offered only when the source can copy rows.
- Deploying or rolling back to another version needs `pipeline.deploy`, an already deployed pipeline that is not deleting/deleted, and a version that is not the deployed one; the confirmation lists the diff notes and, when the project requires approval, that the deploy waits for someone else (`[HistoryTab.tsx:DeployConfirm]`). Save as template needs `project.manage`.
- Promote targets only projects where the caller has `pipeline.edit`, runs a dry run before the real request, offers copying missing connections only with `connection.manage` here and there, and offers deploying there only with `pipeline.deploy` in the target (`[HistoryTab.tsx:PromoteDialog]`).

## Side Effects
- HTTP reads via `[../../../lib/pipelines.ts:useVersions]` and siblings (many poll with `refetchInterval`): `…/pipelines/{pipeline_id}/metrics`, `/events`, `/logs`, `/schema`, `/offsets`, `/positions`, `/versions`, `/config`, `/diff`, project `/events` (activity), `…/policies`, `…/templates`, `…/debezium-images` (Flow), connector types and discovery, and the target project's connections and runtimes in Promote.
- HTTP writes: `POST …/snapshots`, `POST …/offsets/reset`, `POST …/positions/{position_id}/restore`, `POST …/schema/review`, `POST …/deploy`, `POST …/promote`, `POST …/templates`; `GET …/manifest` for Export.

## Verification
- Run: `cd frontend && bun run test src/features/pipelines` (covers `page/__tests__` and `../__tests__/overview.test.ts`, `page.test.ts`)
