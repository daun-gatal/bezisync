type: minor

### Changed
- **CORS `*` no longer sends credentials** - with `*` among the CORS origins, any site's `Origin` used to be answered with credentials, so another site could read the API with a signed-in user's cookie. Origins listed by name keep credentialed CORS; any other origin `*` allows now gets a literal `*` without credentials, and the API warns at startup. A cross-origin UI that signs in with cookies must be listed by name; bearer tokens are unaffected.
- **Project policies that can't be read fail closed** - stored policies that fail to parse used to fall back to the defaults, which allow everything. Now the fields that parsed are kept, the others take their strictest value, and every change in the project (saving a changed spec, creating, deploying, even with Deploy anyway) is refused with `policy_unreadable` until an Admin fixes them: Settings → Policies lists what couldn't be read. Running pipelines keep running; pause, resume and delete still work.
- **Testing a runtime needs `runtime.manage`** - it calls the runtime and rewrites its test result and capacity; members who aren't Admins no longer see Test. MCP `test_runtime` is now a write tool, and `delete_pipeline` asks for `pipeline.deploy`, the permission deleting already needed.
- **Labels are checked when an edit changes them** - at most 20, slug keys, values of at most 63 characters, as on create (and manifest apply). Labels a pipeline already holds still save unchanged.
- **Flink CDC pipelines refuse snapshot requests** - Flink CDC can't copy one table again while the others stream (use Start over); a deploy that adds tables lets Flink CDC copy them itself instead of queuing a Debezium snapshot.

### Fixed
- **Jobs a crashed controller left running are taken back** - a restart, cleanup or notification interrupted by a controller crash runs again on the next leader; an interrupted snapshot is marked failed ("Interrupted…") rather than sent twice. Project deletion and runtime removal no longer stay blocked behind such a job.
- **The "Position moved backwards" alert can fire** - the stored position was dropped on the tick after each probe, so a position going back was never noticed.
- **Delivery canaries keep running after one is lost** - a canary that never arrived stopped canary checking for the pipeline; the alert now names when the lost one was written, and the next canary goes out on schedule.
- **Keep deleted rows before a field step on Iceberg** - `[Keep deleted rows, Rename]` without an explicit Row after change ran the rename on the change envelope; the row shape now comes first. Only pipelines with that chain restart on upgrade.
- **Review deploys the version it checked** - Deploy names the version the final checks ran on, so it never deploys an edit someone saved after.
- **A setting a project policy pins can't be switched** - an On/Off setting pinned by policy stayed clickable in the settings editor.
- **Sign-in opens when saving your profile finds the session expired** - changing Guided/Expert mode or notification settings after the session expired failed silently.
- **Slow Flink CDC helper copies on Docker** - copying the helper was cut off after 30 seconds instead of its two-minute limit.
- **A data race in the remote-runtime agent** - its report read the desired workloads while they were being replaced.
