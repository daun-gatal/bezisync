# Frontend (`frontend/`)

## Layout
```
src/
├── app/            # router (TanStack Router, code-based), AppShell, error boundaries
├── features/       # product areas: auth, onboarding, pipelines, connections, alerts, settings
├── components/ui/  # the design system: text, button, feedback, form, select, layout, table, tabs, overlays
└── lib/            # api client (openapi-fetch + generated api-schema.ts), session/project context, formatters
```

## Rules
- **`bun`** for deps/scripts; tests run on Node (vitest + jsdom). React 19, TypeScript 5.9 strict.
- **Server state only in TanStack Query.** All calls go through `lib/api.ts` (`client` + `unwrap`),
  which adds the CSRF header and turns problems into `ApiError`.
- After any backend API change: `make openapi` (regenerates `openapi.json` + `api-schema.ts`); CI checks drift.
- **Tailwind v4 with design tokens** from `src/styles.css` (`bg-surface`, `text-muted`, `text-ok`, …).
  Each token is written once as `light-dark(light, dark)`; `data-theme` on `<html>` forces one.
  People choose System, Light or Dark in the user menu or on Account (`lib/theme.ts`, `localStorage`
  `bz.theme`); `public/theme.js` (a file: the CSP forbids inline scripts) applies it before the first paint.
  Never hard-code colours; light and dark must both work. Text on an accent fill uses
  `text-on-accent` and text on a danger fill `text-on-bad` (both dark in dark mode); the axe
  gate in e2e enforces WCAG AA contrast. Never put `aria-label` on a plain `<span>`/`<div>`:
  use visible text or `sr-only` text. On a button, put extra words for screen readers in
  `aria-label`, not an `sr-only` span (a span duplicates the row's text for tests and readers).
- **One size scale, in rem.** The type scale is `text-xs` 12 · `text-sm` 13 · `text-base` 14 · `text-lg` 16 ·
  `text-xl` 20 · `text-2xl` 26 px (at the 16 px root) and nothing else exists. Card padding is `p-card`
  (18 px), a card header `h-head` (56 px), a table row `h-row` (48 px). IBM Plex Sans and Mono. All of
  them are rem, and `styles.css` steps the root up on wide screens (base text 15 px from 1920 px, 16 px
  from 2560 px), so the whole interface grows together; size new things in rem (Tailwind's spacing
  already is), never px, or they will not follow. Page content is capped at `max-w-[120rem]`
  (1920 px at the base root); paragraphs keep their own readable widths (`max-w-3xl`).
- **Screens are built from `components/ui` only**, which is the one place allowed to set its own
  sizes. `src/test/source-rules.test.ts` fails on: grid tracks in `fr` without `minmax(0, …)`,
  `min-w-[…px]`, a raw `<h1>`, `toast.error`, arbitrary text sizes or padding, native checkboxes,
  radios and selects, and links hand-styled as buttons (use `ButtonLink`/`ButtonAnchor`).
- **Every text slot declares how it overflows.** User names and headings go through `Title` (wraps
  anywhere; detail pages show the whole name), identifiers in dense places through `Short` (shortens
  in the middle, full value as tooltip and for screen readers), identifiers on detail pages through
  `Ident`. Columns always shrink; tables take a `min` width per column and hide columns by
  `priority`; below 560 px rows become cards. Scroll areas carry `data-scroll-x`.
- **Long lists page** (`components/ui/pager.tsx`), 20 rows by default; the viewer picks 10, 20, 50 or
  100 (`bz.pageSize`, shared by every list). The list a page is about gets the full `Pager` (range,
  rows per page, page numbers) with its page in the URL (`?page=`, `pageSearch` in the router; a new
  filter drops it); a list inside a card that shares the page (a pipeline's versions, a wizard
  card) gets `CompactPager`, which renders nothing when everything fits; cursor-paged endpoints
  (audit log, activity, feedback) use `useCursorPages` (`lib/paging.ts`) with `CursorPager`
  (Newer / Older). Each list scrolls in its own box (`scrollBox`: `page` grows with the window,
  `card` stops at 28rem) with a sticky header row and the pager below it. `DataTable` does all of
  this (`paging`, `page`/`onPage`); other lists use `PagedBox`. Never add "Show all": it brings
  the long scroll back. The e2e suite reads lists 100 rows at a time (its `storageState`).
- **States and errors.** A query renders through `QueryState` (loading, error with Try again and the
  request ID, data). Errors appear inline with `InlineError` (what happened, why, what to do,
  request ID), never in a toast; toasts confirm successes only. A disabled button passes `why`.
- **URLs carry state**: tabs, filters and wizard steps are search params (shareable links).
- The command palette (`app/CommandPalette.tsx`, ⌘K) lists pipelines, connections, pages and the
  visible settings sections; new top-level pages or settings sections appear there automatically
  when added to `NAV` / `SETTINGS_SECTIONS`, otherwise add them to the palette's page list.
- App shell (`app/AppShell.tsx`): the desktop sidebar collapses to an icon rail (remembered per
  viewer in `localStorage` under `bz.sidebar.collapsed`, wrapped in try/catch); labels become
  `md:sr-only` and links get a `title`, so new nav items must keep a text label. Below `md` the
  navigation folds behind a menu button that closes on every navigation.
- Sidebar footer: **Help & tour** (a menu: Tour, Getting started, Learn, Find anything, Testing
  policy in a lab, and About BeziSync: `app/AboutDialog`, the server version from `/api/v1/system/info`,
  the license and links to `/licenses.txt` and `/api/v1/system/licenses`; `lib/about.ts`), **Feedback**, the inbox and the user menu. Add help pages to that menu, not as more
  footer links. The "New here?" card shows until `/tour` is visited or it is dismissed
  (`lib/tour.ts`, `localStorage` `bz.tour.seen`). `window.dispatchEvent(new Event(OPEN_PALETTE))`
  opens the command palette. The Tour (`features/tour/TourPage.tsx`) is static product copy: keep every
  claim true to the code when features change.
- Feedback: the sidebar's **Feedback** opens `features/feedback/FeedbackDialog` (every member); Admins
  read it in Settings → Feedback. `lib/feedback.ts` mirrors the backend kinds. When
  `useProviders().data.lab` is set, the sign-in page shows the open-lab note and links the public
  `/testing-policy` page (`features/auth/TestingPolicyPage`), which renders nothing outside a lab.
- Permissions come from `useProject().can(perm)`; hide actions the user cannot perform.
- Settings (`features/settings`) is for Admins only (`canOpenSettings(ctx)` = `me.user.is_superuser`
  gates the nav entry, the palette and the page). Anything every member needs (API tokens, notifications, mode)
  belongs on the Account page (`features/account`, `/p/$projectSlug/account`, user menu), and links
  into Settings from shared screens check the permission first.
- One place per job. `SETTINGS_SECTIONS` groups sections by what the Admin is doing (People &
  projects, Access, Running pipelines, Guardrails); a `perProject` section shows a project picker in the
  header. **People** is the only screen that edits access (filter `?in=<project id>` for one
  project). Work on pipelines stays with pipelines: the Debezium version is changed from the
  Pipelines list (column, `?debezium=` filter, bulk select) and the pipeline page with
  `ChangeDebeziumDialog`; `lib/debezium.ts` holds `useCatalog`/`useDebeziumImages`. Settings →
  **Engine versions** (`EngineVersionsSection`, `?engine=flink-cdc`) curates each engine's images;
  registering the first Flink CDC image turns that engine on (`lib/engines.ts`: `useEngines`,
  `useEngineCatalog`, `engineGate`). Destinations only Flink CDC writes carry a "Flink CDC" pill
  in the connection type picker and are disabled, with the reason, while it is off. Never add a
  second screen for the same job; renamed or merged section ids go in `LEGACY_SECTIONS` (old
  `/admin/$section` links redirect via `LEGACY_ADMIN_SECTIONS`).
  Checkboxes whose value comes from the server keep row-level local state set in the change
  handler (React flushes it synchronously), then save; a cache update in `onMutate` lands a tick
  later and leaves the box looking unresponsive.
- UX: one primary action per screen (the pipeline page picks it by state; Operate, Change and More
  menus hold the rest); destructive and powerful actions use `Confirm` with an `effects` list
  (and `typeToConfirm` for irreversible ones; the typed text clears on every open); secrets shown
  once use a closable={false} modal with "I've copied it"; forms with unsaved input pass `dirty`
  to `Modal`, and pages with unsaved work use the router's `useBlocker`; status = `Pill` (glyph +
  text, never colour alone); a report with an `unknown` check is a grey pill, never "Ready"; choose, don't type (Select, Combobox, Stepper, TimeRange, ChipsInput,
  SlugInput which shows the saved slug and never rewrites what is typed). Each fact appears once
  per page.
- Sign-in (`features/settings/SignInSection.tsx`, pure rules in `signin.ts`): providers start
  from a preset (Google, Entra ID, Okta, Keycloak, other OIDC); the login page and the session
  dialog render `features/auth/ProviderButtons` from `useProviders().data.providers` and hide the
  password form when `password` is false.
- Access: `app/guard.tsx` declares who may see a route (public, signed-out, signed-in) and
  `decide()` picks render, redirect or "can't reach": only a 401 means signed out. A 401 mid-session
  opens the sign-in dialog over the page (`sessionExpiry`) instead of navigating away. Home opens
  the last project used (`bz.lastProject`).
- Settings' project picker sets `?scope=<slug>` for per-project sections (rendered in a nested
  `ProjectProvider`); it never navigates to another project.
- `/__ui` (development builds only) renders every component with stress data.
- Engine settings (`lib/settings.ts`, `components/SettingsEditor.tsx`, `components/RawProperties.tsx`):
  `SettingsEditor` edits typed settings from `GET /engines` grouped by stage (a Stepper bounded by the
  registry and any project cap, a two-way choice for booleans, a Select for choices), each with its
  `ImpactPill`; `settingError`/`canonicalSetting` mirror `engine.CheckSetting`/`CanonicalSetting`.
  `RawProperties` edits raw properties as key/value rows and shows the server's verdict per key
  (`settings:classify`, `useClassify`), never its own rules. Maps the server stores (`advanced`,
  `tuning`) are compared in JSONB order (`jsonbSorted`), or autosave never settles. Autosave never
  retries a failed save by itself (`wizard/review.ts:useAutosave`): it waits for a change, Try again
  or leaving the step. Review's Engine
  settings card (Expert) holds both: `tuningSettings` picks the pipeline's settings (Flink settings
  stay in `FlinkRunSettings`), and a raw key that is a setting converts with "Use the typed setting".
  `SettingsChangeList` (`components/SettingsChange.tsx`) shows preflight's or the diff's
  `settings_change` grouped by impact, heaviest first: in Review's final checks, History's diff and
  deploy confirmation (`settingsChangeLines`), and the approval banner.
- Flink CDC custom clusters (`lib/cluster.ts`, `components/ClusterDiagram.tsx`,
  `wizard/FlinkClusterEditor.tsx`): Review's Size offers Custom to Experts (and keeps one a
  Guided user already has). It starts from the preset it was (`clusterFromPreset`: the same readers),
  offers only values the server accepts (`SLOT_CPUS`, `TM_MEMORIES`, …) and shows the server's
  packing (`cluster:pack`, `usePackCluster`), never its own: the diagram (wraps; `role="img"` with
  the words), why it splits, the total against the largest node and the fit check. `flinkSpecBlock`
  writes the cluster in `dumpCluster` order. Readers are locked while a deployed pipeline has a
  current saved position (`useSavedPositions`), as preflight's `flink_readers_changed` is.
  "Shrink after the first copy" sets `streaming_slot` (lists stop at the copy size,
  `streamingError` mirrors decodeCluster) and shows both diagrams with the peak and streaming
  totals. The Overview's cluster card reads `artifacts.flink_shape` (`shapeView`) and offers Shrink
  now under the `suggest` policy. Policies' Resources group edits `max_resources`,
  `resource_limits` (dropped when empty) and `shrink_after_copy` (left out when `auto`), and
  `blockedBy` counts each pipeline's server `footprint` (and the Flink presets for a custom cluster).
- `components/Recommendations.tsx` shows preflight's recommendations in Review in both modes
  (`freshRecommendations`: evidence under ten minutes old, a setting the pipeline can use, not yet
  chosen); Use sets the tuning value and autosave saves it.
- The adopt dialog shows the server's `plan` (`ImportBuckets`) and creates the draft from
  `plan.spec`; Change Debezium version previews `debezium-images:effects` before confirming.
- Setting limits (`settings/SettingLimits.tsx`) in Policies: add per setting, the impact count from
  `setting-limits:impact`, obsolete keys flagged. Review hides hidden settings, bounds int maxima
  (`caps`) and shows pins read-only (`SettingsEditor` `pinned`: "Set by project policy").
- Placement (`settings/PlacementProfiles.tsx`): the Kubernetes runtime edit modal edits its profiles
  (`profileError` blocks names and templates the server refuses); Review offers the chosen
  runtime's profiles as Placement and writes `placement` after `tuning`, only when set.
- `SchemaForm` renders descriptor JSON Schemas (connections, a sink's pipeline options): `ui.group`,
  `ui.expert`, `ui.labels` / `ui.descriptions` for enum values and `ui.when` (shown only while other
  fields hold listed values; hidden fields are dropped by `cleanValues`; `context` passes the sibling
  config form's values so a secret field can depend on a config field) and `ui.connection`
  (a pick-list of the project's connections of that `kind/type`, passed in as `links`). A boolean with
  `ui.labels` for `true` and `false` renders as a two-way choice; multi-line fields can load a file.
  `renderSecret` wraps secret inputs: the connection dialog uses it for `SecretFromStore` (Type it /
  From a secret store: store, listed secret, key pre-picked by `guessKey`; `lib/secretRefs.ts`), and
  saves only once a picked reference resolves, with Save anyway. The wizard reads a sink's
  capabilities with `sinkCapsOf` (naming rules and preview, allowed formats, options, required event
  shape and delete handling, minimum size) and mirrors the backend's checks (`sinkNamingError`); a
  source's with `captureOf` (snapshot modes offered, re-copies, canary), which also gates the
  Snapshots tab.
- MCP: Settings → MCP (`features/settings/McpSection.tsx`, Access group) edits the endpoint,
  sign-in and tool switches with one Save (`toolOverrides` sends only non-default choices), and
  lists agents and connections; Account → AI agents (`features/account/AgentsSection.tsx`) shows
  setup snippets (`features/mcp/mcp.ts`) and the person's connections; `/oauth/consent`
  (`features/mcp/ConsentPage.tsx`, signed in) is where people let an agent in.
- Copy to project (`features/connections/CopyDialog.tsx`, `lib/copy.ts`): one dialog for a connection's
  ⋯ menu, the Connections bulk bar and Promote (which passes `initialTarget` and picks the copies with
  `onCopied`). Every choice re-asks the server for the plan (`dry_run`); the copy button stays off
  until the plan is `ready`. A connection with `missing_secrets` shows "Needs its password" instead of
  its check badge. Secret stores: Edit is a visible button, saving sends `test_first` and offers
  "Save anyway" only after `store_test_failed`; `CopyStoreDialog` copies a store (Admins).
- Personal data (Tables step, `wizard/PersonalDataCard.tsx`, pure rules in `wizard/personal.ts`):
  a table picked for the first time gets `defaultsFor` (mask what `canMask` per the source
  descriptor's `transforms.maskable_types`, remove the rest); saved choices are never rewritten
  (`rulesToSave` keeps untouched tables byte-identical). A mask that can't be applied disables
  Continue, mirroring the blocking preflight check.
- New-pipeline wizard: Source → Tables → Destination → **Transform** → Review. A step unlocks when
  the ones before it are done (`stepState`); each step registers a `StepHandle` (`dirty`, `commit`)
  so leaving it by any route saves it, and Review saves its settings as they change. The Transform step
  (`features/pipelines/transforms/`) groups steps by the stage they run in (value formats, keep
  events, event shape, field changes, message key), drawn by `sectionOf`/`drawOrder` and ordered by
  `shapeIndex` (the server's boundary: the first unwrap or Keep deleted rows); `model.ts` holds the
  pure rules (stage-aware `addStep`/`moveStep`, `effectiveDeletes` for what deletes really become, `stepErrors` mirroring the backend decoder, `fieldsBefore` for pickers,
  `outputImpact` for Review and approvals), each type has one editor in `editors.tsx`, and
  `PreviewPanel` calls `…/transforms:preview` with the draft spec and the discovered columns
  (pure on the server). Field names are pickers fed by discovery; only new names and rule values
  are typed, validated like the server (`nameError`, `textError`, no `${`). A new step type adds a
  model case, an editor, a picker card and a test. Steps referring to removed tables are pruned
  on the Tables step (`pruneToTables`).
- Engines in the wizard: the Destination step works out the engines that can run the pair from
  `GET /engines` (`engineChoices`, `pickEngine`: never a copy of the backend's tables), shows a choice
  where both fit (Iceberg; Debezium is the default) or a "Runs on Flink CDC" line, sends `engine` only
  when the person picks one, and saves `specForEngine` (each engine refuses the other's settings). On
  Flink CDC there is no event format, the Tables step can't mask (hash) and says why
  (`caps.why.hash_mask`), and Review swaps the canary, re-copy window and advanced properties for
  `FlinkRunSettings` (schema changes, new columns, commit interval, Freshness for StarRocks/Doris,
  and in Expert the catalogue's Flink settings); sizes describe the cluster (`flinkSizeHelp`) and stay
  fixed once deployed.
- Flink CDC on the pipeline page: the Snapshots tab is **Positions** (`page/FlinkPositions.tsx`: saved
  positions newest first, Restore, and Start over as copy again / from now / since a time, the last
  gated by `caps.start_from_time` with its `why`; all need a pause and the typed name), Overview adds
  the cluster card (`flinkView`: job, TaskManagers, last save, first-copy chunks, components) and
  `transformView(spec, { sinkType })` describes tables kept in sync (never Debezium's event shape),
  Logs picks a component (`logComponents`, `?component=` on the API), the list has an Engine column,
  and Debezium-only actions (Change Debezium version, offsets) are not offered. Iceberg on Flink CDC
  shows only its options (`optionsSchemaFor`: no commit interval or buckets, no append) and records
  no partitions.
- Pipeline Overview (`features/pipelines/page/`): the flow strip, then a full-width chart, then
  Since start + Source protection beside Transforms + delivery. Side by side, the left column sets
  the row's height (at least 320 px): the right column is absolutely positioned inside its cell
  and Transforms scrolls (a focusable "Transform details" group); stacked, it takes its natural
  height. What the cards say is decided by
  pure functions in `page/overview.ts` (`protectionView` per source type, `transformView`), tested
  without rendering; a new source type with a probe adds its case there. Source protection never
  turns a missing value into a reassuring one (no "unlimited"): it is *Unknown* or *Not measured yet*.
  `TimeSeries` hides uPlot's legend and shows a readout (latest or hovered values, peak); its pure
  parts live in `lib/series.ts` because importing uPlot needs a browser.
- Guided vs Expert mode (`useProject().expert`): Guided uses plain language with the Debezium term
  as a secondary label; Expert reveals raw properties. Both edit the same spec. Guided never shows
  the raw editor, but a value that blocks saving is never hidden: Review lists it under "Settings
  that block saving" with Remove. An error names its field (`wizard/review.ts:reviewProblems`).
