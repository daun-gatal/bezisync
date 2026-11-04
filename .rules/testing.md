# Testing & definition of done

Each commit passes the **quick** gates for what it touches; a pull request passes the **full** gates
before it merges (who runs which is under *Who runs what* below):

```bash
# per commit (minutes)
make verify-quick GO_PKGS="./internal/api/ …"     # golangci-lint + vet + tsc everywhere, scripts, vitest, only these Go packages
make e2e-quick SPECS="tests/23-admin.spec.ts …"   # reuse the cluster, rebuild only changed images, run these specs

# per pull request
make verify   # backend: golangci-lint, vet, go test -race (+ schema, corpus and check-reference drift); tools: golangci-lint, vet, go test; frontend: API drift, tsc, vitest, build; e2e tsc
make e2e      # builds images, deploys into a disposable k3s-in-Docker cluster, runs the Playwright suite
```

- Choose the quick scope honestly: the Go packages and specs of every screen, endpoint and journey
  the change touches (a role or permission change touches identity, admin, SSO and approvals; the API
  tests live in `internal/api`).
- `make e2e-quick` tags images by the content of `backend/`, `frontend/` and `e2e/`, so unchanged
  images are neither rebuilt nor imported; it `helm upgrade`s the running release in place (the
  init containers migrate the database) and keeps its data, falling back to the full deploy when
  there is no release or the fixtures are not ready. Data from earlier runs remains, so a spec that
  assumes a clean database (e.g. the first page of the audit log) needs `make e2e`.
- Frontend unit tests need Node ≥ 22.12 (CI uses 24): under bun's Node shim vitest's jsdom workers fail to start.

- Backend tests need PostgreSQL: `make dev-db` starts one; `BEZISYNC_TEST_DATABASE_URL` points at it
  (the Makefile starts it when unset). Each test package creates, migrates and drops its own database
  (`internal/testdb`). The race detector needs cgo (a C compiler); CI always runs it.
- API tests (`internal/api/*_test.go`) run the real router against the database with an in-memory
  Kafka (`kfake`), a fake runtime driver (`installFake`), fake Vault and OIDC servers, and exercise the
  CLI and the agent end to end.
- `testdata/contract/` is the contract corpus recorded from the Python implementation (properties,
  `config_hash`, validation messages, check reports, YAML, diffs): the Go code must keep matching it.
- `make e2e` recreates the BeziSync and runtime namespaces every run (clean database) and keeps the
  data-plane fixtures (`deploy/e2e/fixtures/`, namespace `bezisync-e2e-data`). On failure the
  Playwright report and all pod logs land in `.e2e/`. `make e2e-down` removes everything it created
  (all resources carry the label `app.bezisync.io/e2e=true`).
- Fixtures: Postgres (+ legacy), MySQL, SQL Server, MongoDB (replica set via a headless service),
  Kafka, HTTP echo, Redis, NATS JetStream, Kinesis (`kinesis-mock`; LocalStack now needs a
  licence), Pub/Sub emulator, RustFS (S3 storage plus its Iceberg catalog, "S3 Tables") with
  Apache Iceberg's REST catalog fixture, and a DinD "remote Docker host". Quick mode applies the
  fixtures too (unchanged ones are untouched) and pulls sinks' own Debezium Server builds
  (descriptor `sink.image`) inside k3s by digest. `TestIcebergLive` (connectors) runs the Iceberg
  checks against a real catalog when `BEZISYNC_TEST_ICEBERG_URI` (and `_WAREHOUSE`, `_S3`, `_KEY`,
  `_SECRET`) are set, e.g. a RustFS container. `TestOracleLive` and `TestCockroachDBLive` run a
  source's checks, discovery, prepare, signal, canary, probe and cleanup against a database prepared like
  its e2e fixture when `BEZISYNC_TEST_ORACLE` / `BEZISYNC_TEST_COCKROACHDB` (`host:port`) are set. Source
  fixtures write rows from the runner (NodePorts) or, for Oracle, with `kubectl exec … sqlplus`. Agent journeys run in namespace
  `bezisync-e2e-agent` (recreated); the Playwright runner has `kubectl`/`helm`, an in-network
  kubeconfig and the repo's `deploy/` directory, so tests install agents as users would.
- `E2E_GREP="pattern" scripts/e2e.sh` runs a subset while iterating; commit only after a full run.
- The e2e suite runs locally, not in CI (its ~15 data-plane services need more time and disk than a
  CI runner offers, ~8 GB free); CI type-checks the suite (`e2e` tsc) only. See *Who runs what*.
- Layout is a gate: `e2e/tests/28-layout.spec.ts` seeds names at their longest (63-character slugs,
  a long project name) in its own `layout-stress` project and sweeps every main screen at 390, 768
  and 1280 px with `layoutProblems()` (`e2e/tests/layout.ts`): no overflow, no sideways page
  scrolling, font sizes only from the scale, no text squeezed into a narrow column, card headers
  56 px and table rows at least 48 px. Custom selects are driven with `choose(trigger, option)`
  from `helpers.ts`.
- Accessibility is a gate: `e2e/tests/16-accessibility.spec.ts` runs axe (WCAG 2.2 AA) on the main
  screens in light and dark mode and fails on serious/critical violations. Colours come from
  tokens only; text on an accent fill uses `text-on-accent`.
- Chaos and scale journeys (`e2e/tests/19-resilience-scale.spec.ts`): a connector killed mid-stream
  must lose no rows; killing each controller pod in turn must keep status fresh (leader
  failover); 1,000 source tables and 200 pipelines must stay quick in discovery, the list and the
  wizard. Keep them passing when touching the controller, drivers or list/wizard queries.
- `docs/reference/checks.md` is generated (`bezisync check-docs`); a test fails when it is stale, and
  another when a check code is emitted without an entry in `internal/checks/catalog.json`.
- `make demo-smoke` (also in CI) proves the compose demo stack end to end.
- What to test: pure logic first (checks, compiler, health derivation, policies) with table tests;
  endpoints for authz (404 concealment, 403 permissions), validation and secret non-disclosure;
  every user journey end-to-end in `e2e/tests/` against real Postgres, Kafka and Debezium Server.
- A bug fix starts with a failing test.

## Who runs what

BeziSync is open source (Apache 2.0), and not every contributor can run the whole e2e cluster.

- **Contributors** run `make verify` (CI runs it again) and add or update tests for every behavioural
  change. When the change touches a screen, endpoint or journey, they also run the affected specs with
  `make e2e-quick SPECS="…"` if they can. Either way, the PR's *Testing* section names the specs they ran
  or says "not run". Commits inside a PR do not each have to pass the gates: pull requests are squash-merged.
- **Maintainers** run the full `make e2e` on the PR's head commit before merging any change outside
  docs-only paths (`docs/`, `*.md`, `.github/ISSUE_TEMPLATE/`), and post the result on the PR (pass,
  or the failing specs). A head commit pushed after that run needs a new run.
- Maintainers working on their own branches keep the stricter habit: the quick gates on every commit.

CI runs on pull requests to `preview` (see `releases.md`): `ci.yml` runs the same gates plus the
version and changelog-fragment checks (a fragment exactly when an image changes, a new version for
a changed chart), the manual-edit guard, the source scan, the images built and scanned (when an
image changes), the e2e type check and the demo smoke test; `helm.yml` runs the chart gates below.

## Helm charts

```bash
make chart-test   # helm lint --strict with every ci/*-values.yaml, helm-unittest, READMEs current (in Docker)
make chart-docs   # regenerate deploy/helm/*/README.md (README.md.gotmpl + values.yaml `# --` comments)
```

- `deploy/helm/<chart>/ci/*-values.yaml` are the scenarios CI lints, renders (kubeconform on three
  Kubernetes versions) and, for the control plane's `default` and `external-postgres`, installs on
  kind with the change's images; the agent chart is installed there against that control plane and
  must report in.
- `deploy/helm/<chart>/unittests/*_test.yaml` are helm-unittest suites (template failures, defaults,
  optional paths); `templates/tests/` holds the `helm test` pods (the bezisync image's own
  `healthcheck`).
- Every values key carries a `# --` comment: it becomes the chart README's values table.
