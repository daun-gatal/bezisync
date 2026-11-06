# Pre-PR gate

The checklist every coding agent passes **before** `gh pr create`. A PR is only raised once every
step that applies passes. When a step cannot be run, say so in the PR's *Testing* section and give
the reason; never mark it as passed.

Each step names the rule file that owns the detail. This page fixes the order and what "done" means.

## 1. Branch and scope

- [ ] The branch was cut from an up-to-date `preview` (`git fetch && git switch -c <type>/<slug> origin/preview`). If another session uses the main checkout, work in a separate `git worktree` and never touch that session's checkout, cluster or containers.
- [ ] The PR targets **`preview`**, never `main`. If you are about to push to an existing PR branch, check the PR is still open (`gh api repos/daun-gatal/bezisync/pulls/<n> --jq .state`). Once a PR is open, follow-ups go in a new PR.
- [ ] Batched requests from one thread ship as **one PR with separate logical commits**. Unrelated work goes in its own PR.
- [ ] `git status` shows only the intended changes: nothing from `.egm/` or `.e2e/`, no scratch files, no hand edits to generated files (`CHANGELOG.md`, the app version, the chart READMEs, `frontend/openapi.json`, `docs/reference/checks.md`, the licence files).

## 2. Obligations triggered by what changed

| If the diff touches… | Then, in this PR… | Rule |
|---|---|---|
| A published image (`backend/` or `frontend/` outside its `.dockerignore`, including `Dockerfile` / `.dockerignore`) | Add exactly one fragment, `changelogs/unreleased/<pr>-<slug>.md`. Changes that reach no image take none. | [releases.md](releases.md#what-a-pull-request-carries) |
| `deploy/helm/<chart>/` | Bump that chart's `version` (unpublished, newer than every published one), then run `make chart-docs` and `make chart-test` | [releases.md](releases.md#what-a-pull-request-carries), [testing.md](testing.md#helm-charts) |
| The database schema | Add the goose migration (`make migration m=…`) and update `internal/migrate/testdata/schema.txt` | [backend.md](backend.md#rules) |
| An API route or payload | Change `internal/api/openapi.json` with the handlers, then run `make openapi` | [backend.md](backend.md#rules) |
| Preflight checks | Add the catalog entry and regenerate `docs/reference/checks.md` (`bezisync check-docs`) | [testing.md](testing.md) |
| A dependency | Run `make licenses` (and stay inside the allow-list) | [testing.md](testing.md#licence-files) |
| The compiler or connector properties | Prove existing specs still compile to the same `config_hash`, because the controller replaces drifted pipelines | [backend.md](backend.md#pipelines-compiler-runtimes-controller) |
| Secrets, auth/RBAC, SSRF, agents, MCP or outbound calls | Review against the threat model and update it | [threat model](../docs/security/threat-model.md) |
| User-visible behaviour, or how BeziSync is built, run or tested | Update the guide (`docs/README.md`) and the matching `.rules/*.md` / `AGENTS.md` (the self-updating mandate) | [AGENTS.md](../AGENTS.md) |
| A big, multi-component decision | Include its `Accepted` ADR (and its index row) in this PR (the default), or reference the merged ADR | [adr.md](adr.md#the-flow), [egm.md](egm.md#from-idea-to-pr) |
| Code in a directory with a `README.md` contract | Update the contract where exports, invariants, side effects or verification changed, and add a contract for every new code directory | [egm.md](egm.md#directory-contracts) |

## 3. Tests for the change

- [ ] Every behavioural change has tests. A bug fix starts with a failing test.
- [ ] For an ADR-driven change, every edge-case register row has its test, and that test is green.
- [ ] Endpoints are tested for authz (404 concealment, 403 permissions), validation and secret non-disclosure ([testing.md](testing.md)).

## 4. The gates

Per commit (quick, on what it touches), pushed right after:

```bash
make verify-quick GO_PKGS="./internal/<pkg>/ …"
make e2e-quick SPECS="tests/<NN>-<name>.spec.ts …"
```

Per pull request (full):

```bash
make verify          # includes licenses-check and contracts-check
make e2e             # disposable k3s-in-Docker; maintainers run it on the head commit before merge
```

Add these when they apply: `make chart-test` (charts) and `make demo-smoke` (the compose demo).
For a docs-only PR (`docs/`, `*.md`, `.github/ISSUE_TEMPLATE/`), `make contracts-check` and the
link checks are enough. [testing.md](testing.md#who-runs-what) says who runs what.

## 5. Behaviour in the real app

- [ ] **Screens:** check them at 390, 768 and 1280 px, in light and dark mode, with the longest names (the layout spec's rules). Cover the empty, loading, error, Unknown/stale and permission-denied states. Attach screenshots.
- [ ] **API and CLI:** call them as a permitted and as a denied caller. Confirm the status code, the audit entry, and that no secret appears in responses or logs.
- [ ] **Data plane:** run the change end to end through e2e (real Postgres, Kafka, Debezium Server, and Flink CDC where it applies), not only against mocks.

## 6. Edge cases

Write the edge-case list for the change (for an ADR, its register) and check each row. At
minimum, cover the ones that apply:

- [ ] Empty, maximal and 63-character inputs, Unicode, quoting.
- [ ] Each role and boundary: Viewer/Editor/Admin, project membership, API token, MCP agent, SSO user, approvals and policies.
- [ ] State safety: active, paused, failed and drifting pipelines. Destructive actions show their effect and are audited.
- [ ] Failure paths: the runtime or source unreachable, timeouts, controller failover, retries and idempotency, no lost rows.
- [ ] Upgrade path: existing data and specs survive (the same `config_hash`), and migrations apply to a database that already exists.

## 7. Self-review

- [ ] Review the whole diff as if someone else wrote it, against `AGENTS.md`'s golden rules and the area `.rules/*.md`.
- [ ] Remove dead code, unused exports and debug output.
- [ ] Keep secrets, tokens and personal data out of code, logs, fixtures and screenshots.
- [ ] Make one commit per logical step, with a message that says *why*.

## 8. The PR itself

- [ ] Fill in every section of [`.github/pull_request_template.md`](../.github/pull_request_template.md). In *Testing*, list the exact commands, the specs run and their results, plus anything that could not be run and why.
- [ ] Link the ADR or issue, and list any follow-ups you found but left out of scope.
- [ ] After pushing, watch CI and fix every red check before asking for review. Then tear down any test infrastructure you created.
