# 0002 — Project policies that can't be read fail closed

- **Status:** Accepted
- **Date:** 2026-11-07
- **Scope:** `backend/internal/domain/policies.go` (`LoadPolicies`, `ParsePolicies`, `Dump`, `SaveViolations`, `EvaluatePoliciesOn`, `EngineViolation`, `UnreadableCheck`), `backend/internal/services` (`PoliciesOf`, `UnreadablePolicies`, `RefuseSettingLimits`, `UpdatePipeline`, `PolicyChecks`, the connection copy plan), `backend/internal/checks/catalog.json` and `docs/reference/checks.md` (`policy_unreadable`), `GET /projects/{id}/policies` (`openapi.json`), `frontend/src/features/settings/PoliciesSection.tsx`, the domain and services contracts, the threat model, `.rules/backend.md`, Known issue 3.

## Context

Admins set project policies (allowed runtimes, sources, destinations, engines, transforms, the
largest size, approvals, setting limits). They are stored as JSON in `projects.policies` and read
through `services.PoliciesOf` → `domain.LoadPolicies` on about fifteen paths: save checks,
preflight, deploy, approvals, copy, manifests, engine lists, setting limits, and the controller's
Flink CDC shrink on every tick.

`LoadPolicies` fell back to `DefaultPolicies()` when the stored JSON didn't parse. The default
allows everything: no allow-lists, force deploy on, no approval. Stored policies that stop parsing
(after a release tightens `ParsePolicies`, a downgrade that meets a newer key, or a hand edit of the
database) silently removed every guardrail of the project (Known issue 3). Nobody was told.

What makes the fix hard:

- **Reading must keep working.** The controller reads policies while it reconciles running
  pipelines. An error there would stop pipelines being observed, and pause, stop and delete with
  them.
- **The Admin must be able to repair it,** from the same form, without losing what was set. If GET
  returned defaults, an Admin pressing Save would store allow-everything; if it returned only
  strict values, every allow-list and limit the Admin had set would be gone.
- **`nil` means "any".** The strictest value of an allow-list is an empty list, not `nil`.
- **Policy checks are spread out.** Not every save reaches `SaveViolations`: a `PATCH` that changes
  only the spec never did, the engine check and the copy plan read allow-lists directly, and
  preflight appends setting-limit and resource checks of its own.

## Decision

1. **`LoadPolicies` fails closed and keeps what it can.** For stored JSON that fails to parse it
   returns `Unreadable: true`, `Problems` (each parse error as `field: message`, never a value),
   every field that parsed as stored, and the strictest value in each field that didn't:
   allow-lists `[]`, `max_resources` `small`, `max_pipelines_per_source` 1, `allow_force_deploy` and
   the `allow_*` switches off, `require_approval` on, `shrink_after_copy` `suggest`. Stored data
   that isn't an object makes every field strict. `nil` / `{}` keep meaning "defaults".
2. **One reason, everywhere a change is checked:** `domain.UnreadableCheck()` (`policy_unreadable`,
   blocking, fix for an Admin: "open Settings → Policies, fix the fields it lists and save"):
   - `SaveViolations`, `EvaluatePoliciesOn` and `EngineViolation` return only it, so preflight and
     every runtime/source/destination/engine save show that one reason;
   - `services.PolicyChecks` returns only it (no setting-limit or resource checks on strict values);
   - `services.UnreadablePolicies()` (409 `policy_unreadable`) refuses a `PATCH` whose spec changes
     (saving the same spec again, as an autosave does, isn't a change), and `RefuseSettingLimits`
     refuses creates and manifest applies;
   - the connection copy plan lists it as the problem of every item.
   A forced deploy is refused too: forcing never skips the policy section.
3. **Reads keep working.** The controller keeps reconciling on the loaded values. Pause, resume,
   restart, stop and delete don't evaluate policies. Approvals count as required where
   `require_approval` failed to parse.
4. **`GET /policies` says so.** The response adds `unreadable: true` and `problems` (both omitted
   otherwise, so a readable dump is unchanged). `ParsePolicies` accepts and ignores both keys, so
   the form's draft round-trips. The Settings page shows a notice listing the problems and keeps
   Save enabled while unreadable; an empty allow-list (a field that failed) must be filled before
   saving, as the form already requires. Saving stores valid policies and clears the state.
5. **Visible to operators.** `PoliciesOf` logs a warning with the project id and the problem count.
6. **Rule for future releases:** parsers of stored data keep accepting what they accepted (as
   `require_pii_masking` already is), recorded in `.rules/backend.md`.

Out of scope: a `policies` value that isn't a JSON object fails earlier, when the project row is
scanned. BeziSync only ever writes an object there.

### Implementation plan

One commit: domain + services + catalog/check reference + OpenAPI + frontend + contracts + threat
model + this ADR, removing Known issue 3.

### Verification plan

| ID | Case | Expected | Test |
|---|---|---|---|
| P1 | Stored policies with a wrong value and an unknown key | Unreadable, two problems, parsed fields kept, failed ones strict (`[]`, not `nil`) | `domain` `TestLoadPoliciesFailsClosed` |
| P2 | `nil` / `{}` stored | Defaults, not unreadable | same |
| P3 | Preflight / deploy / forced deploy in an unreadable project | One `policy_unreadable`; deploy 409; force refused | `api` `TestUnreadablePoliciesFailClosed` |
| P4 | `PATCH` with a changed spec, create | 409 `policy_unreadable` | same |
| P5 | Pause / resume | Still work | same |
| P6 | Controller tick | Pipeline still observed (streaming) | same |
| P7 | `GET /policies` | `unreadable`, problems without values, kept and strict fields | same |
| P8 | `PUT` of the GET body by an Admin | 200; readable again; preflight clean | same |
| P9 | Readable policies | Unchanged dump; existing policy tests pass | `domain`, existing `api` policy tests |

## Consequences

- A broken policy blocks changes in that project, with one reason, until an Admin saves, instead of
  silently allowing everything. Running pipelines keep running.
- Two read-only fields in the policies response (OpenAPI) and one new check code.
- The Admin keeps every field that parsed and re-chooses only the ones that didn't.

## Alternatives considered

- **Return an error from `LoadPolicies`.** Stops reconcile of running pipelines and stops the Admin
  from loading the form to repair it.
- **Keep falling back to defaults and only log.** The guardrails stay off; a log is not a control.
- **Return only strict values.** Safe, but throws away every allow-list and limit the Admin set.
- **Return the raw stored JSON from GET.** The form can't render arbitrary invalid JSON; kept
  fields plus the problem list say the same thing safely.
- **Strict values without a dedicated check.** Every allow-list failing at once gives a dozen
  confusing failures ("runtime not allowed", "source not allowed"…) that hide the real cause.
