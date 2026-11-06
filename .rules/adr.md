# Architecture Decision Records

When and how BeziSync records a significant design decision. ADRs live in
[`docs/adr/`](../docs/adr/README.md), which holds the template and the index.

## When an ADR is required

Write one **before** implementation when a change is big: it crosses components or introduces a
decision that outlives the PR. Typical triggers:

- security boundaries, authn/authz, secret handling, SSRF, anything in the [threat model](../docs/security/threat-model.md);
- data flow and state (the job queue, offsets and saved positions, `config_hash` and drift replacement);
- a new engine, runtime driver, connector family or outbound interface (MCP tools, agents, CLI);
- deployment topology (charts, the agent, multi-replica behaviour);
- anything spanning API + controller + UI at once, or with non-obvious trade-offs.

**Skip it** for bug fixes, small features, refactors and anything self-contained. This is a
reasonable process, not a hard gate. If you'd want to agree the approach before writing code,
write an ADR.

## The flow

1. **Draft:** write `docs/adr/NNNN-kebab-title.md` with status `Proposed` and add its row to the
   index in `docs/adr/README.md`.
2. **Discuss:** settle the approach with the maintainer, in the working session or in review on a PR.
3. **Accept:** flip the status to `Accepted` once the maintainer approves the design. The accepted
   ADR is now the spec.
4. **Implement:** build it against the ADR, in one of two shapes:
   - **Same PR (default).** The accepted ADR and its implementation ship together in one PR,
     approved before any code is written.
   - **ADR first.** An ADR-only PR (`Proposed` → discussed → merged as `Accepted`), then the
     implementation in follow-up PRs that reference it. Use this when the decision needs wider
     review or the work spans several PRs.

   Either way, if the implementation has to depart from the ADR, update the ADR (while it is
   unmerged) or supersede it. The merged ADR and the code must never disagree.

## Conventions

- Status lifecycle: `Proposed` → `Accepted` → (`Superseded by NNNN` | `Deprecated`).
- **Never edit an accepted ADR's decision in place.** Write a new ADR that supersedes it, and
  set the old one's status to `Superseded by NNNN`.
- Keep the index in `docs/adr/README.md` current.
- ADRs don't change releases. An ADR-only PR is docs-only: no changelog fragment, and no
  maintainer e2e run ([testing.md](testing.md#who-runs-what)).
- Designs written with the EGM skills follow [egm.md](egm.md): the EGM design document *is* the ADR.
