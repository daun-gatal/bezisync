# tour

## Overview
The in-app Tour page: a static walkthrough of what BeziSync does (problems it removes, pipeline stages, what to try in the current project, what Admins add) for people who only see part of the product, ending with a one-click "Would your team use BeziSync?" vote.

## Interface
- `[TourPage.tsx:TourPage]` — the page; content comes from module constants (`PAINS`, `STAGES`, `TRY`, `ADMIN`, `ANSWERS`, `SOURCES`, `DESTINATIONS`, `RUNTIMES`); after a successful vote a "Tell us in Feedback" button opens [../feedback](../feedback/README.md)'s `FeedbackDialog` with `initialKind="idea"`.

## Invariants
- The vote is recorded as ordinary feedback (`kind: 'other'`, message "Would your team use BeziSync? <answer>", `page` = `/p/<slug>/tour`); the answer buttons disable after one click and a failed vote clears the chosen answer (`[TourPage.tsx:TourPage]`).
- Sample-data wording is used when the instance is a lab or the project `is_sandbox`.

## Side Effects
- HTTP: `POST /api/v1/projects/{project_id}/feedback` (the vote); `GET /api/v1/auth/providers` for `lab`.

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run typecheck` and `cd frontend && bun run test src/test` (source rules).
