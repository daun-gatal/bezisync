# feedback

## Overview
The feedback form people open from anywhere in the app (bug, idea, security report, other). Only Admins read what it sends, in Settings → Feedback ([../settings](../settings/README.md)).

## Interface
- `[FeedbackDialog.tsx:FeedbackDialog]` — modal taking `open`, `onOpenChange`, `page` (the app path, sent so Admins can reproduce) and `initialKind`; shows a thank-you state after sending with "Send another".

## Invariants
- Every opening starts fresh: kind, message and mutation state reset when `open` turns true; a draft survives only while the dialog stays open (`[FeedbackDialog.tsx:FeedbackDialog]`).
- Empty or whitespace-only messages are never sent; the message is capped at `[../../lib/feedback.ts:MAX_FEEDBACK]` characters.
- The browser description is computed from `navigator.userAgent` for display (`[../../lib/feedback.ts:describeBrowser]`); the request body is only `{ kind, message, page }`.

## Side Effects
- HTTP: `POST /api/v1/projects/{project_id}/feedback` with the current project. Reads `useProviders` (`GET /api/v1/auth/providers`) for the lab wording.

## Verification
- Run: `cd frontend && bun run test src/features/feedback`
