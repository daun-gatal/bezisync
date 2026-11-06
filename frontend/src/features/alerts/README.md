# alerts

## Overview
The Alerts page with three tabs: Open alerts (with resolved history), Rules and notification Channels. It is a thin screen over the hooks in `lib/alerts.ts`; channel forms are rendered from each channel kind's schema.

## Interface
- `[AlertsPage.tsx:AlertsPage]` — the page for `/p/$projectSlug/alerts`; the tab is the `?tab=` search param (`firing` default, `rules`, `channels`). Internal components: `OpenAlerts` (paged list, through `AlertList`), `AlertCard` (acknowledge), `Rules` (enable, severity, fire-after delay, channels, email the owner), `Channels` (create, edit, test, delete) and `ChannelDialog`.

## Invariants
- Acknowledge is offered only for `firing` alerts and with `alert.ack`; rule and channel changes need `alert.manage`: rule controls are disabled (the enable switch says why), channel add/edit/delete/test buttons are hidden (`[AlertsPage.tsx:AlertCard]`, `[AlertsPage.tsx:Rules]`, `[AlertsPage.tsx:Channels]`).
- Channel secrets are write-only: editing sends `secrets` only when a secret field was filled, so blanks keep the stored value (`[AlertsPage.tsx:ChannelDialog]`); config (except an email channel's `recipients` list) and secrets go through `cleanValues`.
- Deleting a channel lists the rules that stop notifying it and states that its stored secrets are destroyed.
- The Open list is paged with its page in the URL (`?page=`); the Recently resolved list pages locally.
- A channel's kind is fixed once created; Save needs a name and, for email, at least one recipient (`[AlertsPage.tsx:ChannelDialog]`).

## Side Effects
- HTTP via `[../../lib/alerts.ts:useAlerts]` and siblings: `GET …/alerts`, `POST …/alerts/{alert_id}/acknowledge`, `GET …/alert-rules`, `PATCH …/alert-rules/{rule_id}`, `GET …/alert-channel-kinds`, `GET/POST …/alert-channels`, `PATCH/DELETE …/alert-channels/{channel_id}`, `POST …/alert-channels/{channel_id}/test`.

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run typecheck` and `cd frontend && bun run test src/test` (source rules).
