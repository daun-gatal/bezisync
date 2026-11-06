# auth

## Overview
Public, signed-out screens: the sign-in page (password and/or single sign-on buttons) and the lab's testing policy. They sit outside the project shell; `app/guard.tsx` decides who reaches them and sends signed-in people on.

## Interface
- `[LoginPage.tsx:LoginPage]` — sign-in form for `/login`: lab banner, provider buttons, password form (unless an Admin turned password sign-in off), the `error` search param as a notice.
- `[ProviderButtons.tsx:ProviderButtons]` / `[ProviderButtons.tsx:providerHref]` — one anchor per enabled SSO provider pointing at `/api/v1/auth/oidc/login?provider=…&next=…`; `SignInOption` is the API schema type.
- `[TestingPolicyPage.tsx:TestingPolicyPage]` — public `/testing-policy` page, only on an open lab (`providers.lab`); otherwise an error panel. `[TestingPolicyPage.tsx:accountsPhrase]` turns provider labels into "a Google or GitHub account".

## Invariants
- `next` is always passed through `[../../app/guard.tsx:safeNext]` before it reaches a provider link, so only same-origin relative paths (not `//`, `/\`, `/login`) are used.
- Both query parameters in `[ProviderButtons.tsx:providerHref]` are `encodeURIComponent`-escaped.
- Password fields show only when `providers.password` is not false (default true while loading) (`[LoginPage.tsx:LoginPage]`).
- The policy page never offers the wrong header action: nothing while the session is loading (`[TestingPolicyPage.tsx:HeaderAction]`).

## Side Effects
- HTTP: `GET /api/v1/auth/providers` (`useProviders`), `POST /api/v1/auth/login` (`useLogin`), `GET /api/v1/auth/me` (`useMeQuery`), all in `[../../lib/session.tsx:useLogin]` and siblings. Provider buttons navigate the browser to the server's SSO start endpoint.

## Verification
- Run: No co-located tests; covered by `cd frontend && bun run test src/app/__tests__/guard.test.tsx` (renders `LoginPage`, `TestingPolicyPage`, `accountsPhrase`) and `cd frontend && bun run typecheck`.
