# oidc

## Overview
Package `oidc` implements SSO sign-in with an authorization code and PKCE, for OpenID Connect
providers (ID token verified against the issuer's JWKS) and plain OAuth2 providers such as
GitHub (identity read from a userinfo endpoint). The login transaction travels in an
encrypted, short-lived cookie, so no server-side state is kept. It also defines the shared
`Identity` and provider-kind constants that [`samlsp`](../samlsp/README.md) and the SSO
handlers in [`api`](../api/README.md) use.

## Interface
- `[oidc.go:Provider]` — one configured identity provider (kind, issuer, client, scopes, claim names, OAuth2 endpoints, GitHub mode); `[oidc.go:Claims]` names its claims.
- `[oidc.go:Begin]` — returns the provider's authorization URL (state, nonce for OIDC, S256 PKCE challenge) and the encrypted transaction cookie value.
- `[oidc.go:Finish]` — validates the transaction cookie and state, exchanges the code, verifies the identity and returns an `[oidc.go:Identity]` plus the return path.
- `[oidc.go:Discovery]` (cached one hour) and `[oidc.go:Test]` (fresh discovery + JWKS check, for Settings → Sign-in → Test).
- `[oidc.go:RedirectURI]` — `<PublicURL>/api/v1/auth/oidc/callback`, shared by every provider.
- `[claims.go:Lookup]` — claim by exact name, then as a dotted path (`realm_access.roles`).
- Errors: `[oidc.go:Error]` (message safe to show), `[oidc.go:ErrUnknownProvider]`, `[claims.go:ErrGroupsOverage]`, `[oauth2.go:ErrGitHubPartial]`.
- Constants: `[oidc.go:Cookie]` / `[oidc.go:CookiePath]` / `[oidc.go:TTL]` (`bz_oidc`, `/api/v1/auth/oidc`, 600 s); `[oidc.go:KindOIDC]`, `[oidc.go:KindOAuth2]`, `[oidc.go:KindSAML]`.

## Invariants
- Every provider request goes through the SSRF-guarded client with a 10 s timeout and a `BeziSync/<version>` User-Agent (`[oidc.go:client]`, `[claims.go:userAgent]`).
- The transaction cookie is a `security.EncryptSecret` envelope holding state, nonce, verifier, next path and provider id; it expires after `TTL`, and `state` is compared in constant time (`[oidc.go:Begin]`, `[oidc.go:Finish]`).
- The provider is looked up again at callback time; a removed or disabled one fails with `ErrUnknownProvider` (`[oidc.go:Finish]`).
- RFC 9207 mix-up defence: OIDC rejects a callback `iss` that differs from the discovered issuer, or a missing one when the provider advertises `authorization_response_iss_parameter_supported`; OAuth2 checks `iss` only when `ResponseIssuer` is set (`[oidc.go:Finish]`).
- ID tokens must verify (signature with the advertised algorithms, default RS256; issuer; audience = client id; expiry), carry `exp`, `iat`, `sub`, and the transaction's nonce (`[oidc.go:Finish]`).
- An email is required; it counts as verified only when the provider says so (a missing claim is "no"); unverified addresses fail unless `AllowUnverifiedEmail` (`[oidc.go:Finish]`, `[oauth2.go:finishOAuth2]`). For GitHub only a verified address from `/user/emails` counts (`[oauth2.go:githubEmail]`).
- Group overage (Entra `_claim_names`) or GitHub SAML partial results refuse the sign-in rather than dropping roles (`[claims.go:groupsFrom]`, `[oauth2.go:getPage]`).
- Subjects are namespaced: `<issuer>|<sub>` for OIDC, `oauth2:<issuer>|<sub>` for OAuth2 (`[oidc.go:Finish]`, `[oauth2.go:finishOAuth2]`).
- OAuth2 answers (token, userinfo, GitHub API) are read through a 1 MiB limit; GitHub org/team paging stops after 10 pages or at a `Link` to another origin (`[oauth2.go:maxBody]`, `[oauth2.go:githubPages]`, `[oauth2.go:sameOrigin]`). OIDC discovery, the `Test` JWKS fetch and the OIDC token answer are read without a size limit (`[oidc.go:Discovery]`, `[oidc.go:Test]`, `[oidc.go:Finish]`).
- The return path is stored as given; callers sanitise it before `Begin` (`[oidc.go:nextOf]`).

## Side Effects
- Outbound HTTPS to identity providers: discovery, JWKS, token, userinfo and GitHub API endpoints.
- Process-wide caches: discovery documents (one hour) and remote key sets per JWKS URL (`[oidc.go:Discovery]`).
- Reads `PublicURL` and the master key (via `security`) from `config.Get()`.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/oidc/` and by the SSO API tests: `make verify-quick GO_PKGS="./internal/api/"`.
