# samlsp

## Overview
Package `samlsp` is BeziSync as a SAML 2.0 service provider (Settings → Sign-in, kind `saml`).
One SP serves every SAML IdP: its entity ID is its metadata URL, AuthnRequests go out with the
HTTP-Redirect binding, and the ACS receives HTTP-POST responses. Signature, issuer, destination,
recipient and status checks come from gosaml2/goxmldsig; this package adds the checks they leave
to the caller. It is pure (no DB, no network): `services/sso_kinds.go` stores the keys and the
request state and enforces replay protection, and the auth handlers in [`api`](../api/README.md) serve it.

## Interface
- `[samlsp.go:IdP]` — one configured IdP (entity ID, SSO URL, signing certificates, `SignRequests`, `AllowIdPInitiated`, attribute names).
- `[samlsp.go:BuildRequest]` — an AuthnRequest's ID and the redirect URL carrying it and the relay state.
- `[samlsp.go:ParseResponse]` — verifies a posted `SAMLResponse` against the expected request ID into a `[samlsp.go:Result]` (assertion ID and expiry for the replay guard, NameID, email, name, groups).
- `[samlsp.go:ParseMetadata]` — reduces IdP metadata to `[samlsp.go:Metadata]` (entity ID, HTTP-Redirect SSO URL, signing certificates, `WantsSigned`).
- `[samlsp.go:SPMetadata]` — this SP's metadata document for Admins to give their IdP.
- `[samlsp.go:NewKeys]` (RSA-2048 + 10-year self-signed certificate), `[samlsp.go:LoadKeys]`, `[samlsp.go:Keys]` — the SP key pair.
- `[samlsp.go:ParseCertificate]` (PEM or bare base64), `[samlsp.go:PEM]`, `[samlsp.go:ValidSSOURL]` — configuration helpers.
- `[samlsp.go:EntityID]`, `[samlsp.go:ACSURL]`, `[samlsp.go:MetadataPath]`, `[samlsp.go:ACSPath]`, `[samlsp.go:Skew]` (2 min), `[samlsp.go:MaxMetadata]` (1 MiB); `[samlsp.go:Error]` — a message safe to show.

## Invariants
- A response must carry exactly one assertion (`[samlsp.go:ParseResponse]`).
- With an expected request ID, both the response and the subject confirmation `InResponseTo` must equal it; with none (IdP-initiated), any `InResponseTo` is refused (`[samlsp.go:ParseResponse]`). Whether IdP-initiated is allowed is decided by the caller.
- `Conditions` with both `NotBefore` and `NotOnOrAfter` are mandatory and checked with `Skew`; at least one audience restriction is required and every restriction must name `EntityID()` (`[samlsp.go:ParseResponse]`).
- The assertion needs an ID and a NameID; a transient NameID is refused (`[samlsp.go:transientFormat]`). `Result.Expires` is the later of the `Conditions` and subject-confirmation `NotOnOrAfter` (`[samlsp.go:ParseResponse]`).
- The email comes from the email attribute, else an email-format or `@`-containing NameID; it must be valid (`[samlsp.go:ParseResponse]`).
- Entra's groups-link overage refuses the sign-in instead of signing in with no groups (`[samlsp.go:groupsLink]`).
- Attribute lookup uses only the configured name when set, else the common names in order; names and friendly names match case-insensitively and repeated attributes are merged (`[samlsp.go:attr]`).
- Metadata and decompressed bodies are bounded by `MaxMetadata`; encoded posted responses by `4*MaxMetadata` (`[samlsp.go:ParseMetadata]`, `[samlsp.go:serviceProvider]`, `[samlsp.go:ParseResponse]`).
- Metadata must describe exactly one IdP, offer an HTTP-Redirect SSO endpoint and at least one signing certificate (`[samlsp.go:ParseMetadata]`).
- Error messages never echo response XML; long library errors are cut to 160 characters (`[samlsp.go:describe]`).
- `ValidSSOURL` refuses plain `http` in production unless `AllowPlainHTTP` (`[samlsp.go:ValidSSOURL]`).

## Side Effects
- None beyond reading `PublicURL`, `Environment` and `AllowPlainHTTP` from `config.Get()` and using `crypto/rand`.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/samlsp/` and by the SSO API tests: `make verify-quick GO_PKGS="./internal/api/"`.
