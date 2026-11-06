# version

## Overview
Holds the BeziSync release version compiled into the binary. It is shown by `bezisync --version`
and `GET /api/v1/system/info`, logged at start-up, used for the default agent image tag
(`internal/config`), sent in the agent's hello, the MCP server's implementation info, telemetry
reports and outbound `User-Agent` headers (SSO, OIDC, MCP). The release tool writes the same
version into `frontend/openapi.json`.

## Interface
- `[version.go:Version]` — the release string (for example `"0.3.0"`).

## Invariants
- Never bump it by hand: `scripts/release.sh` sets it together with the frontend and chart versions when it assembles a release (`.rules/releases.md`).

## Side Effects
- None.

## Verification
- No co-located tests; covered by `cd backend && go vet ./internal/version/` plus `make verify-quick GO_PKGS="./internal/api/"`.
