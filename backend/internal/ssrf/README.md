# ssrf

## Overview
Package `ssrf` is the guard for the connections BeziSync opens on a user's behalf (connection checks,
webhooks, secret stores, HTTP sinks, connector previews). Private networks stay reachable
because that is where databases live, but loopback, link-local, multicast and cloud metadata
addresses are refused unless an administrator allows them. A few callers only check the host with
`ResolveChecked` and then send through a plain `http.Client`, which resolves again (and may
follow redirects) — known issue, see [ARCHITECTURE.md › Known issues](../../../ARCHITECTURE.md#known-issues).

## Interface
- `[ssrf.go:DefaultDeny]` — the always-denied networks (loopback, `0.0.0.0/8`, link-local incl. `169.254.169.254`, Alibaba and AWS IPv6 metadata, multicast).
- `[ssrf.go:IsAllowed]` — whether one IP may be contacted.
- `[ssrf.go:ResolveChecked]` — resolves a host (an IP literal is used as is) and returns its sorted, de-duplicated addresses, or `[ssrf.go:BlockedAddress]` naming the blocked ones (a lookup failure is a plain error).
- `[ssrf.go:TCPProbe]` — guarded TCP connect-and-close returning latency in ms.
- `[ssrf.go:DialContext]` — a guarded dialer; `[ssrf.go:HTTPClient]` — an `http.Client` whose every dial goes through it.
- `[ssrf.go:Resolver]` — the DNS resolver (overridable in tests).

## Invariants
- `BEZISYNC_SSRF_ALLOW_CIDRS` is checked first and wins over the deny list; otherwise `DefaultDeny` plus `BEZISYNC_SSRF_EXTRA_DENY_CIDRS` apply (`[ssrf.go:IsAllowed]`).
- IPv4-mapped IPv6 addresses are unmapped before matching, and IPv4 prefixes only match IPv4 addresses (`[ssrf.go:IsAllowed]`, `[ssrf.go:contains]`).
- A DNS answer is refused if *any* address is blocked, not just the first (`[ssrf.go:ResolveChecked]`).
- Dials go to the already-checked IP, not the hostname, so a second DNS lookup cannot rebind (`[ssrf.go:DialContext]`, `[ssrf.go:TCPProbe]`).
- `HTTPClient` disables proxies (`Proxy = nil`) so every request is dialled directly through the guard, redirects included (`[ssrf.go:HTTPClient]`).
- Unparseable CIDR entries in config are skipped silently; an unparseable IP is never allowed (`[ssrf.go:prefixes]`, `[ssrf.go:IsAllowed]`).

## Side Effects
- DNS lookups and outbound TCP connections to user-supplied hosts.
- Reads `SSRFAllowCIDRs` / `SSRFExtraDenyCIDRs` from `config.Get()` on every check.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/ssrf/"`
