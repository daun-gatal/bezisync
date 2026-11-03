# BeziSync threat model

Scope: a self-hosted BeziSync 1.0 install (API, controller, web, agents) and the Debezium
Server workloads it starts. This document lists what we protect, where trust changes, the
threats we considered (STRIDE), the controls that exist in the code today, and the risks
operators must handle themselves. Report vulnerabilities privately (see `SECURITY.md`).

## Assets

| Asset | Where it lives | Why it matters |
|---|---|---|
| Database credentials of sources and destinations | `connections.secrets_enc` (AES-256-GCM), or an external secret store by reference | Read access to production data; replication rights |
| Change events (row data, possibly personal data) | Flows source → Debezium Server → destination; previewed on demand | Confidentiality of business and personal data |
| Master key | `BEZISYNC_MASTER_KEY` (env / Kubernetes Secret) | Decrypts every stored secret |
| Sessions, API tokens, agent tokens | Hashed (keyed) in the database; cookies / bearer headers in transit | Account and runtime takeover |
| Runtime access (Kubernetes ServiceAccount, Docker socket, agents) | Chart RBAC, controller host, agent hosts | Code execution where connectors run |
| Source health (WAL / binlog / oplog retention) | Replication slots and readers on the source | A stuck pipeline can fill a production disk |
| Audit log | `events` table | Accountability, compliance |

## Trust boundaries

1. **Browser ↔ API** — session cookie (HttpOnly, SameSite=Lax, Secure in production) plus a
   CSRF token header on every state-changing request; strict response headers.
2. **CLI / CI ↔ API** — bearer API tokens (`bzt_…`), hashed, scoped to a project, optional
   read-only, expiring; minted only from an interactive session.
3. **API / controller ↔ sources and destinations** — outbound connections for checks,
   discovery, previews and preparation, all through the SSRF guard.
4. **Controller ↔ runtimes** — Kubernetes API (namespaced ServiceAccount) or Docker API;
   for remote runtimes an **agent dials out** with a `bza_…` token, so no inbound access is
   needed to the runtime.
5. **API ↔ identity provider** — OpenID Connect (authorization code + PKCE).
6. **Connector ↔ state database** — Debezium JDBC offset / schema-history store.

## Threats and controls

| STRIDE | Threat | Controls in BeziSync |
|---|---|---|
| Spoofing | Stolen session cookie or CSRF | HttpOnly + SameSite cookies, CSRF header check, session TTL, `Secure` enforced outside development |
| Spoofing | Brute-force sign-in | Rate limiter on login; argon2id password hashes |
| Spoofing | Forged SSO response / token replay | OpenID Connect and OAuth2: PKCE, state (and nonce) in an encrypted path-scoped 10-minute cookie; ID tokens verified against JWKS (`iss`/`aud`/`exp`/nonce); OAuth2 users read only from the configured userinfo URL with the access token (GitHub: primary verified address from `/user/emails`; paging never leaves the configured API host); a verified email is required; same-site return paths only |
| Spoofing | Forged, altered or replayed SAML assertions; XML signature wrapping | Signatures verified by goxmldsig against the configured certificates (the response or each assertion); exactly one assertion; issuer, destination and recipient must match; an audience restriction naming BeziSync is required; validity window enforced (2-minute skew); `InResponseTo` must match a single-use request stored server-side and tied to the browser by a relay cookie (SameSite=None over https); each assertion ID accepted once until it expires; transient NameIDs refused; unsolicited (IdP-initiated) responses refused unless an Admin allows them, and then still single-use; expired certificates refused at save; metadata parsed without external entities, size-capped, fetched through the SSRF guard |
| Spoofing | Account takeover with an email the provider never verified ("nOAuth") | Only an address the provider vouches for counts (OIDC "email verified" claim true, configurable for Entra's `xms_edov`; OAuth2 a named field or GitHub's verified primary; signed SAML); with "accept unverified email" an unverified address only creates a new account, never signs in to an existing one |
| Spoofing | Authorization-server mix-up between providers | Each transaction is bound to its provider in the encrypted state cookie; the callback's RFC 9207 `iss` must match the provider's issuer (required when advertised; GitHub's `https://github.com/login/oauth` preset) |
| Elevation of privilege | Silent role changes from incomplete group lists | Entra's group overage (distributed-claim link, `groups.link` attribute) and GitHub's `X-GitHub-SSO: partial-results` refuse the sign-in instead of syncing a partial list |
| Elevation of privilege | One identity provider taking over accounts bound to another | A person linked to one provider's identity is refused by any other with the same email, unless an Admin turned on "take over" for that provider (audited per person) |
| Spoofing | A malicious or mistyped identity provider (Settings → Sign-in) | Only Admins add providers; every change is audited; the client secret is encrypted, write-only and never logged; discovery, JWKS and token calls go through the SSRF guard; an `http` issuer is refused in production unless plain HTTP was opted into; subjects are bound per issuer (`issuer|sub`), and an email already linked to another identity is refused |
| Denial of service | Locking everyone out by turning off password sign-in | Refused unless a provider passed its test and an active Admin has signed in with SSO; the last enabled provider cannot be turned off or deleted while password sign-in is off; password sign-in stays in force while no provider is enabled; `bezisync enable-password-login` restores it from the server |
| Elevation of privilege | An AI agent (MCP) doing more than its person may | Each MCP request lists only the tools the caller's live roles, token scope and read-only flag allow, and every tool calls the REST API in process as the caller (same permissions, policies, approvals, audit). Destructive tools are off until an Admin turns them on. Agents can't change MCP settings, mint tokens or approve OAuth requests (session-only) |
| Information disclosure | Credentials or personal data reaching an AI model | No MCP tool takes or returns a credential (tokens, runtime/secret-store/channel credentials, sign-in providers, passwords); connection secrets accept secret-store references only; event previews over MCP are always masked; results are capped |
| Spoofing | Stolen or replayed MCP OAuth tokens | Public clients with PKCE S256 only; registered redirect URIs matched exactly (https, loopback http, or reversed-domain app schemes; Admin host allow-list); one-time 60-second codes; `iss` in responses; 1-hour access tokens accepted only on the MCP endpoint; rotating refresh tokens where reuse revokes the grant; grants capped by an Admin, listed and revocable by their owner and Admins; tokens stored hashed; optional Admin approval of new agents; registration and token endpoints rate-limited |
| Spoofing | DNS rebinding / cross-site calls to MCP | Requests carrying an `Origin` must be BeziSync's own or on the Admin's allow-list; bearer tokens only (no cookies) |
| Spoofing | Rogue agent | Per-runtime random token, stored hashed, rotation invalidates the old one immediately; agents only receive their own runtime's workloads |
| Tampering | Changing a pipeline behind someone's back | Optimistic concurrency (`If-Match`), immutable versions, full audit trail; optional four-eyes approval per project |
| Tampering | Overriding managed Debezium properties (offset store, slot, credentials, the transform chain) | `advanced` properties are rejected when they match managed patterns; `debezium.transforms*` and `debezium.predicates*` are managed, so BeziSync's own filter (heartbeat and signal records) and routing always stay in the chain and every step goes through the spec |
| Repudiation | "Who deployed this?" | Every mutation is an audit event (actor, summary, details, request ID); forced deploys record the skipped checks and the reason; export to CSV / JSON Lines |
| Information disclosure | Secrets in API responses, logs or check messages | Write-only secret fields (only `secret_fields_set` / `secret_refs` returned), secret values scrubbed from check messages, secrets rendered as `${ENV}` in configs, a canary-secret test asserts no leak |
| Information disclosure | Personal data reaching destinations | Per-pipeline choices: columns that look like personal data are masked (salted, per-pipeline hash) or removed as soon as their table is picked; "Keep as is" is an explicit, versioned choice. Preflight blocks a mask Debezium can't apply (non-text columns, MongoDB, Spanner) so a "masked" column never leaks; Spanner removes columns by leaving them out of BeziSync's change stream and refuses removal it can't enforce (a user's own stream), warns about undecided personal-data columns, and new ones show in schema changes; event previews need `pipeline.preview_data` and mask PII-looking keys |
| Information disclosure | One connection borrowing another's credentials (a Spanner source's coordination Kafka) | A linked field may only name a connection of the same project and the declared kind and type (stored by id, checked on every save); resolving it needs no extra permission, the same as a pipeline using its destination, and its secrets are scrubbed from check messages too. A linked connection cannot be deleted |
| Information disclosure | Credentials carried from one project into another by "Copy to project" (connections, secret stores) | A copy needs `connection.manage` in both projects (an API token scoped to one project can't reach the other) and leaves saved secrets behind by default: the target's people enter them, and until they do the connection reports `missing_secrets`, its checks show the blocking `secret_missing`, and preflight refuses to deploy a pipeline using it (422 `secret_missing`). Copying saved secrets or a secret store is for Admins only, happens on the server (decrypted and re-encrypted, never returned) and is recorded in the audit logs of both projects with who copied what; references keep naming a store of the target project (never the source's), and the target's policies (allowed types) apply |
| Tampering | Saving secret-store settings that break every connection reading from it | Adding and editing a store from the UI test the settings first (`test_first`) and save nothing when the test fails, unless the Admin chooses "Save anyway" |
| Information disclosure | Secret values reaching the browser through the secret-store picker | Listing returns secret paths and key names only (`…/secrets`, `…/keys`); keys are read server-side and the values dropped there; both need `connection.manage`, like checking a reference. A pasted kubeconfig may only use a static token: `exec` plugins and auth providers are refused, so BeziSync never runs a program a kubeconfig names, and its API server goes through the SSRF guard on every dial |
| Information disclosure | Probing other projects | Project-scoped authorization; unknown and forbidden look identical (404) |
| Information disclosure | SSRF via connection hosts (cloud metadata, loopback) | Every outbound check/store call resolves and refuses blocked addresses (all DNS answers checked); admins opt in with `BEZISYNC_SSRF_ALLOW_CIDRS` |
| Information disclosure | Telemetry leaking details | Off by default; needs both a flag and a collector URL; aggregate counts only; admins can preview the exact payload |
| Denial of service | Source disk filled by retained WAL | Source-impact probes, "At risk" health, alerts on retained log, retention-limit checks with fixes, `uninstall` removes slots |
| Denial of service | Expensive checks hammered | Rate limit on connection checks |
| Denial of service | Feedback spam (an open lab lets anyone sign in) | 10 reports per person per hour, 5,000 characters each, signed-in sessions only (no API tokens) |
| Tampering | Script in feedback messages (stored XSS against Admins) | Messages are stored as sent and rendered only as plain text; the page path keeps app paths only; the message never enters the audit log |
| Elevation of privilege | Editor deploys something unsafe | Policies (allowed runtimes / types / snapshot modes / sizes, "Deploy anyway" off), approvals; policy failures can never be forced |
| Elevation of privilege | A script in a transform runs inside the connector, next to the source credentials and the state-store password in its environment | Raw filter scripts (`condition`) need the project policy `allow_scripted_transforms` (off by default, Admin-only, never forceable); built-in steps are compiled by BeziSync from typed rules with escaped literals |
| Elevation of privilege | Token minting with a leaked token | Tokens cannot create tokens; read-only tokens are limited to `*.view` permissions |
| Elevation of privilege | SSO group mapping drift | Memberships granted by SSO are re-evaluated at every sign-in; manual grants (including an admin's edit of an SSO-granted membership, which takes it off sync) are never changed by SSO |
| Elevation of privilege | Abuse of administration (people, Admin role, projects, runtimes, policies) | Admin is account-wide (`is_superuser`) and the only role that manages anything: people, projects, the Debezium catalog and every project's members, runtimes, policies and audit log. Project roles (viewer, operator, editor) never include `project.manage`, `member.manage`, `runtime.manage` or `audit.view`. Nobody can demote or deactivate themselves; the last active Admin is protected; every change is audited (instance-level events have no project). Upgrading demotes former project admins to editor + approver rather than widening their access, and a legacy SSO rule with `"role": "admin"` grants the same |
| Spoofing | A deactivated account keeps working | Deactivation deletes all its sessions at once; API tokens and SSO sign-in check `is_active` on every request |

## Residual risks and operator responsibilities

- **Docker socket = root.** The Docker runtime in direct mode (and the demo stack) gives
  BeziSync root-equivalent access to that host. Prefer the agent, rootless Docker, or
  Kubernetes in production.
- **Master key custody.** Anyone with the master key and a database dump can decrypt stored
  secrets. Keep it in a secret manager, rotate it with `BEZISYNC_PREVIOUS_MASTER_KEYS`, or
  keep credentials in an external secret store so BeziSync stores only references.
- **Secret values in memory and in workloads.** Referenced secrets are read when needed and
  injected into connector env / Kubernetes Secrets; agent-mode workload specs are stored
  encrypted. Protect the runtime namespace like the database it reads.
- **The state database is on the data path.** Offsets and schema history live there; run it
  highly available and back it up.
- **Connector images.** Custom Debezium images run with the source credentials; only use
  images you build and scan. Apache Iceberg pipelines run a community build
  (`ghcr.io/memiiso/debezium-server-iceberg`) that BeziSync pins by digest, so a re-pushed tag never
  changes what runs; mirror and scan it (`BEZISYNC_ICEBERG_IMAGE`) where policy requires.
- **SNS preview queues.** For the Events tab BeziSync creates an SQS queue per SNS topic
  (`bz-preview-<topic>`, 15-minute retention, a policy that accepts only that topic) with the
  connection's credentials: change events also rest there for 15 minutes. Deleting the pipeline
  removes them.
- **Checks write.** The Database (JDBC) connection check creates and drops an empty test table. The Iceberg connection check creates a one-row table in the namespace,
  reads it and drops it, with the connection's credentials; catalogs that do not purge on drop
  (RustFS) keep its few bytes of files. BeziSync's catalog, S3 and AWS calls go through the SSRF
  guard; the connector's own (Java) traffic does not.
- **Network exposure.** Terminate TLS in front of the web service, keep the API private to
  it, and restrict who can reach `/metrics` (it requires `BEZISYNC_METRICS_TOKEN`).
- **Supply chain.** The backend, controller and agent are one static Go binary on a distroless
  base image (no shell, no package manager, uid 10001), published as `bezisync` (agents run it
  too). Dependencies are pinned in `go.sum`; CI runs `govulncheck` and Grype,
  and release images are signed (cosign) with an SBOM attestation.
- **AppArmor.** Pods keep the container runtime's default AppArmor profile unless
  `appArmorProfile` names another. The optional `bezisync-default` profile (`deploy/apparmor/`) is containerd's
  default plus signals between a container's own processes; its privileged DaemonSet in
  `kube-system` loads it at every boot and it runs the host's `apparmor_parser` through a mount of
  the host's root, so treat its manifest and image like node configuration.
- **Unencrypted source connections.** Vitess's VStream gRPC (Debezium offers no TLS for it) and
  Oracle's listener traffic (BeziSync speaks plain TCP; enable Oracle Native Network Encryption on
  the server) are not TLS: keep them on a private network or behind the SSH bastion option.
- **Cloud credentials in connector workloads.** A Spanner service-account key is mounted as a file
  under `/debezium/secrets` (part of the workload's secret, counted in `config_hash`); prefer
  Workload Identity, which needs no key. The coordination Kafka's credentials reach the Spanner
  connector the same way a Kafka destination's do.
- **SSRF enforcement point.** The guard applies to the probes BeziSync itself makes (checks,
  discovery, secret-store and alert-channel calls); connector traffic from Debezium and SSH
  tunnels to loopback inside the connector pod are not filtered by it.

## Verification

Automated: authorization (404 concealment, permissions), CSRF, secret non-disclosure with a
canary value, SSRF guard, SSO rejections (forged state, replayed nonce, wrong audience,
unverified email, open redirects), token scoping, policy enforcement, uninstall cleanup — see
the API tests in `backend/internal/api` and the e2e suite. Manual review before each release: dependency scan
(Grype in CI), chart RBAC diff, and this document.
