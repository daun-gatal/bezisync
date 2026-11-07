# 0004 — The SSRF guard holds on every dial, not just the probe

- **Status:** Accepted
- **Date:** 2026-11-07
- **Scope:** `backend/internal/connectors` (a new `dial.go`: `DialContext`, `GuardAWS`; every database, broker, HTTP, gRPC and AWS client; `ssh.go`; Event Hubs), `backend/internal/secrets` (Vault, AWS Secrets Manager, Google Secret Manager), `backend/internal/services/alerts.go` (`postJSON`), `backend/internal/ssrf` (`DialContext` timeout), the ssrf, connectors, secrets, services and api contracts, `ARCHITECTURE.md`, `.rules/backend.md`, the threat model, Known issue 1.

## Context

BeziSync opens sockets to addresses its users type: database and broker hosts, HTTP endpoints,
emulator hosts, custom cloud endpoints, Vault, webhooks. The SSRF guard (`internal/ssrf`) resolves
a host and refuses loopback, link-local and cloud-metadata addresses (private networks stay
reachable, since that is where databases live); `BEZISYNC_SSRF_ALLOW_CIDRS` widens it.

The guard was applied at the wrong moment (Known issue 1):

- **Checked, then dialled by someone else.** Alert webhooks and Vault checked the host, then sent
  through a plain `http.Client` that resolved DNS again (a rebinding answer passes the check and
  connects to metadata) and followed redirects. The HTTP sink check did the same.
- **Probed in Check only.** Every database and broker driver (pgx, MySQL, SQL Server, go-ora,
  MongoDB, franz-go, go-redis, nats, the StarRocks/Doris MySQL client, JDBC) dialled with its own
  dialer. Only `Check` probed first; `Discover`, `Prepare`, `Cleanup`, `Signal`, `WriteCanary`,
  `Probe` and `Tail` dialled straight away.
- **Never checked.** A Kinesis custom endpoint, a Pub/Sub `emulator_host`, a Spanner emulator after
  its probe, the SSH bastion after its probe.
- **Probing is never enough anyway.** Drivers dial addresses the *server* names: Kafka metadata
  lists brokers, MongoDB lists replica-set members, NATS advertises `connect_urls`, Redis sends
  cluster redirects. Only the dialer sees those.
- Vault's answer was read without a size limit.

Two constraints shape the fix:

- **SSH tunnels dial loopback on purpose.** `connectors.Tunnel` listens on `127.0.0.1:0` and hands
  the driver that address; `127.0.0.0/8` is denied by default. Guarding every dial naively would
  refuse every tunnelled source.
- **AWS credentials come from link-local addresses.** The default credential chain reads EC2 IMDS
  (169.254.169.254), ECS (169.254.170.2) and EKS Pod Identity (169.254.170.23). Passing a guarded
  client to `awsconfig.WithHTTPClient` (as the SQS, SNS and Iceberg checks did) sends the chain
  through the guard too, which refuses role-based credentials.

## Decision

1. **One dial function for connectors:** `connectors.DialContext` goes through `ssrf.DialContext`,
   which resolves, refuses if any answer is blocked, and dials one of the checked IPs (so DNS can't
   change in between). The only exception is the local end of a tunnel this process opened:
   `Tunnel` registers its listener address and removes it when closed. Every driver gets it:
   - pgx (`DialFunc`: PostgreSQL, CockroachDB, JDBC PostgreSQL), go-sql-driver/mysql
     (`Config.DialFunc`: MySQL, Vitess, StarRocks/Doris, JDBC MySQL), go-mssqldb
     (`Connector.Dialer`: SQL Server, JDBC SQL Server), go-ora (`NewConnector` + `Dialer`, instead
     of `sql.Open`), MongoDB (`SetDialer`), nats (`SetCustomDialer`, bounded by its own timeout);
   - franz-go and go-redis skip their own TLS settings when given a dialer, so `tlsDialer` dials
     through the guard and then speaks TLS itself with the same configuration;
   - Pub/Sub and Spanner emulator hosts: `grpc.WithContextDialer`. Google's own endpoints are fixed
     and public, so they keep the library's dialer.
2. **HTTP clients:** the HTTP sink check, alert webhooks (`postJSON`) and Vault use
   `ssrf.HTTPClient`, whose every dial, redirects and proxies included, goes through the guard.
   Vault and the HTTP sink don't follow redirects (Vault's token header would go with one). Vault
   and Google Secret Manager answers are read up to 4 MiB.
3. **AWS: guard the service calls, not the credential chain.** `LoadDefaultConfig` runs with the
   SDK's client, so the credential providers it builds (they copy the configuration at that
   moment) keep reaching instance and pod metadata. Then `connectors.GuardAWS` sets the guarded
   client on the configuration that service clients use: SQS, SNS, Kinesis, Iceberg (S3 and Glue),
   and AWS Secrets Manager. A custom endpoint is then guarded on every call, and AWS's own
   endpoints resolve to public addresses the guard allows.
4. **SSH:** the control plane dials the bastion through the guard. The forward's target is
   resolved by the bastion ("localhost on the bastion" is a normal setup) and can't be checked
   here. The pipeline's tunnel sidecar keeps a plain dialer (see Scope).
5. **Event Hubs is the one client with no dialer to hand it** (the SDK dials AMQP itself). Each
   client is made only after `eventHubsGuard` resolves and checks the endpoint of the connection
   string it uses (the main one and the Listen one). A DNS answer that changes between that check
   and the SDK's own lookup is not covered; the threat model records it.
6. `ssrf.DialContext` gives every dial a 30-second limit of its own.

**Scope.** The guard protects BeziSync's own processes (API and controller). The Debezium and
Flink CDC workloads, and the SSH sidecar beside them, reach the same hosts by design, outside it;
limiting them is a network policy question for the runtime. Addresses only operators set (the
state database, SMTP, telemetry, the runtime APIs) are out of scope.

### Implementation plan

One commit: `connectors/dial.go` and the driver, HTTP, gRPC, AWS, SSH and Event Hubs changes,
`secrets`, `services/alerts.go`, `ssrf`, their tests, contracts, rules, threat model, changelog,
this ADR, and the removal of Known issue 1.

### Verification plan

| ID | Case | Expected | Test |
|---|---|---|---|
| S1 | Every driver path (PostgreSQL, CockroachDB, MySQL, Vitess, SQL Server, Oracle, MongoDB, Kafka plain/TLS, Redis plain/TLS, NATS, JDBC ×3, StarRocks/Doris) pointed at a loopback listener | Refused with the guard's message; the listener accepts nothing | `connectors` `TestEveryDriverDialsThroughTheGuard` (fails on the old code: 28 connections) |
| S2 | Pub/Sub and Spanner emulators, Kinesis and SQS custom endpoints on loopback | No connection reaches the listener | same |
| S3 | Event Hubs endpoint on loopback | Refused before a client is made | same |
| S4 | A tunnel's own listener | Dialled; once the tunnel closes, refused again | `TestTunnelEndsAreTheOnlyLoopbackException` |
| S5 | Vault redirect; a 5 MiB Vault answer | Not followed (the other server sees nothing); refused as too large | `secrets` `TestVaultRefusesRedirectsAndHugeAnswers` |
| S6 | SSH-tunnelled sources and every connector against real fixtures | Unchanged | e2e (`20-ssh-tunnel`, `07-sources`, `08-sinks`, the full suite) |
| S7 | Existing guard tests | Unchanged | `ssrf`, `secrets`, `api` |

## Consequences

- A user can no longer reach loopback or metadata through any connector path, a server's redirect
  or advertised address, or a rebinding DNS answer, except the Event Hubs gap above.
- A pipeline whose connection resolves to an address the guard refuses now fails Discover, Prepare,
  Cleanup, Signal, the canary, Probe and the Events tab too, not only its check. That was always the
  rule; installs that relied on the gap set `BEZISYNC_SSRF_ALLOW_CIDRS`. The changelog says so.
- IAM-role credentials work again for SQS, SNS and Iceberg checks.
- New connectors must take `connectors.DialContext` (or `ssrf.HTTPClient`); the guard test lists
  each driver, and `.rules/backend.md` says so.

## Alternatives considered

- **Guard every dial including tunnels, and allow loopback for tunnelled connections.** That would
  open loopback to the whole connection; the registry allows only BeziSync's own listener.
- **Dial through the SSH client instead of a local listener.** Not every driver can take a dialer
  per connection the same way, and the listener is already shared by the drivers.
- **Refuse emulator hosts unless allow-listed.** They can be guarded through gRPC; refusing them
  would break existing installs and the e2e fixtures.
- **`awsconfig.WithHTTPClient(ssrf.HTTPClient)`.** Sends the credential chain through the guard and
  refuses role-based credentials.
- **A lint or grep for raw dials.** Blind to a driver left on its default dialer (the main gap) and
  noisy on legitimate clients; the behaviour test covers what matters.
- **Force Event Hubs onto WebSockets** (the SDK accepts a WebSocket dialer). It changes the port
  from 5671 to 443 for every install, and the Event Hubs emulator may not serve WebSockets.
