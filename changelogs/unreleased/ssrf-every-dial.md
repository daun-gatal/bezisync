type: minor

### Changed
- **The SSRF guard holds on every connection, not just the check** - BeziSync used to check a host when testing a connection and then let the database or broker driver dial on its own, so discovery, preparing, cleanup, signals, canaries, probes and the Events tab could reach loopback or cloud-metadata addresses, and a DNS answer that changed after the check, a redirect or an address a server advertises (Kafka brokers, MongoDB members, NATS servers) was never checked. Every driver, HTTP client, emulator host and custom cloud endpoint now dials through the guard; SSH-tunnelled sources keep working. A connection whose host resolves to a refused address now fails on every path, not only its check: allow it with `BEZISYNC_SSRF_ALLOW_CIDRS` if it is meant to be reached (an emulator on `localhost`, for example). An egress proxy set with `HTTPS_PROXY`/`HTTP_PROXY` is still used, after the target is checked, and now also for single sign-on, Iceberg and Pulsar admin calls, which ignored it before.

### Fixed
- **IAM-role credentials for SQS, SNS and Iceberg checks** - their checks sent the AWS credential chain through the SSRF guard, which refuses instance and pod metadata addresses; only service calls are guarded now.
- **Vault answers are bounded and redirects aren't followed** - an answer is read up to 4 MiB, and a redirect no longer carries the Vault token to another address.
