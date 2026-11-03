# BeziSync

**The safe, self-service way to run [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) anywhere.**

BeziSync is a control plane for change data capture. Platform teams connect a runtime (Docker or
Kubernetes) and set guardrails once; any engineer can then stream changes from PostgreSQL, MySQL,
SQL Server or MongoDB to Kafka, Apache Iceberg (REST catalogs, AWS Glue, S3 Tables), PostgreSQL /
MySQL / MariaDB / SQL Server, Pub/Sub, Kinesis, SQS, SNS, Azure Event Hubs, Pulsar, Redis Streams, NATS or
HTTP — with checks that
catch problems *before* they reach production and an honest view of health afterwards. Sign in with
your identity provider, protect personal data per pipeline, and let AI agents work with it through
MCP, with exactly their person's permissions.

> Status: pre-1.0 and under active development. Releases: images `ghcr.io/daun-gatal/bezisync` and
> `bezisync-web`, Helm charts at `oci://ghcr.io/daun-gatal/charts`, and the CLI on the
> [Releases](https://github.com/daun-gatal/bezisync/releases) page.

## Try it in two minutes (Docker)

```bash
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

Open http://localhost:8080 and sign in as `admin@bezisync.local` / `bezisync-demo`. The
**Sandbox** project comes wired to a sample shop database (with live orders), Kafka and a local
Docker runtime: pick the "Shop → Kafka (demo)" template and deploy. `docker compose … down -v`
removes everything. Evaluation only — use the Helm chart for real installs.

## Quick start (Kubernetes)

```bash
helm install bezisync oci://ghcr.io/daun-gatal/charts/bezisync \
  --set postgres.enabled=true \
  --set secrets.masterKey=$(docker run --rm ghcr.io/daun-gatal/bezisync generate-master-key) \
  --set secrets.bootstrapAdminPassword='choose-a-strong-password' \
  --set environment=development --set cookieSecure=false --set publicUrl=http://localhost:8080
kubectl port-forward svc/bezisync 8080:80
```

Open http://localhost:8080 and sign in as `admin@bezisync.local`.

## Documentation

The [guide](docs/README.md) covers installation, pipelines, personal data, agents, policies and
approvals, secret stores, SSO, AI agents (MCP), config-as-code and the CLI, and administration. Reference: every
[preflight check](docs/reference/checks.md) and the [threat model](docs/security/threat-model.md).

## Contributing

Pull requests are welcome: target the `preview` branch (it collects changes until a maintainer
releases them). See [CLAUDE.md](CLAUDE.md) and [`.rules/`](.rules/) for how the code is laid out
and tested; every change passes `make verify` and `make e2e`, and CI runs both. Report security
problems privately, as described in [SECURITY.md](SECURITY.md).

## License

BeziSync is licensed under the [Apache License 2.0](LICENSE).
