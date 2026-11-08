<div align="center">

# BeziSync

**Self-service change data capture on [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) — safe enough to hand to every engineer.**

[![CI](https://github.com/daun-gatal/bezisync/actions/workflows/ci.yml/badge.svg?branch=preview)](https://github.com/daun-gatal/bezisync/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/daun-gatal/bezisync?sort=semver&filter=v*)](https://github.com/daun-gatal/bezisync/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

[Quick start](#quick-start) · [Guide](docs/README.md) · [Preflight checks](docs/reference/checks.md) · [Threat model](docs/security/threat-model.md)

</div>

Debezium is powerful, and easy to get wrong: a forgotten replication slot fills a disk, a snapshot
locks a table, a dashboard stays green while nothing flows. BeziSync puts a control plane in front
of it. Platform teams connect runtimes and set guardrails once; anyone can then stream a database
to Kafka, Iceberg or a dozen other destinations — with the mistakes caught before they deploy.

## Why BeziSync

| | |
|---|---|
| 🛫 **Preflight checks with exact fixes** | Every source and destination is checked before anything runs, and each failure comes with the statement or setting that fixes it, per cloud provider. |
| 🛡️ **Source protection** | WAL, binlog and oplog retention in view, heartbeats and snapshot windows: your production database comes first. |
| 🔍 **Honest health** | Never green without fresh evidence. No data means *Unknown*, not *Healthy*. |
| 🖱️ **One-click operations** | Snapshots, offsets, pause, resume and restart; every change is a version you can diff and roll back. |
| 🔐 **Secrets done right** | Encrypted and write-only, or referenced from Vault, AWS, Google or Kubernetes secret stores. |
| 🧑‍⚖️ **Guardrails** | RBAC, SSO, approvals, project policies and a full audit trail. |
| 🙈 **Personal data, chosen per pipeline** | Mask, remove or keep each column — never a silent leak. |
| 🤖 **AI agents over MCP** | Agents get exactly their person's permissions, and never a credential. |

**Engines:** each pipeline runs on **Debezium Server** or **[Flink CDC](docs/README.md#flink-cdc-engine)**, chosen per pipeline ([which to choose](docs/README.md#choose-an-engine)).<br>
**Sources:** PostgreSQL, MySQL, MariaDB, SQL Server, Oracle (both engines); MongoDB, CockroachDB, Vitess, Google Cloud Spanner (Debezium).<br>
**Destinations:** Kafka and Apache Iceberg (both engines); PostgreSQL / MySQL / MariaDB / SQL Server, Pub/Sub, Kinesis, SQS, SNS, Azure Event Hubs, Pulsar, Redis Streams, NATS, HTTP (Debezium); StarRocks, Apache Doris, Apache Fluss, Apache Paimon, OceanBase, Elasticsearch (Flink CDC).<br>
**Runtimes:** Docker or Kubernetes, in your cluster or remote through an outbound-only agent.

## Quick start

**Try it in two minutes** with Docker: a sample shop database with live orders, Kafka and a
ready-made pipeline.

```bash
git clone https://github.com/daun-gatal/bezisync && cd bezisync
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

Open http://localhost:8080, sign in as `admin@bezisync.local` / `bezisync-demo`, pick the
**Shop → Kafka (demo)** template in the Sandbox project and deploy.

**Install on Kubernetes:**

```bash
helm install bezisync oci://ghcr.io/daun-gatal/charts/bezisync \
  --namespace bezisync --create-namespace \
  --set postgres.enabled=true \
  --set secrets.masterKey="$(docker run --rm ghcr.io/daun-gatal/bezisync generate-master-key)" \
  --set secrets.bootstrapAdminPassword='choose-a-strong-password' \
  --set environment=development --set cookieSecure=false --set publicUrl=http://localhost:8080
kubectl -n bezisync port-forward svc/bezisync 8080:80
```

For production (external PostgreSQL, HTTPS, SSO), follow the [guide](docs/README.md#install).

## How it works

```
Browser · CLI · AI agents (MCP)
          │
          ▼
   BeziSync API ──── PostgreSQL (metadata, jobs, audit, Debezium offsets)
          │
   Controller ─────▶ Docker / Kubernetes ─────▶ Debezium Server (one per pipeline)
                     └─ remote clusters: outbound-only agent
```

One Go binary (API, controller, agent and CLI) and a React UI. PostgreSQL is the only dependency;
it is also the job queue.

## Contributing

Contributions are welcome! Open pull requests against the **`preview`** branch. Run `make verify`
(CI runs it too) and, if you can, the end-to-end specs your change touches (`make e2e-quick SPECS=…`);
the full end-to-end suite needs a large local cluster, so a maintainer runs it before merging. [AGENTS.md](AGENTS.md) and [`.rules/`](.rules/) explain the
layout, the conventions and how to test; [ARCHITECTURE.md](ARCHITECTURE.md) maps the code, and every code
directory has a `README.md` contract to read before you change it. Big designs are recorded as
[ADRs](docs/adr/README.md). Working with an AI agent? [`.rules/egm.md`](.rules/egm.md) is the path from
an idea to a validated PR (the [EGM skills](https://github.com/google/elephant-goldfish-model) are optional). Please report security issues privately, as described in
[SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE)
