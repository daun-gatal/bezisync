# Google GKE and Amazon EKS

BeziSync runs pipelines on managed GKE and EKS clusters in two ways: through an agent inside the
cluster, or in direct mode with a cloud identity.

| Way | How BeziSync reaches the cluster | Use it when |
|---|---|---|
| [Agent](#use-an-agent) | The agent dials out from inside the cluster | The cluster is private, or you want no cloud sign-in in BeziSync |
| [Direct mode](#direct-mode) | BeziSync talks to the cluster API itself | The cluster API is reachable from BeziSync |

## Use an agent

Install the agent chart into the cluster with the runtime's install command (see
[Remote runtimes (agents)](remote-runtimes-agents.md)). The agent uses its own in-cluster
ServiceAccount and only dials out. So it works with private GKE and EKS clusters, and needs no
cloud IAM sign-in or kubeconfig in BeziSync.

## Direct mode

In direct mode BeziSync talks to the cluster API itself, which must be reachable.

1. Untick **Use BeziSync's in-cluster service account**.
2. Choose how to sign in:

| Sign-in | What to enter | Notes |
|---|---|---|
| **GKE** | API server endpoint and cluster CA certificate (both in the cluster's details), and a Google service-account key with *Kubernetes Engine Developer*. Nothing when BeziSync itself runs with Workload Identity | BeziSync mints and refreshes short-lived access tokens |
| **EKS** | Endpoint, CA certificate, cluster name and region, and AWS keys. Nothing when BeziSync runs with IRSA / an instance role | BeziSync creates the same `k8s-aws-v1.` tokens as `aws eks get-token` (15 minutes, refreshed). Map that IAM identity to a Kubernetes user or group with an access entry (or `aws-auth`) bound to the Role below |
| **kubeconfig** | Any kubeconfig with a static ServiceAccount token | Exec plugins such as `gke-gcloud-auth-plugin` are not available in the image |

### Permissions the identity needs

The identity needs the Role from the agent chart in the connector namespace. It covers:

- Deployments, ConfigMaps and Secrets;
- Services (Flink CDC pipelines only);
- pods, logs and `pods/proxy`;
- reading ResourceQuotas, so preflight can tell a deploy the namespace's quota would refuse. Both
  charts grant it from 0.2.1.

## Around the cluster

| Area | How it works on GKE and EKS |
|---|---|
| Databases | Choose the AWS RDS/Aurora or GCP Cloud SQL/AlloyDB preset on the source. Checks and fixes then use the provider's settings (`rds.logical_replication`, `cloudsql.logical_decoding`, …) |
| Secrets | AWS Secrets Manager and Google Secret Manager stores work with keys, or with the control plane's pod identity (IRSA / Workload Identity) when the credentials are left empty |
| Destinations | Kinesis, SQS, SNS, Iceberg (S3, Glue, S3 Tables) and Pub/Sub use keys, or the connector pod's own identity ([below](#use-the-connector-pods-identity)) |
| Images | Connector pods pull `quay.io/debezium/server`. For clusters without that egress, mirror it to Artifact Registry / ECR and choose it as the runtime's default image (or under **Settings → Engine versions**) |

### Use the connector pod's identity

For destinations, set the runtime's *connector service account* to a ServiceAccount bound to IRSA
(`eks.amazonaws.com/role-arn`) or Workload Identity (`iam.gke.io/gcp-service-account`), and leave
the keys empty.

## Where checks run

Checks run where connectors run. For pipelines on an agent runtime, the final checks before a
deploy run on the agent, inside the cluster's VPC. So a database reachable only from there passes,
and the sections say "checked by the agent".

If the agent cannot answer, BeziSync checks from its own network and says so. Checks on the
Connections page always run from BeziSync.

---

[All documentation](../README.md)
