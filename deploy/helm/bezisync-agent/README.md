# bezisync-agent

![Version: 0.2.0](https://img.shields.io/badge/Version-0.2.0-informational?style=flat-square) ![AppVersion: 0.2.0](https://img.shields.io/badge/AppVersion-0.2.0-informational?style=flat-square)

BeziSync agent — runs Debezium Server connectors for a remote runtime; dials out only.

## Install

Add a remote runtime in BeziSync (**Runtimes → Add runtime**, agent mode): it shows the install
command with its one-time enrollment token, for example:

```bash
helm install bezisync-agent oci://ghcr.io/daun-gatal/charts/bezisync-agent \
  --namespace cdc --create-namespace \
  --set controlPlane.url=https://bezisync.example.com --set token=bza_…
```

The agent only dials out to the control plane. `helm test bezisync-agent` checks that it can
reach it. See [Remote runtimes (agents)](../../../docs/README.md#remote-runtimes-agents).

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| controlPlane.insecure | bool | `false` | Skip TLS verification: testing only. |
| controlPlane.url | string | `""` | The control plane's URL, e.g. `https://bezisync.example.com` (required). |
| docker.host | string | `""` | Docker daemon, e.g. `tcp://docker-host:2375` (required with `kind: docker`). |
| docker.imagePullTimeout | string | `"30m"` | How long one image download may take: `10m`, `30m`, `1h` or `2h`. |
| docker.network | string | `""` | Docker network for connectors. |
| existingSecret | string | `""` | Existing Secret with the token (key `BEZISYNC_AGENT_TOKEN`). |
| image.pullPolicy | string | `"Always"` | `Always` re-pulls `latest`; `IfNotPresent` suits a pinned tag. |
| image.repository | string | `"ghcr.io/daun-gatal/bezisync"` | The agent image (the `bezisync` image runs agents too). |
| image.tag | string | `"latest"` | Pin a release (e.g. `0.1.0`) for reproducible installs; `""` uses the chart's appVersion. |
| imagePullSecrets | list | `[]` | For private registries, e.g. `[{ name: ghcr-pull-secret }]`. |
| kind | string | `"kubernetes"` | `kubernetes`: connectors run as Deployments in the release namespace. `docker`: connectors run on the Docker daemon at `docker.host` (e.g. a DinD sidecar or VM). |
| podSecurityContext | object | `{"fsGroup":10001,"runAsNonRoot":true,"runAsUser":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod security context of the agent and test pods. |
| reportNodeCapacity | bool | `true` | Lets the agent report node capacity (cluster-scoped, read-only list of nodes). |
| resources | object | `{"limits":{"memory":"384Mi"},"requests":{"cpu":"50m","memory":"128Mi"}}` | Agent resources. |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Container security context of the agent and test pods. |
| token | string | `""` | The enrollment token shown once when the runtime was added (`bza_...`). Either set it here or reference an existing Secret holding it under key `BEZISYNC_AGENT_TOKEN`. |
