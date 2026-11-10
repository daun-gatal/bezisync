# bezisync

![Version: 0.2.5](https://img.shields.io/badge/Version-0.2.5-informational?style=flat-square) ![AppVersion: 0.8.0](https://img.shields.io/badge/AppVersion-0.8.0-informational?style=flat-square)

BeziSync — self-service control plane for Debezium Server: the API, the controller and the web UI. Agents for remote
runtimes come with the [`bezisync-agent`](../bezisync-agent) chart.

## Install

```bash
helm install bezisync oci://ghcr.io/daun-gatal/charts/bezisync \
  --namespace bezisync --create-namespace \
  --set publicUrl=https://bezisync.example.com \
  --set secrets.masterKey="$(docker run --rm ghcr.io/daun-gatal/bezisync generate-master-key)" \
  --set secrets.databaseUrl=postgresql://bezisync:…@postgres:5432/bezisync
```

For a quick evaluation, `--set postgres.enabled=true` runs a single PostgreSQL pod instead of an
external database. Production needs an HA PostgreSQL (it also stores Debezium offsets), an https
`publicUrl` and `cookieSecure: true`; see the [guide](../../../docs/README.md#install).
`helm test bezisync` checks that the API is ready and that the web UI reaches it.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| agent.chart | string | `""` | The agent chart (default: `oci://ghcr.io/daun-gatal/charts/bezisync-agent`, at the version released with this chart). |
| agent.image | string | `""` | The agent image (default: this release's `bezisync` image, which runs agents too). |
| agent.url | string | `""` | The URL agents dial (default: `publicUrl`). |
| allowPlainHttp | bool | `false` | Production requires https; set true only when the network itself encrypts traffic (e.g. a Service exposed on a Tailscale tailnet). Then use `cookieSecure: false`. |
| api.replicas | int | `2` | API replicas. |
| api.resources | object | `{"limits":{"memory":"768Mi"},"requests":{"cpu":"100m","memory":"256Mi"}}` | API resources. |
| appArmorProfile | object | `{}` | AppArmor profile for every pod of this chart. Empty keeps the container runtime's default. Example: `{ type: Localhost, localhostProfile: bezisync-default }` for a profile loaded on every node (`deploy/apparmor/`). |
| controller.replicas | int | `2` | Leader election via Postgres advisory locks; extra replicas are hot standbys. |
| controller.resources | object | `{"limits":{"memory":"768Mi"},"requests":{"cpu":"100m","memory":"256Mi"}}` | Controller resources. |
| cookieSecure | bool | `true` | Secure session cookies (https). |
| environment | string | `"production"` | `production` refuses development keys, insecure cookies and a non-https public URL. |
| existingSecret | string | `""` | Existing Secret with keys `BEZISYNC_MASTER_KEY`, `BEZISYNC_DATABASE_URL` and optionally `BEZISYNC_STATE_DATABASE_URL` / `BEZISYNC_BOOTSTRAP_ADMIN_PASSWORD`. When empty the chart renders a Secret from `secrets` below (fine for evaluation, not for production GitOps). |
| extraEnv | list | `[]` | Extra environment for API and controller (e.g. `BEZISYNC_SSRF_ALLOW_CIDRS`). |
| image.pullPolicy | string | `"Always"` | `Always` re-pulls `latest`; `IfNotPresent` suits a pinned tag. |
| image.repository | string | `"ghcr.io/daun-gatal/bezisync"` | Image of the API, controller and migrations (one binary; agents run it too). |
| image.tag | string | `"latest"` | Pin a release (e.g. `0.1.0`) for reproducible installs; `""` uses the chart's appVersion. |
| imagePullSecrets | list | `[]` | For private registries, e.g. `[{ name: ghcr-pull-secret }]` (applied to every pod). |
| ingress.annotations | object | `{}` | Ingress annotations. |
| ingress.className | string | `""` | Ingress class. |
| ingress.enabled | bool | `false` | Expose the web Service through an Ingress. |
| ingress.host | string | `"bezisync.example.com"` | Ingress host. |
| ingress.tls | list | `[]` | Ingress TLS. |
| kubernetesRuntime.enabled | bool | `true` | Grant the controller what it needs to run connectors in this cluster. |
| kubernetesRuntime.namespaces | list | `[]` | Namespaces for connectors; defaults to the release namespace. |
| podSecurityContext | object | `{"fsGroup":10001,"runAsNonRoot":true,"runAsUser":10001,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod security context of the API, controller and test pods. |
| postgres.enabled | bool | `false` | Run a single PostgreSQL pod (evaluation and tests only). |
| postgres.image | string | `"postgres:17"` | PostgreSQL image. |
| postgres.password | string | `"bezisync"` | Password of the bundled database. Ignored with `existingSecret`: then the password comes from its key `BEZISYNC_POSTGRES_PASSWORD`. |
| postgres.storage | string | `"8Gi"` | Volume size. |
| postgres.storageClass | string | `""` | StorageClass; the default one when empty (e.g. longhorn). |
| publicUrl | string | `"https://bezisync.example.com"` | The URL people open (cookies, SSO redirects, agent install commands). |
| secrets.bootstrapAdminEmail | string | `"admin@bezisync.local"` | The first admin, created on first start. |
| secrets.bootstrapAdminPassword | string | `""` | The first admin's password. |
| secrets.databaseUrl | string | `""` | PostgreSQL URL; required unless `postgres.enabled`. |
| secrets.masterKey | string | `""` | Encrypts stored secrets: `bezisync generate-master-key`. |
| secrets.stateDatabaseUrl | string | `""` | Database for Debezium offsets and schema history; defaults to `databaseUrl`. |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Container security context of the API, controller and test pods. |
| service.annotations | object | `{}` | Service annotations, e.g. Tailscale operator: `{ tailscale.com/expose: "true", tailscale.com/hostname: bezisync }`. |
| service.labels | object | `{}` | Service labels. |
| service.loadBalancerClass | string | `""` | e.g. `tailscale` with `type: LoadBalancer`. |
| service.nodePort | string | `nil` | Node port when `type: NodePort`. |
| service.port | int | `80` | Service port. |
| service.type | string | `"ClusterIP"` | Service type of the web UI (which also proxies the API). |
| web.replicas | int | `2` | Web replicas. |
| web.resources | object | `{"limits":{"memory":"128Mi"},"requests":{"cpu":"20m","memory":"32Mi"}}` | Web resources. |
| webImage.pullPolicy | string | `"Always"` | Pull policy of the web image. |
| webImage.repository | string | `"ghcr.io/daun-gatal/bezisync-web"` | Image of the web UI (nginx). |
| webImage.tag | string | `"latest"` | Pin a release like `image.tag`; `""` uses the chart's appVersion. |
