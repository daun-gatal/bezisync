# Remote runtimes (agents)

An agent runs pipelines in a cluster or on a Docker host that BeziSync cannot reach, by dialling
out to BeziSync.

The agent runs next to the runtime and only makes outbound HTTPS requests to BeziSync. You need no
inbound firewall rules, and BeziSync stores no runtime credentials.

## Add an agent runtime

1. In [**Settings → Runtimes**](manage-runtimes.md), add a runtime with **Through an agent**.
2. BeziSync shows a one-time install command. Install the agent with it, using either:
   - the Helm chart `deploy/helm/bezisync-agent`, or
   - `docker run` of the `bezisync` image.
3. Confirm **I've copied it** to close the command. The token is never shown again.

## Rotate the agent token

**New agent token** (in **More actions**) rotates the token. The old one stops working
immediately.

## When the agent disconnects

If the agent disconnects for 30 seconds, its pipelines show **Unknown** (never Healthy), with the
reason.

---

[All documentation](../README.md)
