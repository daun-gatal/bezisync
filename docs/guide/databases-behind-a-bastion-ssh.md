# Databases behind a bastion (SSH)

When a database is only reachable through an SSH bastion (a jump host), BeziSync can tunnel to it.
PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, CockroachDB and Vitess connections support this.

## Set up the tunnel

1. Open the connection and go to **Show expert settings → SSH tunnel**.
2. Set the bastion host, port and user.
3. Set the private key: OpenSSH or PEM, with no passphrase. It is stored encrypted and
   write-only. You can also use a secret-store reference.
4. Recommended: set the bastion's **host key**. Get it with:

   ```bash
   ssh-keyscan -t ed25519 <bastion>
   ```

5. Keep the connection's host and port set to the database's address *as seen from the bastion*.

The bastion must allow TCP forwarding (`AllowTcpForwarding yes`).

## How the tunnel works

- **Checks, discovery and probes** open a short-lived tunnel. The readiness checks report whether
  the bastion is reachable (`*_ssh`) and whether the tunnel works (`*_ssh_tunnel`). The tunnel
  check is a warning while the host key is not pinned.
- **Each connector** gets a `bezisync tunnel` sidecar that keeps the forward open and reconnects.
  On Kubernetes it is a native sidecar container; on Docker, a companion container sharing the
  connector's network. Debezium connects to `127.0.0.1:15432`.
- **Vitess** tunnels both VTGate ports: gRPC for the connector, MySQL for the checks.

## Limits

- TLS still works through the tunnel, but the server name is then `127.0.0.1`. Use `require` or
  `verify-ca`, not `verify-full`.
- MongoDB is not supported, because replica-set members announce their own host names. Use a
  network route or an agent runtime inside the network instead.
- Spanner is a Google API, not a host, so it has no tunnel.

---

[All documentation](../README.md)
