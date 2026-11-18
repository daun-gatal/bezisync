# Run your own lab in GitHub Codespaces

A lab is a private BeziSync with sample sources, destinations and teammates, so you can try
pipelines without installing anything. The README's **Open in GitHub Codespaces** badge starts one
in *your* codespace. Nobody else can open it, and nothing is hosted for you.

## What the lab contains

The lab is the [demo stack](install.md#try-it-on-docker) plus:

| Extra | What it is |
|---|---|
| `demo-mysql` | A MySQL shop |
| `legacy-postgres` | A PostgreSQL that isn't ready for change data capture: preflight refuses it and shows the fix |
| `demo-warehouse` | A PostgreSQL warehouse |
| `demo-http` | An HTTP endpoint |
| Templates | One per destination |
| Teammates | `editor@`, `operator@` and `viewer@bezisync.local` |

## Start the lab

1. Click **Open in GitHub Codespaces** in the README. You need a GitHub account.
2. Wait for the first start; it takes a few minutes.
3. Sign in with the passwords the terminal prints.

The admin and team passwords are random, made for that lab. Besides the terminal, they are in
`.devcontainer/lab/.state/login.txt`.

## What it costs and when it stops

- The lab runs on the 2-core machine, which uses your free Codespaces quota (2 core-hours per
  hour).
- A codespace stops after 30 minutes without activity and is deleted after your retention period.
  Those are your GitHub settings, not the lab's.
- To delete it yourself, go to [github.com/codespaces](https://github.com/codespaces).

## Run the same lab on Docker

The same lab runs on any Docker. Start it with `make lab` (or `bash .devcontainer/lab/up.sh`) and
stop it with `make lab-down`.

The extras come from `BEZISYNC_SANDBOX_*` settings (see the [configuration
reference](configuration-reference.md)), so the demo stack can use them too.

---

[All documentation](../README.md)
