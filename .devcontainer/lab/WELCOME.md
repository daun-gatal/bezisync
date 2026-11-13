# Welcome to your BeziSync lab

> **The lab is still starting if no BeziSync tab has opened yet.** The first start downloads
> the images, which takes a few minutes. Watch it with <kbd>F1</kbd> › **Codespaces: View
> Creation Log**. When it's ready, a BeziSync tab opens, and the terminal shows your sign-in.

This codespace runs a private copy of BeziSync with sample databases that change every few
seconds. It's yours alone: you are the admin, and nobody else can open it.

**You don't need this editor.** It only hosts the lab. Everything happens in the BeziSync tab.

## 1. Open BeziSync and sign in

If the tab didn't open (a popup blocker can stop it), open the **Ports** panel at the bottom and
click the globe icon next to **BeziSync (8080)**.

Sign in as `admin@bezisync.local`. The password is a random one made for this lab. It's printed
in the terminal when the lab is ready, and saved in `.devcontainer/lab/.state/login.txt`.

## 2. What's in the Sandbox project

| | Name | What it is |
|---|---|---|
| Source | `demo-shop` | PostgreSQL shop: customers and orders, a new order every 2 seconds |
| Source | `demo-mysql` | MySQL shop with GTIDs, its own live orders |
| Source | `legacy-postgres` | A PostgreSQL that **isn't ready** for change data capture, on purpose |
| Destination | `demo-kafka` | Kafka |
| Destination | `demo-warehouse` | A PostgreSQL warehouse: pipelines create their tables there |
| Destination | `demo-http` | An HTTP endpoint that accepts every event |
| Runtime | `local-docker` | Runs one Debezium Server per pipeline, inside this codespace |

There is a template for each destination (**Shop → Kafka**, **Shop → Warehouse**, **Shop →
HTTP**), and three teammates: `editor@`, `operator@` and `viewer@bezisync.local`.

## 3. Things to try

1. **Create your first pipeline.** Click **New pipeline**, under **Start from** pick **Shop →
   Kafka (demo)**, choose the `demo-shop` source, keep `shop.orders` in **Tables**, and deploy
   from **Review & deploy**. Preflight checks everything first. The first deploy downloads
   Debezium Server (about a minute), then the pipeline turns *Streaming* and its **Events** tab
   shows the orders arriving.
2. **Copy a shop into a warehouse.** Do the same with **Shop → Warehouse (demo)** and the
   `demo-mysql` source. The orders land in the warehouse as the table `demo_shop_orders`.
3. **See preflight catch a problem.** Start a pipeline from `legacy-postgres`. Preflight refuses
   it and says exactly what to change (`wal_level=logical`) and how.
4. **Operate it.** Pause and resume a pipeline, take a snapshot, look at its versions and roll
   one back, and read the audit log.
5. **Try the roles.** Sign out and sign in as `viewer@bezisync.local`, then
   `operator@bezisync.local`, with the team password from the terminal. Compare what each can
   do.

The guide covers everything else: <https://github.com/daun-gatal/bezisync/blob/main/docs/README.md>.

## When you're done

The codespace stops by itself after 30 minutes without activity. A stopped codespace still uses
some of your free storage until it's deleted. Delete it at
[github.com/codespaces](https://github.com/codespaces) (**⋯ › Delete**), or let GitHub remove it
after your retention period (30 days unless you changed it).

## Something went wrong?

- Start or repair the lab: run `bash .devcontainer/lab/up.sh` in the terminal. It's safe to
  run again.
- See what's running: `docker compose -p bezisync-lab ps`.
- Start over with fresh data: `bash .devcontainer/lab/down.sh && bash .devcontainer/lab/up.sh`.
- Still stuck? [Open an issue](https://github.com/daun-gatal/bezisync/issues/new?labels=lab)
  and paste what `up.sh` printed.

Want to run BeziSync for real? See **Install** in the
[guide](https://github.com/daun-gatal/bezisync/blob/main/docs/README.md#install).
