# cmd

## Overview
The backend module's binaries. BeziSync ships one Go binary for every role (API, controller, agent, migrations, CLI), so there is a single command here; all logic lives under `backend/internal/`.

## Subdirectories & Boundaries
- [bezisync](./bezisync/README.md) — `main` for the `bezisync` binary; delegates straight to `internal/cli`.
