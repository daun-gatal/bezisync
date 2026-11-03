#!/usr/bin/env bash
# Release tool (tools/cmd/release): assemble a release from changelog fragments, print the
# current/next version or a release's notes, or check that a change carries a fragment exactly
# when it reaches an image or chart. `scripts/release.sh --help` lists the modes.
set -euo pipefail
cd "$(dirname "$0")/../tools"
exec go run ./cmd/release "$@"
