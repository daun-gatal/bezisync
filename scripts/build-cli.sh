#!/usr/bin/env bash
# Build the bezisync CLI (the same binary as the image: api, controller, agent and CLI) for every
# platform a release ships, into dist/: bezisync_<version>_<os>_<arch>.tar.gz (.zip on Windows)
# and checksums.txt (sha256).
#
#   scripts/build-cli.sh 0.2.0
set -euo pipefail
VERSION=${1:?usage: scripts/build-cli.sh VERSION}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIST=$ROOT/dist
TARGETS=(linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64)
rm -rf "$DIST" && mkdir -p "$DIST"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
for target in "${TARGETS[@]}"; do
  os=${target%/*} arch=${target#*/}
  name=bezisync_${VERSION}_${os}_${arch}
  bin=bezisync
  [ "$os" = windows ] && bin=bezisync.exe
  mkdir -p "$work/$name"
  (cd "$ROOT/backend" && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath \
    -ldflags "-s -w -X github.com/daun-gatal/bezisync/backend/internal/version.Version=$VERSION" \
    -o "$work/$name/$bin" ./cmd/bezisync)
  cp "$ROOT/LICENSE" "$ROOT/NOTICE" "$work/$name/"
  # BeziSync's and every linked module's licences (also `bezisync licenses`).
  cp "$ROOT/backend/internal/legal/licenses.txt" "$work/$name/LICENSES.txt"
  if [ "$os" = windows ]; then
    (cd "$work" && zip -qr "$DIST/$name.zip" "$name")
  else
    tar -C "$work" -czf "$DIST/$name.tar.gz" "$name"
  fi
  echo "built $name"
done
(cd "$DIST" && sha256sum -- * > checksums.txt)
