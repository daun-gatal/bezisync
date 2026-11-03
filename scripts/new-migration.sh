#!/usr/bin/env bash
# Start the next goose migration (plain SQL, applied in a transaction by `bezisync migrate`).
#   scripts/new-migration.sh "add pipeline labels"
# Write the up and down statements, then run `make verify`: the migrate tests apply it to a fresh
# database and compare the result with internal/migrate/testdata/schema.txt (update that dump in
# the same change).
set -euo pipefail
cd "$(dirname "$0")/../backend/internal/migrate/sql"
[ -n "${1:-}" ] || { echo "usage: $0 \"what it does\"" >&2; exit 2; }
next=$(printf "%05d" $(( $(ls [0-9]*.sql | sed 's/_.*//' | sort -n | tail -1 | sed 's/^0*//') + 1 )))
name="${next}_$(echo "$1" | tr '[:upper:] ' '[:lower:]_' | tr -cd 'a-z0-9_').sql"
cat > "$name" <<SQL
-- +goose Up
-- $1


-- +goose Down

SQL
echo "backend/internal/migrate/sql/$name"
