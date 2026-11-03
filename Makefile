.PHONY: help verify verify-quick verify-backend verify-backend-quick verify-tools verify-frontend e2e e2e-quick e2e-down openapi openapi-check dev-db migration demo demo-down demo-smoke

export PATH := $(HOME)/.local/bin:$(HOME)/.local/go/bin:$(HOME)/.local/node/bin:$(PATH)
TEST_DB = $${BEZISYNC_TEST_DATABASE_URL:-$$(../scripts/dev-db.sh)}

help:
	@echo "make verify         backend (golangci-lint, vet, go test -race) + tools + frontend (types, tests, build, API drift)"
	@echo "make verify-quick   lint + vet everywhere, tools, frontend unit tests, and only the backend packages GO_PKGS=… (default all)"
	@echo "make e2e            build images, deploy to a disposable k3s-in-Docker cluster, run Playwright"
	@echo "make e2e-quick SPECS=…  reuse the cluster, rebuild only changed images, run only these specs"
	@echo "make e2e-down       remove everything the e2e run created"
	@echo "make openapi        render frontend/openapi.json from backend/internal/api/openapi.json + typed client"
	@echo "make migration m=…  start the next goose migration (SQL)"
	@echo "make dev-db         start the disposable test Postgres and print its URL"
	@echo "make demo           start the demo stack (deploy/compose) on http://localhost:8080"
	@echo "make demo-smoke     bring the demo stack up, stream through the sandbox, tear it down"

dev-db:
	@scripts/dev-db.sh

verify: verify-backend verify-tools verify-frontend

# While iterating: everything but the race detector and the production build.
verify-quick: openapi-check verify-backend-quick verify-tools
	cd frontend && bun run typecheck && bun run test
	cd e2e && npx tsc --noEmit

# Go tests need the test database; GO_PKGS=… limits them while iterating.
GO_PKGS ?= ./...
verify-backend-quick:
	cd backend && golangci-lint run ./... && go vet ./...
	cd backend && BEZISYNC_TEST_DATABASE_URL=$(TEST_DB) go test $(GO_PKGS)

verify-backend:
	cd backend && golangci-lint run ./... && go vet ./...
	cd backend && BEZISYNC_TEST_DATABASE_URL=$(TEST_DB) go test -race ./...

# tools/: the release tool and the demo smoke test.
verify-tools:
	cd tools && golangci-lint run ./... && go vet ./... && go test ./...

verify-frontend: openapi-check
	cd frontend && bun run typecheck && bun run test && bun run build
	cd e2e && npx tsc --noEmit

openapi:
	cd backend && go run ./cmd/bezisync openapi --output ../frontend/openapi.json
	cd frontend && bun run generate:api

openapi-check:
	cd backend && go run ./cmd/bezisync openapi --output /tmp/bezisync-openapi.json
	diff -q /tmp/bezisync-openapi.json frontend/openapi.json || (echo "frontend/openapi.json is stale: run make openapi" && exit 1)
	cd frontend && bun run check:api

migration:
	scripts/new-migration.sh "$(m)"

e2e:
	scripts/e2e.sh

e2e-quick:
	scripts/e2e.sh quick $(SPECS)

e2e-down:
	scripts/e2e.sh down

demo:
	docker compose -f deploy/compose/docker-compose.yml up -d --build

demo-down:
	docker compose -f deploy/compose/docker-compose.yml down -v

demo-smoke:
	scripts/demo-smoke.sh
