.PHONY: help verify verify-quick verify-backend verify-backend-quick verify-tools verify-frontend e2e e2e-quick e2e-down openapi openapi-check licenses licenses-check contracts-check dev-db migration demo demo-down demo-smoke lab lab-down lab-smoke chart-test chart-docs

export PATH := $(HOME)/.local/bin:$(HOME)/.local/go/bin:$(HOME)/.local/node/bin:$(PATH)
TEST_DB = $${BEZISYNC_TEST_DATABASE_URL:-$$(../scripts/dev-db.sh)}

help:
	@echo "make verify         backend (golangci-lint, vet, go test -race) + tools + frontend (types, tests, build, API drift)"
	@echo "make verify-quick   lint + vet everywhere, tools, frontend unit tests, and only the backend packages GO_PKGS=… (default all)"
	@echo "make e2e            build images, deploy to a disposable k3s-in-Docker cluster, run Playwright"
	@echo "make e2e-quick SPECS=…  reuse the cluster, rebuild only changed images, run only these specs"
	@echo "make e2e-down       remove everything the e2e run created"
	@echo "make openapi        render frontend/openapi.json from backend/internal/api/openapi.json + typed client"
	@echo "make licenses       regenerate the licence files the binary and the web image ship (after a dependency change)"
	@echo "make contracts-check  directory contracts (README.md per code directory, ARCHITECTURE.md) match the code"
	@echo "make migration m=…  start the next goose migration (SQL)"
	@echo "make dev-db         start the disposable test Postgres and print its URL"
	@echo "make demo           start the demo stack (deploy/compose) on http://localhost:8080"
	@echo "make demo-smoke     bring the demo stack up, stream through the sandbox, tear it down"
	@echo "make lab            start the Codespaces lab locally (make lab-down removes it)"
	@echo "make lab-smoke      start the lab as a codespace does, check every Sandbox extra, tear it down"
	@echo "make chart-test     Helm charts: lint (every ci/*-values.yaml), unit tests, helm-docs drift (in Docker)"
	@echo "make chart-docs     regenerate deploy/helm/*/README.md from README.md.gotmpl and values.yaml (in Docker)"

dev-db:
	@scripts/dev-db.sh

verify: verify-backend verify-tools verify-frontend licenses-check contracts-check

# While iterating: everything but the race detector and the production build.
verify-quick: openapi-check verify-backend-quick verify-tools licenses-check contracts-check
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

# backend/internal/legal/licenses.txt and frontend/public/licenses.txt: BeziSync's licence and
# every shipped dependency's. Fails on a licence outside the allow-list (tools/licenses).
licenses:
	cd tools && go run ./cmd/licenses

licenses-check:
	cd tools && go run ./cmd/licenses --check

# README.md per code directory and ARCHITECTURE.md: sections, links, cited symbols, test targets (.rules/egm.md).
contracts-check:
	cd tools && go run ./cmd/contracts

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

lab:
	bash .devcontainer/lab/up.sh

lab-down:
	bash .devcontainer/lab/down.sh

lab-smoke:
	scripts/lab-smoke.sh

# Helm charts, in containers so no local helm is needed (CI runs the same checks in helm.yml).
HELM_UNITTEST_IMAGE = helmunittest/helm-unittest:4.3.0-1.2.1
HELM_DOCS_IMAGE = jnorwood/helm-docs:v1.14.2
chart-test: chart-docs
	tar -cf - deploy/helm | docker run -i --rm --user root --entrypoint sh $(HELM_UNITTEST_IMAGE) -c '\
	  mkdir -p /w && tar -xf - -C /w && cd /w && \
	  for c in bezisync bezisync-agent; do for v in deploy/helm/$$c/ci/*-values.yaml; do \
	    helm lint deploy/helm/$$c --strict -f $$v || exit 1; done; done && \
	  helm unittest -f "unittests/*_test.yaml" deploy/helm/bezisync deploy/helm/bezisync-agent'
	git diff --exit-code -- deploy/helm/bezisync/README.md deploy/helm/bezisync-agent/README.md

chart-docs:
	tar -cf - deploy/helm | docker run -i --rm --entrypoint sh $(HELM_DOCS_IMAGE) -c '\
	  mkdir -p /w && tar -xf - -C /w && cd /w && helm-docs --chart-search-root deploy/helm >&2 && \
	  tar -cf - deploy/helm/bezisync/README.md deploy/helm/bezisync-agent/README.md' | tar -xf -
