# One static binary for every role: api, controller, agent, migrate and the CLI.
# The build stage runs on the build machine's platform and cross-compiles for the target, so
# multi-arch images need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
ARG TARGETOS TARGETARCH
# Set by the release workflows (e.g. 0.2.0 or 0.2.0-pre); empty keeps internal/version's value.
ARG RELEASE_VERSION=""
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w ${RELEASE_VERSION:+-X github.com/daun-gatal/bezisync/backend/internal/version.Version=$RELEASE_VERSION}" \
      -o /out/bezisync ./cmd/bezisync

# Distroless: no shell, no package manager; CA certificates and tzdata only.
FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
ARG COMMIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="bezisync" \
      org.opencontainers.image.description="BeziSync control plane, controller and agent" \
      org.opencontainers.image.source="https://github.com/daun-gatal/bezisync" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"
COPY --from=build /out/bezisync /usr/local/bin/bezisync
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["bezisync"]
CMD ["api"]
