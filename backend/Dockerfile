# One static binary for every role: api, controller, agent, migrate and the CLI.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/bezisync ./cmd/bezisync

# Distroless: no shell, no package manager; CA certificates and tzdata only.
FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
ARG COMMIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="bezisync" \
      org.opencontainers.image.description="BeziSync control plane, controller and agent" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"
COPY --from=build /out/bezisync /usr/local/bin/bezisync
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["bezisync"]
CMD ["api"]
