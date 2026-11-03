# syntax=docker/dockerfile:1.7
# Static assets: built once on the build machine's platform for every target.
FROM --platform=$BUILDPLATFORM oven/bun:1.4 AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bunx --bun vite build

# The stable branch: nginx backports security fixes to it (a pinned 1.29 stopped receiving them).
FROM nginxinc/nginx-unprivileged:stable-alpine
ARG VERSION=dev
ARG COMMIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="bezisync-web" \
      org.opencontainers.image.description="BeziSync web UI" \
      org.opencontainers.image.source="https://github.com/daun-gatal/bezisync" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"
# The base image lags behind Alpine's security fixes: take them at build time (CI scans the result).
USER root
RUN apk upgrade --no-cache
USER 101
ENV BEZISYNC_API_UPSTREAM=http://bezisync-api:8000
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
