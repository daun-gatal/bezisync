# syntax=docker/dockerfile:1.7
# Static assets: built once on the build machine's platform for every target.
FROM --platform=$BUILDPLATFORM oven/bun:1.4 AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bunx --bun vite build

FROM nginxinc/nginx-unprivileged:1.29-alpine
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
ENV BEZISYNC_API_UPSTREAM=http://bezisync-api:8000
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
