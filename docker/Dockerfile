# Only an authenticated original binary and Host package enter this context.
# Node's official multi-architecture index/config metadata was verified on
# 2026-10-27 (Node 24.22.1 on amd64/arm64); compatibility still requires smoke.
FROM node:24-bookworm-slim@sha256:55fb5f5a527ea2c5c4a95f963e48ef90aa38c7b346054fdc8ee9957a2b55d205

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/bamboo/ms-playwright
WORKDIR /opt/bamboo/browser-runtime

# The original lock owns Host dependencies. This is a runtime-only install,
# without package scripts, frontend installation, Vite or a Rust toolchain.
COPY browser-runtime/package.json browser-runtime/package-lock.json ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 libgcc-s1 libstdc++6 \
    && npm ci --omit=dev --ignore-scripts --no-audit --no-fund \
    && node node_modules/playwright-core/cli.js install --with-deps --only-shell chromium \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*
COPY browser-runtime/host.cjs ./host.cjs
COPY bamboo /usr/local/bin/bamboo
COPY originals.json /opt/bamboo/originals.json

# Keep the existing unprivileged identity, data directory and broker override.
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin bamboo \
    && mkdir -p /data \
    && chown bamboo:bamboo /data
ENV HOME=/data
ENV BAMBOO_BROWSER_HOST_SCRIPT=/opt/bamboo/browser-runtime/host.cjs
ENV BAMBOO_BROWSER_NODE=/usr/local/bin/node
ENV RUST_LOG=info
ENV BAMBOO_DATA_DIR=/data
ENV BAMBOO_PORT=9562
ENV BAMBOO_BIND=0.0.0.0
WORKDIR /app
EXPOSE 9562
USER bamboo
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9562/api/v1/health >/dev/null \
    && curl -fsS http://127.0.0.1:9562/ >/dev/null \
    && test -s /data/frontend/.frontend-manifest.json \
    || exit 1
ENTRYPOINT ["bamboo"]
CMD ["serve", "--port", "9562", "--bind", "0.0.0.0", "--data-dir", "/data"]
