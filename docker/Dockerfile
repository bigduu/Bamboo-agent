# Bamboo (bamboo-agent) container image
#
# Builds the `bamboo` binary and ships it on a slim Debian runtime. The image
# serves two roles:
#   - default: the unified HTTP server (`bamboo serve`, the ENTRYPOINT/CMD below)
#   - broker worker: the orchestrator's DockerDeployer overrides the entrypoint
#     to seed a writable data dir and run `bamboo broker-agent serve ...`
#
# NOTE: Pin the builder image to the same Debian release as the runtime image.
# Using `rust:latest` can move to a newer glibc (e.g. 2.39) while the runtime
# stays on Bookworm (glibc 2.36), producing:
#   /lib/.../libc.so.6: version `GLIBC_2.39' not found
# Keep builder+runtime aligned to avoid glibc ABI mismatches.

FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy the full workspace. `.dockerignore` keeps target/ and .git out;
# builtin_skills/ is embedded by bamboo-skills' build.rs (include_bytes!), so it
# must stay in the build context (do NOT exclude **/*.md).
COPY . .

# Build from crates.io (default sparse registry). Builds on CI runners with
# direct registry access — no vendoring, no registry mirrors.
RUN cargo build --release --locked --bin bamboo

# ===== Runtime image =====
FROM debian:bookworm-slim

WORKDIR /app

# Runtime deps: TLS certs + curl (healthcheck) + OpenSSL for reqwest
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user. The container was previously PID-1 root with a
# read-write host-config mount, so a compromise of the tool-executing agent
# could rewrite the host's config / encryption key / sessions as root. A
# system user with /data as its home closes that escalation. (#249)
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin bamboo \
    && mkdir -p /data \
    && chown -R bamboo:bamboo /data

COPY --from=builder /app/target/release/bamboo /usr/local/bin/bamboo

ENV RUST_LOG=info
ENV BAMBOO_DATA_DIR=/data
ENV BAMBOO_PORT=9562
# The server binds 0.0.0.0 INSIDE the container so a published port can reach
# it; restrict *exposure* at the compose publish layer (127.0.0.1) rather than
# here — see docker-compose.yml. Do not publish to the network directly: a fresh
# instance is unauthenticated, and the server treats every private-LAN peer as
# trusted-local (skipping the password check by design), so LAN exposure is
# unauthenticated even with a password set — front it with an authenticating
# reverse proxy instead.
ENV BAMBOO_BIND=0.0.0.0

EXPOSE 9562

USER bamboo

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9562/api/v1/health || exit 1

ENTRYPOINT ["bamboo"]
CMD ["serve", "--port", "9562", "--bind", "0.0.0.0", "--data-dir", "/data"]
