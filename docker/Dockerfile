# Bamboo (bamboo-agent) container image
#
# Builds the `bamboo` binary and runs the unified HTTP server.
#
# NOTE: Pin the builder image to the same Debian release as the runtime image.
# Using `rust:latest` can move to a newer glibc (e.g. 2.39) while the runtime
# stays on Bookworm (glibc 2.36), producing:
#   /lib/.../libc.so.6: version `GLIBC_2.39' not found
#
# Keep builder+runtime aligned to avoid glibc ABI mismatches.

FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy the full workspace: root bin (src + build.rs), crates/ members,
# and builtin_skills/ (embedded by bamboo-skills' build.rs). .dockerignore
# keeps target/.git out of the build context.
COPY . .

# Build just the bamboo binary (release, workspace-aware)
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

RUN mkdir -p /data

COPY --from=builder /app/target/release/bamboo /usr/local/bin/bamboo

ENV RUST_LOG=info
ENV BAMBOO_DATA_DIR=/data
ENV BAMBOO_PORT=9562
ENV BAMBOO_BIND=0.0.0.0

EXPOSE 9562

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9562/api/v1/health || exit 1

ENTRYPOINT ["bamboo"]
CMD ["serve", "--port", "9562", "--bind", "0.0.0.0", "--data-dir", "/data"]
