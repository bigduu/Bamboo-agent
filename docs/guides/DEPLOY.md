# How-to: deploy `bamboo serve`

Three ways to run Bamboo as a long-lived server, in increasing order of
isolation: bare binary, systemd unit, Docker.

## Option 1 — bare binary

```bash
cargo install --path .          # or: cargo install bamboo-agent
bamboo init --non-interactive --provider anthropic --api-key "sk-ant-..."
bamboo serve
```

Good for a single-user desktop/laptop setup, or as the target a sidecar
process manager (like Bodhi) launches directly.

## Option 2 — systemd (bare-metal Linux server)

```ini
# /etc/systemd/system/bamboo.service
[Unit]
Description=Bamboo agent server
After=network.target

[Service]
Type=simple
User=bamboo
Environment=BAMBOO_DATA_DIR=/var/lib/bamboo
Environment=BAMBOO_BIND=127.0.0.1
Environment=BAMBOO_PORT=9562
ExecStart=/usr/local/bin/bamboo serve
Restart=on-failure
RestartSec=5
# Hardening (optional but recommended for an always-on service)
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/bamboo
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --home /var/lib/bamboo bamboo
sudo mkdir -p /var/lib/bamboo && sudo chown bamboo:bamboo /var/lib/bamboo
sudo -u bamboo BAMBOO_DATA_DIR=/var/lib/bamboo bamboo init --non-interactive --provider anthropic --api-key "sk-ant-..."
sudo systemctl enable --now bamboo
```

## Option 3 — Docker

```bash
cd docker && docker compose up -d --build
curl http://localhost:9562/api/v1/health
```

`docker-compose.yml` (in `docker/`) already does the right things by default:

- **Publishes to the host loopback only** (`127.0.0.1:9562:9562`) — see
  [Network exposure](#network-exposure-the-part-you-must-not-skip) below for
  why this matters.
- Runs as a non-root user, drops all Linux capabilities (`cap_drop: [ALL]`,
  the agent needs none), sets `no-new-privileges`, and caps `pids_limit`.
- Uses an isolated named volume (`bamboo-data`) rather than bind-mounting your
  entire `~/.bamboo` read-write into the container. Uncomment the alternate
  bind-mount line in the compose file if you specifically want to share the
  host profile.
- Sets `BAMBOO_DATA_DIR=/data`, `BAMBOO_PORT=9562`, `BAMBOO_BIND=0.0.0.0`
  (the in-container bind — actual host exposure is controlled at the
  `ports:` publish layer above, not this bind address).

Configure the provider either by mounting a `config.json` into the volume
before first start, or via the [provider API key env vars](../config-reference.md#environment-variables)
(`BAMBOO_ANTHROPIC_API_KEY`, etc.) added to the `environment:` block — these
are in-memory-only and never written to disk, which is the preferred pattern
for container/CI deploys. `docker/config.example.json` is a starting point if
you'd rather mount a file.

## Network exposure — the part you must not skip

**Do not widen the Docker publish (or any bind) to `0.0.0.0`/a LAN IP without
also putting an authenticating reverse proxy in front.** Two things compound:

1. A fresh instance has no credential configured — the access-control gate is
   inert until you set a password.
2. Even once a password **is** set, the server treats every private-range
   (RFC1918) peer as trusted-local and skips the password check by design
   (desktop-mode convenience) — so any host on the same subnet reaches the
   tool-executing agent unauthenticated.

Keep the loopback-only publish/bind and put a real reverse proxy (nginx,
Caddy, Traefik) in front on whichever network needs remote access, terminating
TLS and its own auth there. Alternatively, set `server.tls` (`cert_file`/
`key_file` in `config.json` — see the [config reference](../config-reference.md#server))
for manual TLS termination inside Bamboo itself, if a separate proxy isn't an
option.

## Reverse proxy example (Caddy)

```
bamboo.example.com {
  reverse_proxy 127.0.0.1:9562
  basicauth {
    admin JDJhJDE0JC4uLg==   # bcrypt hash, generate with `caddy hash-password`
  }
}
```

Caddy handles TLS (via Let's Encrypt) and HTTP basic auth in front of a
loopback-only Bamboo — the recommended pattern instead of Bamboo's own
`server.tls`/`access_control` for anything beyond a single trusted LAN.

## CORS

If a browser-based client (e.g. a self-hosted Lotus frontend on a different
origin) talks to this server directly, set `BAMBOO_CORS_ALLOW_ORIGINS` (or
the equivalent config key) to an explicit allowlist — exact origins
(`https://app.example.com`), bare hosts (`app.example.com`), or wildcard
subdomains (`*.example.com`). Leave it empty for a same-origin-only setup
(e.g. Bodhi's embedded sidecar, which talks to `127.0.0.1` directly).

## Backing up a deployment

Back up the whole data directory (`BAMBOO_DATA_DIR`, default `~/.bamboo`) as
one unit — it holds `config.json`, `connect.json`, `schedules.json`,
`model_limits.json`, every session, and (critically) `.bamboo_encryption_key`.
Losing the key file without a copy of `BAMBOO_CONFIG_ENCRYPTION_KEY` makes
every encrypted-at-rest secret (provider API keys, IM bridge tokens,
notification push tokens, …) in that directory permanently unrecoverable —
see [Encryption at rest](../config-reference.md#encryption-at-rest).

## Health checks

`GET /api/v1/health` (used by the Docker example above) or `bamboo health`
(same check, from the CLI) — both exit/return non-zero if the server is
unreachable or unhealthy, so either works as a readiness/liveness probe.
