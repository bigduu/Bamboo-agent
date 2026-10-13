# Scoped broker peers

`bamboo broker serve --peer-policy-stdin` opts one listener into explicit peer
authentication. Provide a private JSON policy on stdin. `--token` is incompatible;
`BAMBOO_BROKER_TOKEN` is ignored by this listener. Non-loopback numeric binds
require `--cert` and `--key`. Workers retain the existing Hello and ProvisionSpec
wire; supply their configured credential through the child process environment
and in `spec.bus.token`, with explicit CA trust for private WSS certificates.

The closed policy has `peers`, each with `credential`, `host`, `mailbox`, optional
`role`, `expires_at` (UTC timestamp), and `destinations` entries containing exact
`mailbox` and `kinds`. Optional `cancel` and `presence` arrays grant exact targets
and roles. Missing grants deny access. Credentials are opaque 32–256 byte ASCII
graphic strings, never identity claims. Identifiers are 1–256 ASCII letters,
digits, `-`, `_`, or `.`, excluding `.` and `..`. Mailbox identities and
mailbox selectors additionally require lowercase spelling, rejecting filesystem
case aliases rather than silently normalizing them. Limits are 64 KiB, 64 peers and
16 destinations/cancel targets/presence roles. Duplicate fields, credentials,
mailbox principals, destinations and kind grants are rejected with static errors.

Every frame checks the captured identity, grant and deadline. Durable Event and
live event batches also validate physical source and QoS. Current subscription
ownership gates strict ACK admission; delivery-only and replaced connections
cannot remove pending records. Valid same-peer reconnect/uplinks remain allowed.
Already admitted I/O can finish after replacement or expiry; this is not an
Actor lease, instantaneous filesystem revocation, renewal or replay protection.
Deadline checks cover throttled delivery, idle connections and outbound sends.

The actual strict CLI uses `<base>/scoped-peers-v1/mailboxes`; legacy CLI and
embedded legacy constructors retain `<base>/mailboxes` and their global token.
No backlog migration or protection against trusted operators aliasing roots is
claimed. This does not grant logical Actor authority, register Hosts or deploy
remote workers. #1311, #932 and #791 remain separate work.

Focused fixtures exercise real WSS/Maildir ACKs and actual same-source CLI plus
BambooRuntime/provider execution, including separate-root legacy Ask delivery.
They require openssl and never substitute Echo; only the native test's host
fixture thread has an explicit debug stack size. Worker processes use defaults.
