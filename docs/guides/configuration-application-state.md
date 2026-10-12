# Provider configuration application diagnostics

`GET /api/v1/bamboo/config/live-health` retains its existing `providers` and
`mcp` health objects and adds a secret-free `application` object. Health reports
input validation and last-known-good recovery. Application receipts report the
configuration actually used by a runtime consumer.

This first slice ([#1891](https://github.com/bigduu/Bamboo-agent/issues/1891))
tracks `provider-runtime`. MCP, Connect and future-use readers report `unknown`
until their instrumentation in [#1892](https://github.com/bigduu/Bamboo-agent/issues/1892)
and [#1893](https://github.com/bigduu/Bamboo-agent/issues/1893). Listeners and
background services remain `unknown` and need separate instrumentation.

`application.source` is `process_observed_facade`. Section revisions identify
the effective snapshots observed by this process, including retained LKG. An
external edit can still be waiting for the existing watcher; a GET does not
promise a fresh disk read or promote an applied receipt.

A successful provider registry and default-client publication records the
provider client inputs, effective Core proxy inputs and keyword masking. This
is local client initialization, not proof that a remote endpoint accepts a
request. A partial registry reports `unknown`; a usable default does not
acknowledge every enabled client.

The vectors contain section revisions, not a global counter. An unrelated field
in the same section can advance the desired revision while the consumer remains
`in_sync`, preserving its actual applied vector and timestamp. A failed attempt
retains that vector and exposes only a static `last_apply_error.code`. A rejected
typed request may leave `state: in_sync` with that error when desired and applied
inputs still match; successful repair clears the error.

Credential source `observed_inputs` requires a private comparison against the
whole retained credential document, including sibling Core proxy inputs. An
owned Provider transaction alone cannot prove a rotated proxy credential was
used. Unproved sources remain `unknown`, and a GET never upgrades them. Runtime
environment-key precedence is preserved; an uncovered missing reference remains
unavailable. Values, secret identifiers, private digests and runtime errors are
never exposed.

Receipts are process-local and rebuilt on restart. They add no persistence file,
watcher, restart operation or second configuration authority. Legacy/injected
states without a facade report `unknown`. Coverage is limited to the listed
Provider fields; `core.headless_auth` is separately tracked in
[#1876](https://github.com/bigduu/Bamboo-agent/issues/1876).
