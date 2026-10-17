# Providers, MCP and applied configuration

Provider instances have stable routing IDs; provider type and instance ID are
not interchangeable. Legacy provider stanzas remain a compatibility surface.
The selected/default provider, role-model routing, credentials and effective
proxy inputs can all influence runtime construction. Credential presence alone
does not prove a model is available, authorized or reachable.

The compatibility configuration's serialized MCP key is `mcpServers`; `mcp`
is a legacy accepted alias. Do not infer a typed section envelope from that
compatibility shape. MCP manager, client and tool-index publication is an
existing transaction; inspecting documentation must not start or reconnect a
server or alter its tool catalog.

## What live-health proves

`GET /api/v1/bamboo/config/live-health` exposes provider/MCP health and an
application object. Its source is `process_observed_facade`. Section revisions
are observations of effective snapshots, including retained last-known-good
values. Reading does not force a disk reload, advance a revision or promote an
applied receipt; a file watcher may not yet have observed an external edit.

Provider receipts acknowledge local registry/default-client publication. They
do not prove successful remote authentication, network reachability, quota or
completion. Partial registries remain unknown; one usable default does not
acknowledge every enabled client.

MCP receipts acknowledge the successfully published manager/client/tool-index
generation, including effective proxy inputs. They are not continuous
connection-health checks. A later connection failure is a different observation.

Desired and applied revisions are vectors by section, not a global counter.
An unrelated change in a section can advance a desired revision while inputs
remain in sync. Failed application retains previous receipts and reports a
static error code. A rejected typed update may coexist with an in-sync prior
configuration. A GET cannot establish that newly observed credentials were
used by every consumer.

Receipts are process-local and rebuilt after restart. Only explicitly listed
consumer coverage is supported; legacy/injected states and uninstrumented
consumers remain unknown. Do not generalize provider/MCP receipts to listener,
background-service or Connect application state.

For any instance explanation, identify the authorized observation's time,
scope and coverage. Prefer allowlisted state labels, counts, booleans and
revisions. Do not forward raw config, private identifiers/digests, endpoints,
environment values or runtime error text by default. This manual adds no new
runtime diagnostic endpoint or automatic collection.

Source: `crates/infra/bamboo-config/src/config.rs`, `section_facade.rs`;
`docs/guides/configuration-application-state.md`;
`crates/app/bamboo-server/src/handlers/settings/bamboo_config/credentials.rs`.
