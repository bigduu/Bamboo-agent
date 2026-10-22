# Host-authorized Session discovery (B3)

Issue [#2167](https://github.com/bigduu/Bamboo-agent/issues/2167) continues B1
#2148/#2151 and B2 #2155/#2164, using #2162's final read API at `12fce15a`.
The latter includes the strict lookup correction to `82aa7385`. This source
slice is independent of prompt #2146 and contains no Host send writer.

`HostSessionDiscovery` implements the existing role-neutral discovery port.
It calls only canonical `list_host_scoped_targets`/`load_host_scoped_target`
for peer data. Those methods retain installed publication and canonical
lifecycle/Task/Session/tree authority through physical reads, including the
complete original caller generation. No boolean preflight, global index or
ordinary `load_session` supplies Host peer authority. The bounded progress
projection is the same function used by B1's Supervisor adapter.

`SessionDiscoveryRouter` gives an installed Host token priority for every role.
An installed, stale, revoked, denied or unsupported scope never falls through.
Only never-installed absence in the current store instance can select the real
original Supervisor adapter. Ordinary callers without a Host read scope receive
an explicit blocker; Project membership, supervision, IDs and cursors grant
nothing. ListMetadata and InspectBoundedProgress remain separate rights.
Legacy list/inspect recheck original caller and then Host absence before
returning; an installation or revocation during the old reader's awaits rejects
the old observation instead of switching routes or leaking its wider result.

## Pages and original operations

Pages retain B1's limit 20, query byte limit 128 and cursor byte limit 4096.
Authorization precedes filtering/order/paging. The cursor binds grant UUID and
revision, normalized query/limit/position and SHA-256 of the complete original
`SessionCallerReference`, including its reset ledger. The ledger and workspace
bytes are not embedded in model context. A changed receipt at the same reset
revision changes the digest. This digest locates a page; final Storage still
checks the complete caller and grant. It is not an authorization substitute.

Host metadata rows have `link_revision: 0` because there is no Supervisor link,
plus explicit `authorization_revision`. Their page `scope_revision` is the Host
grant revision. Legacy Supervisor rows retain their real link revisions and
omit the additive authorization field. Inspect still reports persisted,
Session-scoped evidence: no live status, input-specific attribution or verified
completion is claimed. It adds no history to another Session.

## Runtime and compatibility boundary

Server Root and SDK defaults use the new `host_routed` viewer. SDK defaults also
register the exact filtered `session_history_current` tool. Explicit SDK
tools/no-tools/default-executor choices retain precedence. Base/Child surface
and native ceiling integration remain with their separate owner. The existing
ordinary Ultra orchestration allowlist still blocks `session_history`; this
slice does not claim that model path is enabled before the surface owner's
change.

Every routed list/inspect uses the router. The ordinary Root viewer's legacy
global listing/search and known-ID peer raw history, metadata, compressed cache
and export shortcuts are narrowed to explicit list/inspect. Self reads retain
existing filters and budgets. Routed/self readers recheck the full original
generation before and after their existing read when the original observation
is available; the routed constructor requires it. Existing legacy constructors
remain explicit embedding compatibility surfaces.

A real Supervisor with never-installed Host scope retains its existing managed
`get_meta`/`read_messages` reader; this reader retains its canonical old guards
and rechecks absence/original caller before returning. Installed Host scope has
only metadata/progress rights here, never full transcript authority. No new
ReadFullHistory right or implicit permission follows from bounded inspection.

Native read-only/Plan/disabled gates are unchanged. Configless SDK read calls
follow existing read-only permission behavior; that does not authorize create
or send, nor remove B2's separate configless-send blocker. The in-process Host
issuer/registry is default-empty and ephemeral. A new store instance needs an
explicit trusted Host installation before admission when Host scope is required;
no durable grant/revocation restoration across process restart is claimed.

At the parent's request this slice also freezes the one-condition Overlay fix
for bound None/v1/unsupported behavior versions: unresolved Root references
stay exact instead of re-entering a raw Child alias fallback. Unbound Host
compatibility and resolved exact owners remain unchanged; retained Child/custom
operations keep their separate exact replay selector.

## Validation status

Eleven new Rust case sources cover seven temporary canonical Host/router cases, one
pure large-ledger cursor case, both real Server/SDK assembly entries, and the
bound-version Overlay regression. They include all three caller roles, right
separation, metadata privacy, bounded visible progress, query/grant/generation
changes, known-ID/raw/global bypass rejection, true legacy Supervisor absence,
unsupported/final denial, revocation, a fresh store issuer and publication
changes while both legacy readers are paused. Scope installs and revocations
occur only in temporary fixtures, never in user configuration or production
assembly. There are no provider calls or actual history reset operations.

Direct rustfmt, diff and source-static checks are lightweight evidence only.
**Cargo, compilation, Clippy and all Rust cases remain UNRUN** until the parent
hands off the Rust window. Keep the stacked PR Draft. Required compilation,
affected runtime cases and current-head/base review block admission. This slice
does not complete ordinary cross-Root sending or all Child runtime surfaces.
