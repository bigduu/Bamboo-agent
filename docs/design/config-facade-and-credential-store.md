# ADR: Modular live configuration and credential isolation

- Status: accepted and implemented
- Issue: #597
- Date: 2026-07-21

## Decision

`Config` remains a compatibility/effective-value facade, not a persistence
aggregate. Each persisted section owns a versioned JSON store and an immutable
last-known-good snapshot. `AtomicJsonStore<T>` is the shared durable primitive;
`LiveSection<T>` provides revisioned process snapshots and health transitions.
Runtime components receive a snapshot/facade dependency and do not read files
on a request path.

Secrets are addressed by stable `CredentialRef` values and stored only in the
versioned, encrypted `credentials.json`, environment variables, or an explicitly
documented external secure store. Section DTOs may contain `credential_ref`,
`token_env`, certificate/key paths, and configured metadata, but never plaintext,
ciphertext, or a UI mask.

## Final section and file mapping

| Section | Target file | Contents |
| --- | --- | --- |
| core/network | `core.json` | server bind/port, proxy references, headless mode |
| providers | `providers.json` | provider instances, routing, defaults/features, credential refs |
| MCP | `mcp.json` | server transport/settings and credential refs |
| tools/skills | `tools-skills.json` | tool policy, skill policy and catalog settings |
| memory | `memory.json` | memory and background-maintenance settings |
| subagents/broker metadata | `subagents.json` | limits, broker discovery/placement metadata; no bearer token |
| notifications | `notifications.json` | channel settings and credential refs |
| connect | `connect.json` | chat-platform settings, allowlists and credential refs |
| cluster fabric | `cluster-fabric.json` | hosts, paths and credential refs |
| environment variables | `env.json` | non-secret values; secret entries carry a credential ref |
| access control | `access-control.json` | enabled state and credential refs/configured metadata only |
| hooks | `hooks.json` | hook definitions and policy |
| keyword/model mappings | `model-policy.json` | masking and Anthropic/Gemini mappings |
| model limits | `model_limits.json` | existing model limit records |
| credentials | `credentials.json` | encrypted, versioned credential records only |

During migration, `config.json`, `broker.json`, `settings.json`, `connect.json`,
and already extracted sidecars remain readable. Existing `memory.json`,
`subagents.json`, and `providers.json` writes now share `AtomicFileStore`; their
wire shape stays unchanged until the manifest migration switches them to the
revisioned envelope.

## Snapshot lifecycle and precedence

Precedence is defaults, migrated legacy values, section file, environment, then
explicit CLI overrides. Environment and CLI layers are effective views and are
not written back. Every section snapshot records `revision`, `loaded_at`, source
path/kind, status, and a redacted `last_error`.

The parent-directory watcher is non-recursive, accepts create/modify/rename/delete
events, filters store temp/lock/backup/quarantine files, and coalesces bursts.
Self-writes are suppressed only when the observed bytes match the committed
fingerprint, so a subsequent external edit is not discarded. A candidate is
parsed and validated before publication. Invalid input retains the previous data
and revision while changing only health/error metadata and publishing
`config.invalid`, even when a disk backup is usable. A valid repair publishes
`config.recovered`. Ordinary commits/reloads publish `config.changed`. An external
edit whose content differs without advancing the revision is normalized under the
store lock to a new durable monotonic revision before publication, so an older CAS
token cannot overwrite it.

Deletion is first treated as a transient watcher condition. Server adapters must
retry across the debounce window before applying each section's missing-file
policy. Provider, MCP, environment, notification and cluster side effects run
after snapshot publication; adapters must construct a replacement runtime first
and keep the old runtime if construction fails, marking the section degraded.

## Atomicity, CAS and recovery

The shared store uses a same-directory UUID temp file, write-all, file fsync,
atomic rename, parent-directory fsync on Unix, cleanup-on-error, rotating
schema-valid backups, and an inter-process advisory lock. Sensitive stores enforce
directory `0700` and file `0600` on Unix. Windows applies a protected DACL granting
full access only to the owner and SYSTEM on sensitive directories, data files and
lock files, and uses write-through replace semantics. Ordinary new files honor the
process umask, while replacements preserve an existing stricter Unix mode.

The required ordering is:

1. parse and validate candidate;
2. acquire the store lock and compare `expected_revision`;
3. durably commit;
4. publish immutable snapshot;
5. apply runtime side effects.

Each live section serializes commit and reload operations through a section-local
operation mutex covering snapshot read, store operation, publication, and event
construction. This closes the interval after the file lock is released but before
the durable candidate is published; read-only snapshot access does not take this
operation lock.

A stale revision is a conflict (HTTP adapters map it to `409`). A failed commit
does not mutate the live snapshot. Multi-section API operations must be decomposed
into independent user-visible commits; any future operation requiring all-or-none
semantics must use a staged manifest/journal transaction rather than sequential
file rewrites.

For a credential-backed modular section, the owned section envelope is the sole
public CAS authority. This applies to `core` proxy authentication, `env`,
`notifications`, `connect`, `access-control`, and `cluster-fabric`. The
`credentials.json` revision is an internal transaction member and diagnostic
health value, never the precondition for one of those domain forms. An exact
transaction compares the client section revision, stages metadata and explicit
credential actions together, and three-way-merges an unrelated credential-store
winner under the migration lock. A competing edit to the owned section returns
`409`; an unrelated credential edit does not create a false domain conflict.
The committed section envelope and matching `config.changed` event use the same
revision, even for a secret-only replace or clear.

On a corrupt primary, the bytes are copied to a uniquely named quarantine and the
newest valid backup is loaded as degraded last-known-good. No default is written
over the corrupt file. Repairing the primary transitions to healthy. Errors exposed
through snapshots/events are category-only and never include user data or paths.
Repeated reads of identical corrupt bytes reuse the same quarantine after comparing
contents under the store lock, avoiding unbounded duplicate files without placing a
content-derived value in the quarantine filename.

## Credential inventory and references

| Legacy secret | Credential reference convention |
| --- | --- |
| built-in provider API key | `provider.<provider>.api_key` |
| provider-instance API key | `provider_instance.<id>.api_key` |
| HTTP proxy authentication | `proxy.default.auth` |
| MCP stdio secret environment value | `mcp.<server>.env_<name>` |
| MCP HTTP/SSE auth header | `mcp.<server>.header_<name>` |
| user environment entry with `secret=true` | `env.<name>.value` |
| ntfy token | `notification.ntfy.token` |
| Bark device key | `notification.bark.device_key` |
| cluster password/private key/passphrase | `cluster.<node-id>.<field>`; ordinary config stores these refs and configured metadata in `cluster_fabric.credential_refs` |
| connect platform token/app secret | `connect.<stable-platform-id>.<field>` |
| external broker bearer token | `broker.external.bearer_token` |
| access password/device token | `access.root.password_verifier` / `access.<device-id>.device_token_verifier`; ordinary config stores refs/configured metadata only |
| Copilot GitHub OAuth access token | `copilot.oauth.github_access_token` |
| Copilot chat token cache | `copilot.oauth.chat_config` |

The store reuses Bamboo's existing AES-GCM encryption key and records a key
version for future rotation. Its public status APIs return only reference,
configured state, source and update time. Mutations are replace or clear; empty
strings and masks are not credential values. Runtime resolution returns a
non-serializable, redacted-debug wrapper.

## Legacy migration protocol

Migration must be idempotent and manifest-gated:

1. acquire the migration/advisory lock and encrypt parseable legacy inputs into
   an owner-only, transaction-scoped backup (directory `0700`, files `0600` on
   Unix); delete both stage and backup after completion;
2. use independent provider/MCP/root and external-broker planners, parsing each
   legacy input as raw JSON so flattened and unknown fields remain attached to
   their owning section (unclassified fields go to the core section's `extra`
   map); a malformed optional broker document cannot block main configuration;
3. extract the credential inventory into encrypted records and replace ordinary
   values with references/configured metadata;
4. stage every candidate file and validate the complete candidate set;
5. fsync staged files, then atomically install a versioned manifest as the commit
   point;
6. on restart, discard an uncommitted stage or resume from the manifest; never
   infer completion from the presence of one section file.

Provider, provider-instance, MCP, proxy, secret env, notification, connect,
cluster-fabric, access-control and external-broker credentials use the shared
manifest-gated migration protocol. The
main and broker planners run independently under the same lock, stage and fsync
`credentials.json` plus only their affected members, install a pending manifest
as the commit point, and finish or resume any committed domain before runtime
readers use transaction members. Built-in/instance API keys, MCP stdio env values,
MCP HTTP headers, and the external broker bearer token become stable credential refs;
plaintext and legacy ciphertext are removed from ordinary documents and parseable
root/broker backup generations. Unknown fields remain attached to the raw JSON
candidate. A concurrent editor/API write is compared under the section file lock
and rebased with a higher migration generation instead of overwritten.
User-written credentials always outrank migration replay.
Backup generations are processed newest first: a backup-only instance is committed
to the credential store before that backup is rewritten, while an already configured
same ref remains authoritative over an older backup value. Unparseable root backups
are left untouched for manual recovery rather than destructively guessed at.

An older binary may later rewrite an unversioned sidecar. Migration compares the
resolved legacy value with an existing migrated credential: equal values are a
no-op, while a different value advances the stored migration generation. This
keeps old committed-stage replay from rolling the credential back while still
accepting genuinely newer legacy input. Pending or malformed manifests are a
fail-closed state: provider/MCP/broker loaders, startup health, watchers and typed writes
retain their current snapshot until recovery finishes; they never read a partial
transaction member.
Only `NotFound` means migration metadata is absent; permission, directory and
other read failures are redacted fail-closed errors. Before planning a new
transaction under the migration lock, Bamboo removes orphan stage/backup
directories only when their name is the exact managed prefix plus a canonical
UUID and no valid manifest or journal references them. Symlinks, non-UUID names
and referenced transactions are never traversed or removed.

Provider-instance create/update/delete, compatibility PATCH, and CLI dot-path
writes share the same exact credentials/providers/root transaction. Client-owned
`credential_ref` and legacy ciphertext fields are stripped, credential commit
precedes live publication, and generic saves fail closed if an unreferenced
instance secret would re-enter `config.json`.

Notification ntfy/Bark updates use their own exact Notifications-section and
credentials transaction scope. The complete notification subtree is
revision-protected; a committed transaction rebases unrelated credential-store
edits, rejects a competing Notifications-section edit, and rolls back both
members on an unsafe consumer conflict. Parseable root backup generations are
scrubbed only after their secret is durably represented in the credential store.
Runtime hydration fails closed when configured refs are missing or corrupt.
`PUT /bamboo/config/notifications`
requires the Notifications section revision and accepts explicit
`keep | replace | clear` actions. The bounded root-PATCH compatibility path also
requires that section revision. Both reject masks and client-owned
ref/configured metadata; GET and mutation responses pair the exact typed section
envelope with secret-free credential status.

The external broker loader completes/rechecks migration before reading
`broker.json`, then resolves `broker.external.bearer_token` through the credential
store. Missing, corrupt or configured-but-unbound references fail closed and fall
back to an embedded broker instead of dialing an external endpoint without its
bearer. Generic credential status/replace/clear APIs retain revision/CAS semantics;
the ordinary broker file remains metadata-only.

The two historical Copilot plaintext cache files are adopted at facade startup:
the complete credential value is committed to the encrypted store before the
unchanged legacy file is removed. A crash between those boundaries is retry-safe,
and all subsequent Copilot reads/writes use the credential authority.

## Server integration status

The server owns one `ConfigFacade` for the process and watches every modular
section. Ordinary section changes publish an immutable snapshot and update only
their corresponding live effective-config field. Provider and MCP candidates
have stronger runtime gates: provider candidates must construct the replacement
registry/default provider before publication, while MCP candidates stage every
added or changed client through connect, initialize and tool discovery before
replacing runtime-map/tool-index entries and the effective MCP snapshot. A
failure discards all staged clients, leaves working runtimes and tool aliases
intact, keeps the last-known-good revision, marks health degraded and publishes
`config.invalid`; repair publishes `config.recovered`. Directory debouncing and
missing-file retries cover editor temp-write/rename bursts.

Generic credential metadata/status/replace/clear HTTP adapters use the encrypted
store with credential-document CAS only for unowned references. Active proxy,
Env, Notifications, Connect, Access Control, and Cluster references reject that
generic mutation path and point to their domain transaction. Responses contain
only status metadata and health; conflicts return HTTP 409. Successful mutations
publish `config.changed` through the durable account feed, which also supplies
the v2 WebSocket `feed` channel.

Read-only typed provider and MCP section endpoints expose the same independent
revision/health/source envelopes used by the watcher. Their DTOs are intentionally
diagnostic projections: provider keys, ciphertext, request overrides and unknown
provider fields are omitted, while MCP transport environment/header names are
reported without values. URL diagnostics drop user info, query strings and
fragments; MCP argument values are omitted. Typed provider/MCP mutation endpoints
preserve credential references, reject new inline secret material, and persist
metadata-only sidecars. Runtime construction hydrates references from
`CredentialStore`; a missing or corrupt referenced credential rejects the
candidate with a redacted degraded/invalid transition and retains the
last-known-good runtime.

The typed section API exposes GET envelopes and revisioned PUT mutations for
ordinary non-credential sections. Provider, MCP, Env, Notifications, Connect,
Access Control, Cluster Fabric, and credentials use dedicated validated
transactions; generic `PUT /config/sections/{id}` rejects those domains so it
cannot become a second write authority. Server-owned credential-reference fields
are preserved and cannot be replaced through an ordinary DTO. Compatibility
writes are preflighted against the facade projection and rejected before the
first durable write if they change more than one section. `model_limits` cannot
be combined with another section. The legacy full-reset endpoint is likewise
rejected for an active modular layout until it has a recoverable multi-file
manifest; callers reset sections separately.

The domain mutation adapters use explicit secret intent without masks:

- Env accepts `credential_change: {action: keep|replace|clear}` per secret
  entry; omission retains the documented missing/nonempty/empty compatibility
  form. A secret-to-plain conversion requires an explicit new plain value.
- Notifications and Connect expose dedicated PUT/GET adapters whose mutation
  payloads separate metadata from `credential_change`, `token_change`, and
  `app_secret_change`.
- `POST /bamboo/access/password` requires the Access Control section revision
  and supports revisioned password `replace` and `clear`, preserving paired
  devices. The public pre-auth Access status exposes the authoritative
  revision, health and source projection but omits section data and local
  source paths; the gated password mutation returns its exact committed
  envelope.
- `POST /bamboo/proxy-auth` requires the Core section revision and returns the
  exact committed Core envelope.

Every mutation response is built from the runtime/credential snapshot captured
under the exact transaction and contains no plaintext, ciphertext, or UI mask.
Each credential field reports one explicit state: `configured` when the owned
record is usable, `from_env` when an environment source is active, `missing`
when no usable value is bound, or `error` when configured metadata cannot be
resolved safely. Credential-store revision and health remain nested diagnostics
and never become the domain mutation precondition.

Omitted or empty reference metadata preserves an existing binding (clearing is a
separate credential operation); an explicit replacement reference must parse and
resolve before runtime staging or durable commit. Root `config.json` uses a
disk-only MCP projection that removes both hydrated plaintext and legacy
ciphertext for ref-backed env/header fields. Public compatibility serialization
continues to round-trip the hydrated MCP shape.

Proxy authentication now follows the same isolated-store boundary. Legacy
`proxy_auth_encrypted`, per-scheme encrypted fields, and any legacy inline
`proxy_auth` object migrate to `proxy.default.auth` through the recoverable
credential/config manifest. Ordinary root config and rotated backups retain
only `proxy_auth_credential_ref`; runtime construction resolves and parses the
credential after migration readiness. The dedicated set/clear endpoint uses an
exact Core-section transaction, and its status and mutation responses return the
typed Core envelope plus credential status without username, password,
ciphertext, or mask values. Generic root saves refuse an unisolated proxy secret
rather than recreating legacy ciphertext.
