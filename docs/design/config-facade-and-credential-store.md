# ADR: Modular live configuration and credential isolation

- Status: accepted; persistence/snapshot foundation implemented, server adapters pending
- Issue: #597
- Date: 2026-07-19

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
| cluster fabric | `cluster-fabric.json` | hosts, paths and credential refs |
| environment variables | `env.json` | non-secret values; secret entries carry a credential ref |
| access control | `access-control.json` | enabled state and verifier records only |
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
directory `0700` and file `0600` on Unix; Windows uses the platform's file security
and is a best-effort follow-up for explicit ACL hardening. Ordinary new files honor
the process umask, while replacements preserve an existing stricter Unix mode.
Backup replacement remains atomic on Unix and uses remove-then-rename only as the
Windows fallback when the platform refuses to rename over an existing generation.

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
| ntfy token / Bark device key | `notification.<channel>.token` |
| cluster password/private key/passphrase | `cluster.<host>.<field>` |
| external broker bearer token | `broker.external.bearer_token` |
| access password/device token | verifier only; plaintext is never persisted |
| Copilot OAuth cache | stays in its existing OAuth cache until a platform keychain adapter exists; it must not migrate into ordinary config |

The store reuses Bamboo's existing AES-GCM encryption key and records a key
version for future rotation. Its public status APIs return only reference,
configured state, source and update time. Mutations are replace or clear; empty
strings and masks are not credential values. Runtime resolution returns a
non-serializable, redacted-debug wrapper.

## Legacy migration protocol

Migration must be idempotent and manifest-gated:

1. acquire the migration/advisory lock and copy parseable legacy inputs to a
   timestamped migration backup;
2. parse legacy root, broker and current sidecars as raw JSON so flattened and
   unknown fields remain attached to their owning section (unclassified fields go
   to the core section's `extra` map);
3. extract the credential inventory into encrypted records and replace ordinary
   values with references/configured metadata;
4. stage every candidate file and validate the complete candidate set;
5. fsync staged files, then atomically install a versioned manifest as the commit
   point;
6. on restart, discard an uncommitted stage or resume from the manifest; never
   infer completion from the presence of one section file.

Until that manifest migration is wired, compatibility PATCH/dot-path operations
must continue using the full effective projection and must not claim that all
secrets have moved. The server integration must add typed section and credential
status/replace/clear endpoints, event-feed adapters, watcher lifecycle ownership,
and migration fixtures before #597 can be considered complete.
