# Authorized Actor subtree snapshot v1

Issue #1337 is the first read-only delivery slice of #928 / #791. The canonical
gateway route is `GET /api/v1/actors/{root_id}/snapshot?subtree_id={actor_id}`.
Omitting `subtree_id` selects the whole Root tree. These IDs only select data.

## Authority and consistency

The browser gateway requires the existing bootstrap authentication result to be
`LocalBypass` or `Authenticated` (host owner). A remote request under the `Open`
policy remains unauthenticated for this route. Device/cookie verification and
locality retain the existing gateway rules. Codex `bcx1_` run tokens retain their
Responses/models-only permission and cannot read this snapshot.

The internal `ActorSnapshotPort` accepts a principal constructed by trusted code,
with no `Deserialize` implementation. A live Actor principal must prove the entire
current activation fence, unexpired lease, active row, and verified durable
lineage. It may select itself or its descendants. A sibling, ancestor, foreign
Root, Session ID alone, or client-supplied `requesterId` cannot grant authority.
The full live fence is checked before any children enumeration; publication then
rechecks complete lineage and lease expiry under the same held locks.

The adapter reads canonical `sessions/<root>/[children/<actor>/]` files. It does
not use the rebuildable index or call `inspect_actor`, `ensure_actor`, Session
loaders, or auto-recovery. It holds the existing lifecycle shared guard and Task
transaction exclusive guard for the entire actual read, including after a caller
cancels the request. Pending Task or copy journals reject the entire view without
repair. Main/runtime identity, Project, birth, Root proof, Supervisor proof when
applicable, every direct-parent edge, depth, and actor-row ancestor observations
must agree for the observed Main frame and separately read witnesses. Historical
missing actor row + initialization marker stays unknown;
losing one of the pair, stale observations, or inconsistent identity fails closed.

The source root and all children must fit the budgets, even when selecting a
smaller subtree. No partial tree is published. Every child directory is a candidate;
unknown entries consume the scan budget and cannot be silently skipped forever.

## Public wire contract

```json
{
  "schema_version": 1,
  "root_actor_id": "root",
  "subtree_actor_id": "root",
  "snapshot_id": "as1-opaque-equality-digest",
  "stream_cursor": null,
  "nodes": [{
    "actor_id": "root",
    "parent_actor_id": null,
    "root_actor_id": "root",
    "depth": 0,
    "title": "Public Session title",
    "role": "root",
    "logical_state": null,
    "placement_class": null,
    "revision": {
      "session_metadata_version": 0,
      "actor_directory_revision": null
    },
    "activation": null
  }]
}
```

Nodes are sorted by absolute depth and logical ID. The public title is the existing
Session title, limited to 160 Unicode characters, with control/bidi formatting
characters removed. Role is only `root` / `child`. Proven actor rows supply their
logical state, row revision, and optional safe activation UUID/attempt/status.
For host-owner views, logical state and activation status are the latest durable
record, not proof that an Actor is currently online. Heartbeat and lease liveness
remain unknown. The internal live-Actor principal additionally requires an
unexpired lease before gaining descendant read authority.
Placement exposes only a proven activation placement class, never a physical
host, endpoint, placement intent, owner, lease ID, run ID, or broker address.
Per-Session metadata version is its durable row value, not a tree revision.

Queue/wait/request/health are absent because this slice has no authoritative
public source for them. Unknown row/state/placement/activation is JSON `null`,
never synthesized `cold`, `healthy`, or zero. No private profile, responsibility,
Project metadata, environment path, credential, prompt, transcript, tool args or
results are serialized. The adapter never serializes `ActorDirectoryEntry`.

`snapshot_id` and HTTP ETag are opaque equality identities of this public view.
`If-None-Match` may produce 304 after a fresh authorized read. They are not ordered,
global monotonic revisions, event cursors, checkpoints, or replay positions.
Canonical cursor/event delivery is deferred to #929 / #930; a later Lotus Next adapter
must not treat this identity as a stream coordinate.

## Bounds and errors

Default ceilings: 256 source nodes, 4096 accumulated directory entries, 2048
attempted file reads (including absent optional files), 512 KiB per source file,
8 MiB aggregate actual reads, and 256 KiB final serialized payload. Root proof and
initialization/revocation evidence additionally use 4 KiB ceilings; Supervisor
proof uses 256 KiB. Internal callers may tighten ceilings, never raise them.
Ancillary fd reads are capped and use one counted overflow byte to detect
post-stat growth. Final payload size includes its identity. IDs are at most 256
bytes. Main uses the accepted leading compact section: the entire section,
including fixed framing, must fit the per-file and remaining aggregate budgets.
The complete Main file can exceed 512 KiB; history is never read by this observer.
Actual header and payload bytes, including partial reads, are counted once.
The already-open regular-file FD ends at the declared section boundary; no suffix,
EOF scan, growth probe, read-ahead or full-buffer fallback follows Main's close.
Runtime/proof/row/marker/revocation reads retain their complete-file limits.

## Compact observation boundary (#1339)

This view validates the exact literal v1 prefix, ten decimal length digits,
framing, closed 15-field typed payload and all readable witnesses listed above.
Unsupported legacy, moved, escaped or unknown leading encodings fail explicitly
with `unsupported_authority`; GET never prepares, repairs or upgrades a file.
Malformed/truncated recognized framing or payload rejects the whole view.
Existing full Main readers continue validating the complete JSON and matching
present compact authority against the flat fields.

A legal frame plus matching Runtime/proofs/rows is a public graph observation,
not full Main integrity. This consumer cannot detect unseen flat-only birth or
Project modifications, later duplicate members, an invalid suffix, private
transcript corruption, or arbitrary replay/tampering invisible to that frame and
its readable witnesses. Such files can yield the same public snapshot while the
full compatibility reader rejects. A snapshot/ETag does not grant any action,
file/context access or ContextRefs authority; #1343 remains a separate contract.

The actual blocking read owns lifecycle Shared then Task Exclusive guards.
Once started, caller abort or whole-runtime shutdown does not release them until
the closure ends. It does not promise a queued job starts or an entire interrupted
async transaction completes. Native fixtures preserve one 134-node tree while
only growing private history, trace actual Main FD offsets, and use started-reader
barriers with independent physical-lock/writer/reopen checks. Native execution
and exact artifact receipts are required separately from source review.

Typed static errors contain no source path or content: `invalid_selector` (400),
`not_found` (404), `unauthorized_scope` (403), `budget_exceeded` (413),
`stale_authority` / `inconsistent_authority` / `pending_transaction` (409),
`unsupported_authority` (501), `storage_unavailable` (503). Missing host
authentication is 401. The endpoint never returns partial successful nodes.

The initial reader supports macOS/Linux using capability-bound `openat`, retained
directory fds, `O_NOFOLLOW` on every ancestor/final open, regular-file fd checks,
and fd directory enumeration. Configured home must be an absolute path without
symlink ancestors. Other platforms return `UnsupportedAuthority` before acquiring
locks or reading authority. Windows handle reading is tracked separately in #1338;
full cross-platform delivery remains in #928 / #791. Native validation for this
slice is performed on macOS; Linux runtime verification remains a separate gate.

No frontend, live subscription, replay, history, ParentRequest/Inbox, remote
placement protocol, or new persistent global revision/journal is introduced.
