# Root deletion and explicit recreation

V2 storage retains canonical revocation evidence when a Root is deleted. An old
full or runtime snapshot cannot recreate that Root through ordinary save, even
when its directory and index entry are gone or another process has restarted.
The existing Root context fence still checks birth, Project and metadata revision
when a live directory exists.

The lifetime cases have distinct boundaries:

| Operation | Result |
| --- | --- |
| First full save of a never-deleted ID | Creates the Root as before. |
| First runtime save with no persisted target | Falls back to full creation as before. |
| Retry of an interrupted initial full save | May finish an empty creation layout or the exact existing runtime birth/context; it cannot advance a partial pair's context. |
| Ordinary full/runtime save after deletion | Returns typed `SessionAuthorityConflict` before writing files, index, or search work. |
| Trusted explicit same-ID recreation | Constructs a blank Ordinary Root with a host-generated birth newer than every revoked lifetime. |
| Retry after complete recreation publication | Strictly validates the complete pair and returns the surviving Root, preserving its model and history. |

Trusted hosts can recreate a deleted Ordinary ID through the Storage port exposed
by the SDK:

```rust,no_run
use bamboo_sdk::Agent;

async fn recreate(agent: &Agent, deleted_id: &str) -> std::io::Result<()> {
    let root = agent.storage()
        .recreate_root_session(deleted_id, "configured-model")
        .await?;
    println!("{} {}", root.id, root.created_at);
    Ok(())
}
```

The operation takes an ID and initial model, constructs the Session itself, and
does not accept a snapshot or caller-selected birth marker. It is an in-process
host port, with no model tool or new HTTP route. Normal HTTP creation uses fresh
IDs and retains its existing idempotency contract. The reserved default Supervisor
uses its separate trusted bootstrap port, which also assigns a fresh birth after
deletion. Unsupported Storage implementations fail before mutation.

Deletion acquires the existing exclusive lifecycle and Task locks, reads birth
markers from canonical main/runtime files, then atomically writes and synchronizes
`.root-revocations/<id>.json`. The record contains the ID, format version, and
greatest revoked birth. It records only deletion evidence: active sessions and
Supervisor management grants have no parallel registry. The cutoff never
decreases. Recreation chooses the later of the current clock and one nanosecond
after that cutoff; timestamp exhaustion is an error before publication.

Revocation publication is the logical deletion point. Physical directory removal
and derived index removal follow it, with removal errors propagated. A failure or
crash after revocation can leave files or index rows behind, but strict authority,
history, runtime, copy, migration and index-rebuild paths cannot restore the
revoked Root's authority. Remaining children are hidden while that Root is
revoked. Startup reconciles stale derived index rows from the same revocation
evidence. Damage confined to one Root hides only that Root and its children from
the derived index; reconciliation repeats after a required rebuild so compatibility
recovery cannot restore those rows while healthy Roots remain available. A retry
or trusted recreation can remove a remaining revoked directory.

Recreation and Supervisor bootstrap publish a complete main/runtime pair through
staged-directory rename under those same locks. A crash before publication leaves
no new authority; unpublished staging directories are inert. A crash after pair
publication but before index publication preserves the new lifetime, and retry
repairs the index. Ordinary save callers still use the existing per-session and
Task locks, so deletion cannot race the final writer check or a filesystem commit.

Revocations live outside `sessions/` and survive the development `dev_reset`
operation. Reset first records the canonical Roots it removes, including Roots
missing from a stale local index. This prevents surviving processes from saving
old snapshots after a history reset. Cleanup uses the same Root deletion boundary.
Revocation evidence is not automatically pruned while old snapshots may exist.

Existing corrupt, mismatched, unreadable, or non-regular revocation evidence fails
closed. As with all canonical Session data, arbitrary removal or rollback of
canonical files by the operating-system user is outside the storage trust
boundary; no second index reconstructs deleted revocations. Deletions completed
by older Bamboo versions before revocation evidence existed cannot be recovered
retroactively. Unreadable canonical birth files must be repaired before a new
deletion can safely establish their cutoff.

The typed writer error also prevents the existing runtime persistence callbacks
from publishing a rejected snapshot to caches or events, including callers that
normally publish runtime state after an unrelated I/O error. A cached role, old
snapshot, or recreated ID is not management authority; Supervisor controls must
read the strict current Root and bind to its new birth/context.

This boundary does not implement Supervisor relationships or control tools, and
does not add generic Child or Task incarnations. In particular, it does not claim
to distinguish an old child snapshot from a newly created child after the parent
Root has been explicitly recreated; that requires its own lifecycle contract.
