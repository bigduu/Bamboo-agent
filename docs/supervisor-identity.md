# Trusted default Supervisor identity

Bamboo provides a trusted host/SDK service for one stable Supervisor Root in
each local data domain. It establishes identity and manages explicitly scoped
links to independent Ordinary Roots. Commands such as followup and cancellation
remain separate capabilities tracked by #1051–#1058.

```rust,no_run
use bamboo_sdk::Agent;

async fn supervisor(agent: &Agent) -> std::io::Result<()> {
    let identity = agent.supervisor_sessions()
        .get_or_create_default("configured-model")
        .await?;
    println!("{} {}", identity.session_id, identity.incarnation_id);
    Ok(())
}
```

Hosts can also construct `SupervisorSessionService::new(agent.storage().clone())`.
Both entry points use the same canonical Storage port. Bootstrap does not call
the model, launch an agent, inherit the SDK's Project/workspace, or replace a
Session cache entry. `initial_model` is used on first creation only; repeat calls
preserve the existing model, history and other context.

The receipt contains only `session_id`, `incarnation_id` and `created`. Keep the
incarnation with the ID: explicitly deleting and recreating the default Root
produces a new incarnation. A receipt is an observation, not a transferable
grant to inspect or control another Session.
Root deletion also retains [canonical lifetime revocation evidence](root-session-lifetimes.md),
so ordinary full/runtime snapshots cannot restore a deleted Root while its ID
is absent. Trusted Supervisor bootstrap publishes a fresh birth after deletion.

`Session.authority_identity` is a typed `Ordinary` or
`Supervisor { incarnation_id }` value, separate from Root/Child kind and raw
metadata. Old serialized sessions default to Ordinary. Normal create, Chat,
metadata PATCH and all Child constructors do not assign authority. Copies are
Ordinary Roots; workers, residents, Guardians and nested children remain Ordinary.

The reserved ID is `bamboo-default-supervisor`; possession of this string is
not authority. If an ordinary Session already occupies it, bootstrap returns an
explicit conflict and preserves that Session. The host must resolve that
conflict through its normal Session management policy; bootstrap never promotes
or deletes the existing conversation.

Cold bootstrap also checks canonical child placements on disk, so a missing or
stale index cannot make an occupied child ID available. This scans Root directories
only when no default Supervisor Root exists; ordinary repeat calls avoid the scan.

The V2 implementation publishes a complete `session.json`/`runtime.json` pair
through one staged-directory publication. Existing lifecycle, Task and per-Session
cross-process locks serialize it with ordinary writers. The session index remains
rebuildable; a complete published identity can repair its missing index entry.
There is no second identity registry, singleton pointer or recovery journal.

`Storage::load_root_authority` is the strict Root control-plane port. It returns
no messages and must never be used to replace a full conversation in a cache.
Absent and damaged published state are distinct: missing/corrupt/mismatched
Supervisor authority fails closed, including during repair and writeback. It
does not recover authority from an older `session.json` when the runtime sidecar
is unavailable. Pair validation still reads the canonical main file's bytes;
the control-plane return type does not promise partial or constant-size disk I/O.
Ordinary sessions outside the reserved Root keep their existing compatibility
reads. Unsupported Storage implementations return
`ErrorKind::Unsupported` for identity and management ports, without ordinary
load/save fallback.

Merge/save adopts durable identity into an Ordinary snapshot of the same Root
(matching creation time) before committing; it does not rebind a different Root.
The final full/runtime writer rejects mismatching identities with a typed
`SessionAuthorityConflict`; it never silently substitutes identity in an internal
copy. This also rejects an explicit different Supervisor incarnation, so a
snapshot from a deleted incarnation cannot overwrite a recreated Root. If
bootstrap wins a concurrent first save, that stale save fails without publishing
its rejected identity to a cache. Unrelated I/O failures keep their existing
runtime publication behavior. Task writes, migration, clear, copy and recovery
must respect the same authority integrity boundary.

## Trusted Project scope and links

The same service exposes `inspect_scope`, `configure_project_scope`, `attach`,
`detach` and `inspect_link`. These are trusted in-process host operations; there
is no model tool, HTTP self-grant route, automatic scope inheritance, session
directory or cross-Root history export.

```rust,no_run
use bamboo_sdk::{Agent, SupervisorReference};

async fn attach_existing_root(agent: &Agent, target_id: &str) -> std::io::Result<()> {
    let service = agent.supervisor_sessions();
    let identity = service.get_or_create_default("configured-model").await?;
    let supervisor = SupervisorReference::from(&identity);
    let observed = service.inspect_scope(&supervisor).await?;
    // The host chooses this complete set from its trusted authorization policy.
    let projects = ["host-authorized-project".parse().expect("valid Project ID")].into();
    let configured = service.configure_project_scope(
        &supervisor, observed.state_revision, projects,
    ).await?;
    let attached = service.attach(&supervisor, configured.state_revision, target_id).await?;
    let observation = service.inspect_link(&supervisor, target_id).await?;
    assert!(observation.authorized);
    service.detach(&supervisor, attached.state_revision, target_id).await?;
    Ok(())
}
```

Scope defaults to empty, including for existing Supervisors. The Supervisor's
own Project, labels, raw metadata, workspace path and caller-supplied Session IDs
never grant Project access. The host scope accepts at most 64 typed Project IDs,
using the existing Project parser's 64-byte bound and path-safe alphabet.

`Session.supervisor_management` is separate from `authority_identity`. Its schema
version is 1, and its persisted incarnation must match the canonical Supervisor.
A missing field means empty scope, no links and revision zero; persisted state
has a positive revision. Only the management CAS port changes it. Ordinary
constructors and copies have no state, and children never inherit it. Canonical
Supervisor `runtime.json` owns updates; later full saves may checkpoint the same
state into `session.json`. There is no relationship registry or target-side grant.

Each link binds the target's exact ID, `created_at`, typed Project and
`metadata_version`. Attach requires a current complete independent Ordinary
Root in the configured Project set. It preserves both target files, including
identity, lineage, Project, workspace, model, permissions and history. A Project
A-to-B-to-A change or deletion/recreation invalidates authorization. Unrelated
metadata revision changes conservatively invalidate it too: the host must inspect
current state and explicitly attach again to revalidate that target.

State revision and each link's own revision increase on changes. Detach disables
a link but retains its tombstone. Removing a Project disables all its enabled
links; regranting the Project never revives them. Explicit attach creates a fresh
binding with the next link revision. A maximum of 256 link entries **including
disabled tombstones** is retained per incarnation. Entries are never evicted;
at capacity an existing entry can be reattached, but another target ID is rejected.
Target IDs also have a 256-byte bound and obey the storage path-safety rules.
Unknown schema versions, malformed identities, invalid scope/link state and
regressing or divergent overlays fail closed before authority use or publication.

Every mutation requires an expected state revision. Independent stores racing
with the same revision cannot both change state. A stale request returns
`ErrorKind::WouldBlock` even if its intended result has since been achieved.
The caller must reload scope and explicitly invoke the operation again with the
new revision; an already-satisfied fresh request returns `changed: false` without
writing. This is idempotent desired-state behavior, not exactly-once command
deduplication. Exhausting either `u64` counter rejects any required increment
with `InvalidInput` before publication; a satisfied no-op can still succeed.

V2 acquires lifecycle shared, Task shared and lexically ordered Session file
locks. It privately reloads canonical Supervisor identity/state and, for attach
or an enabled observation, the target record. These locks remain held through
the durable atomic Supervisor sidecar replacement. No public loader is called
from inside that locked path. Detach and scope revocation need only verified
Supervisor state, so an absent or damaged target cannot prevent revocation.
Disabled or missing links return `authorized: false` without requiring target
authority; damaged enabled target authority returns an error, never authorization.

Ordinary full/runtime writers reject any management-state mismatch. Existing
merge/save paths adopt canonical state into the caller's own snapshot only for
the same Root birth and Supervisor incarnation. Task CAS also rejects a staged
observation when management state changed before its final writer fence: no
Task commit or staged publish callback runs. Conditional wrappers return false;
the unconditional wrapper returns `WouldBlock`. A fresh caller invocation may
retry. This protects the public staged callback observation; production repository
Task callbacks continue to patch only Task fields.

Receipts and observations contain bounded identity/revision/link fields, never
a history-free Session to install in a full conversation cache. `inspect_link`
proves authorization only while its locks are held. The returned boolean is an
observation, not a retained grant for a later command. Future #1051 command
admission must retain a final relationship/target authorization fence through
durable inbox acceptance; this link API does not solve that later race.

Followups, cancellation, Tracker subscriptions and Plan delegation still need
their own trusted admission checks. Like the existing Session store, this is not
an OS sandbox against arbitrary data-directory changes by the same OS user.
