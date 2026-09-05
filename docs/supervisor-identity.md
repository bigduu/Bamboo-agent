# Trusted default Supervisor identity

Bamboo provides a host/SDK bootstrap service for one stable Supervisor Root in
each local data domain. It establishes identity; management links and control
of other Roots are separate capabilities tracked by #1071 and #1051–#1058.

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
`ErrorKind::Unsupported` for both new ports, without ordinary load/save fallback.

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

This API is for trusted in-process hosts. It is not a model-callable bootstrap
tool or a new HTTP route. It does not provide a Project allowlist, management
relationships, cross-Root reads, followups, cancellation, Tracker subscriptions
or Plan delegation. Those operations must use their subsequent trusted authority
checks; neither raw metadata, cached role labels nor a bootstrap receipt replaces
them. Like the existing Session store, this is not an OS sandbox against arbitrary
modification of the data directory by the same operating-system user.
