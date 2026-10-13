# Ordinary Root tool-authority proof (#1323)

This V2 storage boundary protects `root_orchestration_only` and its monotonic
`root_tool_authority_revision` without reading the full transcript on an
Ordinary Root control-plane load or steady runtime-only save. The default
Supervisor still checks its separate management authority against the main
file; a bounded Supervisor proof is tracked in #1324.

## Durable files

Each Root has `session.json`, `runtime.json`, and a small
`root-tool-authority.json`. The proof records its schema version, Root ID,
creation time, authority identity, mode, revision, and a `prepared` or
`committed` state. It binds the Root's tool authority, not mutable Task,
Project, or conversation data. Missing, corrupt, oversized, prepared, or
sidecar-mismatched proof is a recovery conflict.

Under the existing cross-process per-Session writer lock, a new Root or mode
selection publishes these durable states in order:

1. `prepared` proof for the new tuple;
2. `runtime.json` for the new control plane;
3. `session.json` for the complete snapshot;
4. `committed` proof for the same tuple.

Every intermediate crash leaves the Root unavailable. A failure after step 4
but before index publication leaves a complete canonical Root; ordinary
recovery can rebuild the derived index. A full save that keeps the same tool
tuple preserves the committed proof and the existing sidecar-first ordering.
Root copies, recreation, and Supervisor bootstrap write their committed proof
inside an unpublished staging directory before its atomic rename.

Control-plane reads check a regular main-file entry, then compare the bounded
sidecar and committed proof. Full Session reads and full saves still parse the
main file, so malformed history cannot be silently overwritten. A later
corruption of main-file contents cannot be detected by a bounded
control-plane read; full reads report it. Indexless cold runtime repair also
verifies the complete canonical pair before publishing an index entry.
An indexless miss with a retained revoked Root directory uses the revocation
marker, runtime identity, and committed proof; it does not parse the main
transcript merely to report the deleted Root as absent.

## One-time upgrade

Before Task and copy journal recovery, startup scans legacy Root pairs while
holding the existing exclusive recovery gate. It creates a committed proof
only when both files parse fully, have the same physical identity, and agree
exactly on tool mode and revision. A sidecar ahead of main may be an
interrupted explicit disable and is never promoted. Startup scans again
after journal recovery and durably writes a global migration marker last.
Later proof loss cannot trigger automatic reconstruction. Legacy main-only
Roots remain unavailable until their canonical authority is repaired from
independent evidence. Child fallback is unchanged.

The protocol covers ordered writes and ordinary crashes, including an old
runtime sidecar being restored while the committed proof survives. No single
replaceable proof can detect an adversary rolling back the proof, migration
marker, and Session files together; that requires a separate monotonic trust
anchor. The proof also does not replace the Supervisor management check.
