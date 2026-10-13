# Default Supervisor authority proof (#1324)

The fixed default Supervisor stores a bounded `supervisor-authority.json` beside
its canonical `session.json`, `runtime.json`, and Root tool proof. The new file
binds the Session ID, birth time, incarnation, Root tool selection/revision, and
the complete bounded management state (Project scope, links/tombstones and
revision). It is a canonical authority fence, not a directory or a second
management writer. The management state remains in `runtime.json`.

## Operational reads

`load_root_authority`, Supervisor scope/link reads, followups, and management
mutations acquire the existing lifecycle, Task, and Session locks. They parse
`runtime.json`, check the physical Root placement and regular main file, then
require committed Root tool and Supervisor proofs matching that runtime
snapshot. They do not read conversation bytes from `session.json`. After a
same-ID recreation, the durable revocation cutoff is compared with the proved
birth time, rather than scanning the main transcript. Full Session loads and
full saves still parse `session.json` and verify its identity and management
overlay against the sidecar.

## Publication and recovery

- Bootstrap stages the complete main/runtime/proof set before publishing the
  directory and index. Copying a Supervisor still creates an Ordinary Root and
  never copies Supervisor authority.
- A management mutation durably publishes `Prepared` proof, then the new
  `runtime.json`, then `Committed` proof. A full save coordinates this order
  with the Root tool proof and writes the main before committing both proofs.
  Any interrupted intermediate phase denies operational authority. If the
  final commit succeeded but acknowledgement failed, a read after restart
  observes the committed revision; the caller should reload before retrying.
- The existing Task journal changes Task fields only. Startup performs a
  one-time Supervisor proof upgrade before Task recovery, then checks for a
  newly recovered pair before publishing the durable migration marker.
  Thereafter a missing, corrupt, pending, or stale proof is never regenerated.
  An old pair is upgraded only after full main/runtime parsing, Supervisor
  overlay validation, and existing Root proof validation. An old runtime
  management revision ahead of main is valid when it satisfies the overlay
  rules, as management updates historically wrote the sidecar alone.

The proof is bounded to 256 KiB; management capacity is at most 64 Projects
and 256 links. Its per-operation cost is independent of transcript length.
The protocol handles interrupted local writes and stale single-file state. It
does not claim to detect coordinated rollback of all canonical files and the
migration marker. Repairs of an interrupted `Prepared` management publication
require explicit recovery from trusted prior evidence; ordinary writers cannot
guess or silently roll it forward.
