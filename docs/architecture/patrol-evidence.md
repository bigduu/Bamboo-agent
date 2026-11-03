# Unmetered patrol evidence policy

`bamboo-orchestration::patrol::evidence` owns the evidence data contract and
deterministic `EvidencePlan`. It consumes canonical Ticket snapshots and Domain
Project values; it does not obtain authority, read files, start a server or call
a model. A plan is data, not a retained read or execution grant.

`EvidencePlan::prepare` checks snapshot/Scope consistency and selects the
Assignment's adopted context through its immutable DispatchSpec hash. A newer
candidate Work context does not replace it. Coverage is `bounded_snapshot`;
the since/through cursors do not establish complete delta/event history.

The core validates Project resource declarations and bounds source output to
8 KiB, the complete response to16 KiB and total references to8 (two fixed plus
at most six source selectors). Missing, unsupported, unresolved or oversized
sources do not report all sources matched; an empty source set also remains
unmatched. No content is clipped into a misleading exact-version result.

Native reference/status values are opaque generic data payloads. The core has
no dependency on Jiandu, MemoryStore, AppState or HTTP and does not duplicate
native version/schema validation. A concrete Host adapter must validate native
identity and freshness before projecting any matched source into the plan.
Matching content versions establish consistency, not truth, liveness, recovery
or Work acceptance.

The result has no provider/model, token estimate or budget reservation and no
automatic conversion to `DiagnosticInput`. Protocol metering and canonical Work
budget admission remain required before any later provider submission.

Pure tests run with `cargo test --locked -p bamboo-orchestration --test
patrol_restart`, reusing one canonical Ticket fixture and a single child module.
Its controlled runtime receipts are not native Host/worker execution evidence.

The concrete Host/Jiandu/physical Project reader and real Task/cold-reopen
acceptance are the dependent [#2508](https://github.com/bigduu/Bamboo-agent/issues/2508)
slice. This core-only delivery does not activate semantic patrol or change
existing program patrol, persistence, providers or installed applications.
