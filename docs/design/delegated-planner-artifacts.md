# Delegated Plan: selected context and versioned results

`Plan` keeps the #1028 read-only Child and existing parent wait. With an explicit
`context_packet` v1 it uses the already supported fresh local Bamboo worker
route. Unsupported routes fail before Child persistence. The parent role,
permission mode, Project, and ordinary orchestration posture do not change.
Legacy task/fork calls keep their existing result path.

The selected-context planner returns the strict ChildReport v1 JSON plus one
`plan_markdown` member (64 KiB maximum, beginning with a heading). The complete
JSON report has an 80 KiB cap; the compact resume retains its separate 8 KiB cap. It does not call Write.
The runtime validates the committed Child completion seal, assignment, selected
source contents and parent/Child lifetimes before publishing. Evidence and
verification in the report remain model claims; publication grants no execution
approval.

The complete report remains in the existing terminal Session commit. A derived,
immutable `plan/delegated/<revision>.md` materialization includes the plan,
reported evidence and open decisions. Its revision binds the source seal,
parent birth, assignment and report. Existing PlanStore heading indexing is
reused. This adds no outcome queue, authoritative store or recovery lifecycle.
The ordinary completion coordinator publishes before durable Inbox delivery,
including recovery of an already terminal sibling. Replayed delivery retains
the original completion identity and cannot create another Child.

A trusted boot watchdog can persist a failed terminal outcome without a report
seal when the worker disappears. For the same owned Child and canonical failed
status, that outcome resumes the existing parent wait with a bounded
`available:false` result. It exposes no unsealed text, evidence, read selector or
artifact file, and does not claim execution approval or successful completion.
A source-less success still fails the report seal check.

The automatic resume contains only a bounded artifact reference and summary.
Call `Plan` with its `read` object to get section IDs; add `section_id` to read
one section. Reads require the exact Child birth, assignment digest and revision,
revalidate canonical parent/Child snapshots after IO, reject corrupt bytes, and
paginate UTF-8 content within a serialized result budget. A later result or a
changed selected source makes an old selector stale. A missing derived file can
be reconstructed only from the same valid canonical completion.

The no-follow directory-FD file publisher supports Unix. Other platforms reject
this selected-context artifact route before creating a Child; legacy Plan is
unchanged. The first native acceptance target is Linux. Neither component tests
nor compile-only Windows CI constitute native Windows acceptance.

Acceptance is finite: existing Plan contracts plus selected-context positive and
denial components; nine artifact/completion/recovery tests; and two actual CLI
Host/current-exe worker tests using only a bounded loopback fake provider. The
healthy case must prove source Read, denied Write even with a permissive parent,
one Child, automatic parent resume without the full plan, partial section read,
and the same immutable revision after a real Host restart. The failure case
kills the actual Host while the Planner model request is in flight, then proves
boot recovery resumes the parent with an unavailable result, no artifact and no
worker redispatch. Existing #2294
authority evidence is retained separately; no old run is relabeled for this
source. The encompassing #1048/#1968 Epics remain open.
