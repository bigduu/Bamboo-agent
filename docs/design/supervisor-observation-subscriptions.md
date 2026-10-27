# Durable Supervisor observation subscriptions

Issue #2300 completes #1056 using canonical Observer registration, immutable
context views, SessionInbox and native Child activation. A trusted Host retains
Creator's original read-only Child ID (`auto_run=false`) and calls
`subscribe_supervisor_observer(ObserverSubscriptionStart)`: register-only,
persist configuration, then activate through the first actual Inbox notice.
It never calls Observer start/enqueue separately; native capability gates remain.

Canonical Supervisor schema 3 persists filter, interval (1 second–1 hour), exact
registration epoch, bounded producer window and visible retry state. Empty
fields preserve original schema 1/2 bytes; older writers reject schema 3. Window
targets must belong to the registration's fixed maximum-eight links. Each stage,
delivery and derived read verifies parent/Child births, incarnation, epoch,
Project and enabled links. Installed/revoked Host namespaces never fall back;
revoked registrations remain tombstones and exact retries cannot re-enable them.

The existing wake tick inspects one Observer and at most eight source Roots.
Actual pending/claimed Inbox items or an active native run coalesce new changes.
Existing provider/tool/token/concurrency budgets still apply. Enqueue receipts
are neither consumer ACK nor model completion, and ordinary Work/Planner waits
remain under the original completion coordinator.

One safe source snapshot supplies fixed status/brief files and opaque meaningful
fingerprints. Status includes execution ID; needs-input hashes question ID.
Tasks include title and every item's ID/description/status, even after the 32
rendered previews: a meaningful change can leave displayed previews identical.
Notes, evidence, token counts and timestamps do not trigger changes. Transcripts,
question text and source paths never enter notices. Strict identity checks may
read Main; only fixed view files have the existing 8/16/8 KiB read limits.

Actual view publication and retained commitment/provenance verification precede
canonical pending. Pending freezes its selector across later source changes.
The typed notice targets only its registered Observer; generic Inbox delivery
cannot mint it. Complete authority guards survive physical Maildir jobs and
async cancellation. Only the integrated adapter's actual receipt advances the
producer cursor. Crash-after-enqueue retries the same event through Inbox dedupe.

After enqueue, candidate history files publish before cursor commit. Publication
failure preserves pending. The last 16 references derive at most four four-entry
JSONL shards and a hash manifest under runtime-derived
`coordination/observer-changes/v1/<observer-hash>/<selector>/`. Retained directory
FDs, no-follow opens, fsync and atomic rename protect Linux/macOS publication;
other platforms fail closed. The explicit 64-version quota never silently deletes
old snapshots. Files are derived enqueue references, never another queue/ACK truth.

`read_supervisor_observer_changes(reference, child_id, epoch, after)` checks fresh
scope and verifies/reconstructs the physical bundle. Corruption/symlinks fail;
a rotated cursor returns `gap=true` plus assigned targets requiring fresh scoped
snapshots. Wrong epochs/future cursors fail. A selector, JSONL or receipt-shaped
DTO grants no authority. Failures persist a non-sensitive code with bounded
backoff; eight failures block visibly, and exact retries cannot reset the cap.
The ordinary Work fixture does not claim Planner #1059 or full Epic acceptance.


## Terminal fault reporting (#2314)

A terminal observation failure freezes one typed diagnostic inside the same
subscription: Supervisor incarnation, parent/Child births, registration epoch,
original error code/count and exhaustion time bind its stable event ID. A
nonempty diagnostic upgrades Supervisor schema to 4; absent fields preserve
schema 3 bytes, and later registration/subscription writers retain the maximum
schema. Delivery failures cannot replace the original payload or retry ID.

The Host delivers `observer_subscription_fault_v1` to the original Supervisor's
canonical Inbox with `RespectSpecificWait`. It announces no available file,
contains no target content/path and is not a ChildOutcome or control grant.
Diagnostic admission requires the current native parent/Child registration and
scope epoch, enabled original links and retained Host namespace absence. It can
report its own failure when a physical source Root is unavailable. A rejected
Host grant permits only a private canonical delivery-failure counter update,
never target reads or Maildir admission. Revoked registrations cannot send.

The integrated adapter retains the complete authority through actual Maildir
jobs. Only its actual receipt marks `reported`; a crash before this checkpoint
retries the same semantic event. `reported` is producer enqueue truth, not
consumer ACK. Fault delivery has its own eight-error finite exponential backoff;
exhaustion remains visibly unreported and exact retries never reset it. Inbox
or canonical storage failure is never represented as a successful notification.
A deleted/replaced Child likewise permits only the original Supervisor's bounded
failure counter update; it cannot borrow the replacement's lifetime or authority.

## Original Epic composition and test ownership

The canonical feature executor owns these ordinary, nonignored default library
tests; they add no feature-only catalog entry:

- `bamboo-engine`: `runtime::runner::loop_execution::pipeline::tests::partial_child_outcome_can_drive_tool_work_before_resuspending_exact_inherited_wait`
  and `runtime::runner::loop_execution::pipeline::tests::supervisor_new_orphan_wait_keeps_terminal_reply_deferred`.
- `bamboo-server`: `app_state::supervisor_epic_acceptance_tests::two_project_tracker_planner_followup_and_restart_preserve_original_authority`.

The two engine functions cover eight role/tag/buffering arrangements and two
new-orphan negative arrangements. Those are two test functions, not ten. The
Host function composes all original seven rows using canonical Store, FileInbox
and Host with controlled provider input: two Project Roots, a finite Tracker,
an actual held read-only Planner, immutable plan sections, one original-worker
followup and cold restart. Supervisor uses current-user discovery without
manual grants; Tracker does not inherit that global management scope. Reports
may drive an intermediate Supervisor reply but must preserve the entire old
Planner wait, USER input and the Planner's completion obligation.

`supervisor-epic-acceptance.yml` is an explicit manual diagnostic for a full
40-character source SHA. It preserves the original API-only recipe, exact
binary/case/count checks, strict engine/Host library lint and clean source/lock
guards. It supplies neither a second automatic Linux test owner nor required
merge-source evidence. A manual source must not be relabeled as canonical M.

Historical source `71f045267544e65851b550fc78bb472260fd3c25`, tree
`b2c6ee19ca139ab57b961da3de17bd8a8130413b`, actually passed both engine functions
and the complete Host function (3P0F0I), strict lint and source guards in
[run40472740573/job122083508051](https://github.com/bigduu/Bamboo-agent/actions/runs/40472740573/job/122083508051).
That result preserves the original API-only source/configuration identity; it
does not establish required canonical feature acceptance for a later head/base.
The distinct 19 additive feature cases and the 24 subscription/fault API-only
executions must never be conflated. CI configuration ownership remains #2338.
