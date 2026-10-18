# Durable Ready retry boundaries

Follow-up to #2051, covering a bounded part of #2042 B and #2046. Existing
Start/CAS, Assignment, DispatchIntent/RuntimeReceipt, generation/epoch, stop
confirmation and resource claims remain authoritative. No Ticket schema, HTTP
wire shape, Task, lease, execution budget ledger or provider loop is added.

The server assembles the existing `ReadyWorkScheduler` with an outer
`FileReadyRetryStore` and a narrow absolute clock port. Startup still installs no
policy. A newly constructed scheduler remains disabled even when retry files
exist. An explicitly installed in-memory Host policy and all existing live
authority, Project/context, workspace, capacity and native-budget checks are
required. The legacy `Default` embedding retains its process-local behavior;
the production server uses the durable assembly.

## What is persisted

The stable key is `(scope incarnation, Work ID)`, not a changing epoch, cap or
policy nonce. Each record binds a hash of the complete Supervisor binding,
Project, Work contract/context/workspace and round cap. Changing those inputs
cannot create an empty retry budget for the same key. Stored hashes and receipt
references constrain retries; they cannot reconstruct an execution grant.

Records contain a revision, consumed attempt count, first absolute deadline,
last observed clock floor, earliest `next_check_at`, typed diagnostic and the
original operation/request hash or dispatch key/spec hash. No wall-clock value
is inferred from Ticket sequence numbers or Session range indexes. Reinstall
does not extend the first deadline (at most one hour). Backward wall-clock
observations fail closed; monotonic policy expiry is checked separately.

Every dispatch/ensure first uses a durable CAS reservation, including the first
attempt. At most three such calls are permitted for a Work key. Cancellation,
lost reservation replies and uncertain writes do not refund a consumed count.
Transient retries wait at least 1s then 2s; resources and observed-but-not-yet-ACKed
admission wait 5s. A resource-only preflight does not consume an execution
attempt. Notifications cannot bypass `next_check_at`; the existing coalesced
five-second reconciler repairs missed notifications. These are earliest check
times, not an exact wake-up SLA or a cumulative monetary budget.

The Host carries a nonserialized `ReadyAdmissionFence` through its internal
awaits, checking again before canonical Start, prepared-Child creation and the
existing Child admission call. Already admitted execution keeps the existing
round cap, shutdown, stop/ACK and resource protocol; the retry deadline does not
assert that an admitted worker has stopped.

## Crash and failure decisions

| Observation | Decision |
| --- | --- |
| No Host policy after restart | Disabled; no provider/admission call |
| Backoff still pending | Return persisted deadline/count/reason; no preflight/admission |
| Definite synchronous scope CAS rejection | Keep the charge and deadline; fresh full eligibility before another selection |
| Start ACK/result lost with matching request hash | Recover Assignment from that canonical receipt; ensure only its original key/spec under the same current epoch/generation |
| Interrupted Start without matching canonical receipt | Quarantine; do not guess an Assignment or issue a new Start ID |
| Runtime outcome unknown | Quarantine; claims and uncertain effects remain held |
| Ticket writer reopened | Old execution becomes OutcomeUnknown under Ticket rules; no automatic readmission from retry bytes |
| New explicit policy for never-admitted generation-zero Work | May use the new epoch only when no request was reserved; preserve old count/deadline |
| Store corrupt, missing, full, denied, or CAS unconfirmed | Fail closed before admission; do not silently replace with an empty budget |

A per-Work port failure is reported for that Work so another healthy grant can
continue. A corrupt shared file fails the whole storage adapter closed. Neither
case invokes Supervisor implementation or expands its tool authority.

## Outer file adapter

`<data-root>/ready-retry-v1/state.json` is bounded to 1024 entries and 8 MiB,
with version/checksum/record validation. Its stable lock inode is outside the
renamed JSON file. Bootstrap and later CAS transactions acquire that same fs2
lock before directory creation or reads. Temporary bytes and the file are
synchronized before replacement; the containing directory is synchronized
before a successful reply. Errors at every boundary propagate. Blocking writes
retain their lock until completion even if the async caller is cancelled.

A missing file in an initialized directory, an unknown schema, invalid record,
checksum mismatch or symlink is not an empty queue. No live, exhausted or
quarantined record is evicted to make capacity. Automatic metadata repair and
safe compaction are deferred; exhaustion requires explicit Host review.

## Remaining scope

This slice does not implement cumulative cross-run Work accounting, fairness,
priority, replacement workers, automatic workspace repair, generation-two
continuation, preemption, production semantic Patrol/Jiandu adapters or health
HTTP. Patrol owns separate observation/checkpoint files and incident identities;
Ready does not consume those as execution facts. Missing receipt, progress or
stop proof remains unknown rather than a reason to manufacture new permission.

Validation uses real disposable files and TicketService fixtures, injected
virtual clocks, and the existing controlled native provider. Source-bound
receipts and actual results are recorded in the follow-up Draft; written tests
alone are not a passing result.
