# Owned input-request data scope

The Engine lifecycle module exports `scope_input_request_data` and
`with_scoped_input_request_data`. The main pipeline now scopes its one owned
batch through metadata, provider retries and Tool execution, then recovers the
same owner before the next boundary. The defaults SDK borrows this data through
its private current-run/current-User resolver; Q itself supplies no authority.
Server, child and deployed-worker Skills integration remains pending under #1595.

The scope accepts an existing `Option<BoundedInputRequestBatch>` and a future.
It moves the unique batch into Tokio's task-local scope and lends it during
each poll. A synchronous reader receives a shared reference only when both
supplied Session and execution IDs exactly match the batch. Empty IDs, absent
data and mismatches return `None` without calling the reader. `None` in an
inner scope shadows an outer batch. A returned reference or a future borrowing
the batch cannot escape the closure.

Normal completion returns the future's output and the original batch through
the supported `TaskLocalFuture::take_value` API. A normal `Result::Err` also
returns ownership. Cancellation, drop or panic restores the surrounding scope
and drops the cancelled work's owner. That work cannot continue using recovered
data. Unwrapped `spawn` and `spawn_blocking` tasks do not inherit data; moving a
whole scoped future transfers its own scope when the future satisfies `Send`.
Inline futures have no added `Send` requirement. Inline parallel executor
futures borrow one batch, while the existing Core dispatch scope supplies each
call's separate output-cap observation.

This scope neither classifies input as New nor authenticates a caller. A raw
projection, matching ID or unsealed SDK input remains untrusted data. Source,
mode, schema, disabled state, host ceilings and tool permissions must be checked
independently at actual use. Holding a scoped batch is not a Source lease and
does not delay revocation. There is no persistent index, grant cache, second
writer, public Core layout change, broad snapshot copy or whole-batch Clone.

The pipeline applies the existing `InputObservation::update_current`
at each accepted round boundary. Same-execution successful NoNew retains the
owner; New replaces it, including a new input without any request; unavailable
or foreign observations clear it. Recover the scope before the next boundary.
Do not poll another scoped future while a synchronous reader holds its borrow.
The defaults SDK connects the actual caller, preappend F, final User checkpoint
and Reader while disabling its old Instruction gates. Its direct String append
order is preserved; session-only execution and resume cannot reconstruct a new
invocation from history. The remaining official host surfaces and complete
atomic switch stay open under #1595.
