# Durable Supervisor observation subscriptions

Issue #2300 completes #1056 over the existing canonical Observer registration,
immutable context views, SessionInbox and native Child activation. There is no
second mailbox, model poller, grant registry, execution owner or ACK watermark.

A trusted Host provisions the original read-only native Child through Creator
with `auto_run=false`, retains its ID, and calls `subscribe_supervisor_observer`
with `ObserverSubscriptionStart`. This calls the register-only Store operation,
persists configuration and lets the first real Inbox notice activate the Child.
It does not call the separate Observer start API or enqueue a second launch.
The existing native capability check rejects unsupported execution backends.

`SupervisorManagementState` schema 3 stores the selected `status`, `needs_input`
and/or `tasks` filter, interval (1 second to 1 hour), exact registration epoch,
bounded producer window and visible retry state. Empty new fields preserve the
original schema 1/2 encoding. Older writers reject schema 3. All window targets
must belong to the exact registration's fixed (maximum eight) target links.
Current parent/Child births, Supervisor incarnation, registration epoch, Project
and enabled links are checked again for each stage, delivery and derived read.
An installed or revoked Host namespace never falls back to durable Observer
registration. Revoked registrations remain tombstones and retries do not re-enable them.

The existing wake-reconciliation task inspects one Observer and at most eight
fixed source Roots per tick. A pending/claimed Inbox item or an active native
run coalesces newer observations. It recovers that existing wake before producing
another notice. The original Child provider/tool/token/concurrency budgets still
apply; producer enqueue receipt is not evidence of consumer ACK or model completion.

The existing safe context renderer supplies both fixed status/brief files and
opaque meaningful fingerprints from the same authorized source snapshot. Status
includes execution ID as well as persisted status; needs-input includes a hash of
the pending question's ID. Tasks include the title and every structured item's
ID, description and status, including items after the 32 bounded previews. Thus
a task change can create a new selector while the displayed first 32 previews
remain identical. Notes, evidence, token counts and update timestamps do not
trigger changes. Question text, transcripts and source paths never enter notices.
The original strict identity checks may read canonical Main for proof; only the
fixed view bundle has the existing 8/16/8 KiB physical read limits.

A successful immutable view publication precedes canonical pending. The Store
reopens the actual retained, bounded bundle and verifies its selector, files and
current provenance. Pending freezes that exact view even if the source changes.
A typed runtime notice uses its stable event ID and targets only its registered
Observer. Ordinary generic Inbox delivery cannot mint this notice. The complete
Host/lifecycle/Task/Session authority stays alive through physical Maildir jobs,
including async cancellation. Only this integrated adapter's actual receipt can
advance producer sequence. A crash after enqueue leaves pending; restart retries
the same event through the same permanent Inbox dedupe.

After a real enqueue, the candidate bounded history is published before its
canonical cursor commits. A physical publication failure leaves pending, rather
than announcing the shard as ready. A retry reuses the actual Inbox receipt.
History is the last 16 enqueue references, not an input-consumption log. Derived
bundles live under runtime-derived `coordination/observer-changes/v1/<observer-hash>/<selector>/`,
with a hash-bound manifest and up to four `0.jsonl` … `3.jsonl` shards of four
entries each. Retained directory descriptors, no-follow opens, synced files and
atomic directory rename protect publication on Linux/macOS; other platforms fail
closed. Like context views, each Observer has a visible 64-version quota; no old
snapshots or another owner's files are silently deleted.

`SessionStoreV2::read_supervisor_observer_changes(reference, child_id, epoch, after)`
checks fresh scope, returns the bounded page and physical immutable selector/path,
and verifies or reconstructs missing derived bundles. Corrupt or symlinked files
fail explicitly. A rotated cursor returns `gap=true` and the current assigned
target IDs requiring fresh scoped `export_context` snapshots. Wrong epochs and
future cursors fail. Neither JSONL, the selector, a model-supplied ACK nor a
receipt-shaped DTO can change canonical producer state or grant access.

Failures persist one non-sensitive diagnostic code with exponential bounded
retry; after eight failures the subscription is visibly blocked. Exact subscribe
retries preserve this state and cannot clear pending or reset the cap. The
ordinary Work/Planner wait stays under its existing completion coordinator.
The focused fixture with an ordinary Work Child does not claim Planner #1059
artifact or seven-step Epic acceptance coverage.
