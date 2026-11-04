# Frozen Ticket evidence for Patrol decisions

Issue #2486 extends the existing program-only Patrol delivery. The actual Ready
assembly stages bounded historical Ticket facts in its existing Supervisor
Inbox notice. It does not call `diagnostic_input`, select a model, meter tokens,
reserve a budget or authorize activation. Normal startup still installs no policy.

## Source and authority

When the current program pass first creates an escalation, the Host saves its
original Ticket commit, exact cursor/execution-facts revision and full escalation
operation in the same checkpoint CAS. The observation cursor must equal the
current snapshot sequence. A budget-only HEAD change is therefore a conflict,
even when execution facts did not change. No recovered escalation gains a source
afterward; the absence of a source means legacy delivery.

Historical reads use the existing bounded, depth-zero scoped inspection and
fixed-snapshot APIs. Current Host authority, Scope and Ready fences are checked
before and after the read, and again before staging. The original binding,
Project, Work, Assignment, generation, contract, immutable DispatchSpec/hash and
receipt must agree with the current authorized attempt. A stored source is a
selector and cannot recreate a grant.

The rendered facts describe the original commit, not observation time: Work and
Assignment states, the canonical `process_stopped` flag, counts of unknown
effects, the exact awaiting request's kind/status/revision, and a hash/counts of
the declared workspace. Workspace availability is unknown. No filesystem
availability, transcript, tool/error/resource history, start, heartbeat or
meaningful progress is inferred. Record inputs, identifiers, workspace counts
and serialized evidence are bounded before hashing/rendering; evidence is at
most 4 KiB and the complete existing notice remains at most 16 KiB. There are two
scoped content references. Failure or unsupported evidence stops delivery rather
than substituting current facts or a healthy/recovered claim.

## Existing journal, explicit compatibility

The public `Checkpoint` and `CheckpointPort` are unchanged. The server's private
codec writes schema 2 of the existing `bamboo/patrol-checkpoint` file only when
an immutable source exists. Its single checksum covers both `StoredCheckpoint`
and source. The same retained directory/writer lease, version CAS, bounded read,
atomic rename, fsync and poison/reopen rules apply. No second file/store/queue is
created. Older schema-1 readers reject schema 2 and cannot silently drop a source.
Mixed binaries require the existing writer quiescence rule.

Schema-1 decode and encode delegate to the original codec and retain exact bytes.
Legacy notifications retain their original data, text and ID, including an old
escalation with zero delivery attempts. Ordinary saves and ACKs retain a source
for its original escalation even after the checkpoint's latest cursor advances.
Only the existing permitted resolved-to-higher-progress incident transition may
drop/replace an old source. Current production has no progress producer.

An enriched notice is rendered entirely from the frozen source. Its ID and full
body remain identical after later Ticket facts, a failed checkpoint ACK, cooldown
and cold Host reopen. The existing canonical Inbox receipt supplies deduplication;
evidence never marks a message activation eligible or increments an interrupt.

## Acceptance ownership

The server default library owns the real native Host/Store/Inbox acceptance. Its
controlled provider produces a real stopped submission. The production stager
then succeeds, and a private `cfg(test)` hook makes the actual checkpoint ACK
publication fail before rename. The process exits naturally and a fresh Host
reconciles the original receipt from the same root after a canonical source
change. No checkpoint rollback, receipt fabrication or production fault switch
is used. Assertions retain provider/activation/interrupt/Ticket/Actor boundaries.
Cold reopen explicitly installs a fresh policy through the existing trusted Host
Rust API; no cached grant is restored. It does not claim owner HTTP reinstallation
for attempted/submitted Work: that separate owner route admits generation-0 Ready
Work only.
Private codec/store cases cover legacy bytes, no retrofit, source immutability,
future/tampered records, uncertain publication and legitimate incident replacement.
Written cases are not execution evidence; exact commands/results belong to the PR.
