# Task runtime filesystem publication lifetime

Bamboo #1354 retains the existing exclusive Task transaction lock through each
**already-started** Task runtime replacement. This covers a single CAS, either
paired updated write, either undo write, and existing explicit or constructor
recovery. It does not promise completion of the entire async transaction after
caller cancellation or runtime shutdown.

## Ownership and publication

The private exclusive Task guard shares one lease through `Arc`. The lease owns
the original process `OwnedRwLockWriteGuard` and original FileExt-locked file
handle. Cloning retains this same authority; it does not acquire another lock.
The final clone's destruction unlocks the physical file and releases the process
gate. The ordinary shared guard and public Storage interfaces are unchanged.

Every private locked Task commit/recovery helper requires a borrowed exclusive
guard from its existing caller. The complete runtime replacement clones that
guard into one `spawn_blocking` closure. That closure performs the existing
unique temporary file creation, complete write, file sync, replace, Unix parent
directory sync, and error cleanup with synchronous filesystem calls. Its guard
outlives every one of those operations, including cleanup when the async waiter
has disappeared. Windows keeps the existing true replace-existing primitive.
There is no remove-then-rename fallback introduced by this change.

Existing constructor, copy, clear, cleanup, reset, recursive deletion, Supervisor
bootstrap, and Root recreation callers pass their already-acquired exclusive
Task guard through existing Task recovery. Their lifecycle/proof/copy protocols
are not converted into new owned transactions. The default Session writer's
existing holder shape is unchanged, and no helper reacquires its own Task lock.

## Existing authority and recovery remain authoritative

Single/pair CAS still revalidate the durable Task generation and non-Task fields
under the exclusive lock. A physical replacement is built from the just-read
current Session, patching only Task fields. Undo still rereads the strict target
and patches only the journal's Task list/generation. Session/Root/Project/birth
checks, journal schema and states, fault points, durability event order, and
pending-journal fail-closed behavior are unchanged.

If cancellation occurs during the first paired write, that started write can
finish, but the second async write and journal finalization need not run. The
retained pending journal continues to prevent ordinary shared access until
existing exclusive recovery restores the pair. Cancellation during undo or
recovery can likewise leave recovery incomplete. The completed filesystem job
releases its lock; a later recovery call must finish the existing protocol.

An error before replace leaves the old runtime target in place. An error after
replace may expose the new target even though final durability failed. Neither
case is reported as a successful durability event. Temporary cleanup remains
best effort and preserves the original error. These outcomes are not rolled
into an invented whole-transaction success guarantee.

## Native acceptance fixtures

`v2/task_publication_lifetime_tests.rs` uses synchronous barriers inside the
actual std replacement and error-cleanup path. Test-only hooks are isolated by
Store or exact unique constructor home; there is no production hook registry.

The bounded matrix covers:

- single CAS, paired first/second replacement, first/second undo, explicit and
  constructor recovery;
- caller abort before replace, and actual Tokio `shutdown_background` or
  `shutdown_timeout` while the native job is parked after replace;
- errors before and after replace, plus cancellation and runtime shutdown at an
  actual error-cleanup barrier;
- precreated independent Stores and a separate FileExt probe showing the Task
  lock remains held while the old job runs;
- pending pair journals rejecting ordinary reads until existing recovery;
- legitimate subsequent context B publication using the now-current Task
  generation, then actual never-activated Actor claim and reopen verification
  of summary, compression events, model-context state, history and Actor birth.

The fixture releases native barriers during panic cleanup. After runtime
shutdown it uses the precreated Store for lock/recovery observation and a fresh
Store for later publication on the replacement runtime, rather than claiming a
terminated search worker continues to run.

This document describes the implementation and acceptance design. Execution
receipts separately identify the exact tested source, platform, and outcomes;
source inspection alone does not establish native test success.

## Non-goals

No new journal, recovery state, generation, Actor classifier, global writer
opt-in, exactly-once promise, or public protocol is added. Async journal
publication/finalization/deactivation remain the existing protocol; this slice
retains ownership for complete Task **runtime** replacements. Independent
SupervisorManagement full-runtime jobs (#1355), SessionInbox jobs, input
checkpoints, worker events, and unrelated writers remain separate scopes.
